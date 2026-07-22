# Fix: varchar writes and criteria matching skip JSON-unescape (index keys, records, comparisons)

> **Execution rules:** branch off `main` (fresh feature branch, do not work on `main`); do
> tasks in order; leave everything **uncommitted** for review; locate edits by searching the
> quoted anchor text (line numbers drift); build `SKIP_TESTS=1 rtk ./build.sh`, test
> `rtk ./build/bin/shard-db-test run <name>` for the affected case(s) and
> `rtk ./build/bin/shard-db-test run-all` for the full suite before calling this plan done; never
> claim a step passed without pasting the real output; if any quoted anchor is not found
> **exactly**, write `PLAN_NOTES.md` describing the mismatch and halt the entire execution
> run immediately — do not guess, reinterpret, or continue to another task. Resumption
> requires the human/planner to classify and patch the stale anchor or wrong assumption.
> If you hit a decision this plan doesn't cover, stop and ask — do not improvise.

Source: discovered while executing
`docs/plans/2026-07-17-agg-csv-topn-format-bug.md`'s Task 1a (indexing a varchar
`group_by` test field surfaced a pre-existing, unrelated bug). See that plan's "Status
update (2026-07-22)" section for how it surfaced and how its own scope was narrowed to
stay independent of this one.

> **Revision note (round 1):** this plan was reviewed before execution and had seven
> confirmed defects (three critical/high correctness gaps, two scope-completeness gaps,
> one factual error about existing behavior, and a process-compliance gap). All seven
> were addressed in that revision — see inline callouts marked **[review fix]** at each
> affected section.
>
> **Revision note (round 2):** the round-1 revision was itself reviewed and had five new
> "High"-severity correctness defects, all traced to claims about existing code behavior
> that turned out to be wrong once actually checked against source. All five, plus the
> process gaps raised alongside them, are addressed in this revision — see inline
> callouts marked **[round 2 fix]**. Nothing has been executed under either the original
> or the round-1 version.
>
> **Revision note (round 3):** the round-2 plan was checked against the current tree and
> had six execution blockers: two live simple-equality criteria parsers were missing;
> malformed `IN` cleanup left a dangling pointer that the tree caller would double-free;
> late decode/OOM failures were treated as field absence in index and bulk-update paths;
> sites 2/3 plus bitmap/trigram/composite routing lacked direct regression coverage; the
> Task 2h anchor had drifted from the fuzzer-hardened source; and the required ASan/TSan
> gates were absent. This revision integrates all six fixes. Nothing has been executed
> under any prior revision.
>
> **Revision note (round 4):** execution reached Task 4's audit (round-3 scope) with
> Tasks 1-2's ten sites already implemented and uncommitted, and its `json_get_fields(\|
> json_obj_strdup(` grep surfaced two hits that did not fit any of Task 4's four
> documented buckets, per this plan's own "stop and ask" rule. Both were investigated,
> confirmed as genuine same-root-cause defects, and the human decided to fold them into
> this plan rather than split them out: **site 11** (`query.c:5545`,
> `parse_cursor_object`'s `order_by` extraction — cursor-pagination seek values were
> never decoded, so a varchar cursor round-tripping an escaped value would seek to the
> wrong btree position) and **site 12** (`storage.c:2192`, `cmd_get_multi`'s CSV branch —
> extracts field text from an already-JSON-serialized record string without decoding it
> back, the same defect class as the aggregate CSV top-N bug fixed separately, but at a
> call site that bug's fix never touched). Both are added to the call-site inventory
> below (rows 11/12) and get their own Task 2l/2m, Task 1 tests (1l/1m), and changelog
> coverage. Tasks 1-2's original ten sites are unaffected by this addition — resume
> execution at Task 1l/1m (new tests), then Task 2l/2m, then re-enter Task 4's audit
> (now updated to expect these two, see the note there), then continue to Tasks 5-7 as
> before.

## Root cause

`config.c`'s main typed-record encoder (the path `insert` uses, ~line 2231-2244) does
this correctly and has a comment explaining why: it calls `json_unescape_string` to
decode JSON escapes (`\"`, `\\`, `\n`, `\uXXXX`, ...) out of a field's raw request-JSON
text *before* handing it to `encode_field`/`encode_field_for_index` — because a
`FT_VARCHAR` field's on-the-wire JSON text and its actual string content are not the
same bytes whenever the value contains an escapable character, and both `encode_field`
(record encoding) and `encode_field_for_index` (index-key encoding) are dumb byte
copiers that trust the caller already did that decoding.

**Eleven** live extraction/parser sites reimplement "extract field text from raw request
JSON, then encode or compare" instead of routing through that same central, correct
pattern — and all eleven skip the unescape step (the historical numbering retains
sites 7b/7c/7d and 8 so review references remain stable):

1. **`index.c:648`**, `index_parallel` (insert-time index maintenance) —
   `json_get_fields(value, unique_keys, unique_count, extracted)`, then `extracted[]`
   feeds `encode_field_for_index` for both the composite branch (~line 689) and the
   single-field branch (~line 724).
2. **`index.c:890`**, `build_index_key_from_json` (used by `storage.c`'s
   `v2_insert_pre_commit` when `insert` upserts an existing key, plus fresh bitmap/
   trigram routing), composite branch — `json_get_fields`
   (890) → `encode_field_for_index` (898).
3. **`index.c:921`**, `build_index_key_from_json`, single-field branch — `json_obj_strdup`
   (921, no unescape) → `encode_field_for_index` (930).
4. **`storage.c:1167`**, the single-record partial-`update` path — `json_get_fields`
   (1167) → `encode_field` directly (1187). This writes the **stored record bytes**,
   not just an index key.
5. **`query_bulk.c:2898`**, `bulk_upd_shard_worker_v2` (feeds
   `v2_bulk_upd_value_compute`, the criteria-driven `bulk-update` shape: `{"criteria":
   ...,"value":...}`) — `json_get_fields` (2898) → `encode_field` directly (in
   `v2_bulk_upd_value_compute`, ~line 2753 in the reviewed tree). Also writes **stored
   record bytes**.
8. **[review fix — missing from the original inventory] `query_bulk.c:3981`**,
   `bulk_upd_json_run` (feeds `v2_bulk_upd_json_value_compute` via
   `bulk_upd_json_shard_worker_v2`) — the **other** `bulk-update` shape: `{"records":
   [...]}` or `{"file":"..."}`, dispatched from `cmd_bulk_update_json`/
   `cmd_bulk_update_json_string`. This is a genuinely separate code path from site 5 —
   different top-level parser (`bulk_upd_json_run` parses either a `{"key":{...}}`
   object-format or a `[{"key":...,"value":{...}}]` array-format batch, one JSON blob
   *per record* — not one shared patch applied to every matched record like site 5),
   different worker function (`bulk_upd_json_shard_worker_v2`, not
   `bulk_upd_shard_worker_v2`), different value-compute callback
   (`v2_bulk_upd_json_value_compute`). Confirmed it also writes **stored record bytes**
   directly from un-unescaped text: `json_get_fields(data_str, field_names,
   ts->nfields, vals_buf)` (query_bulk.c:3981) → `vals_buf[i]` is stored into
   `BulkUpdJsonRec.field_values[]` (ownership transferred, ~line 4027) → consumed by
   `v2_bulk_upd_json_value_compute`'s `encode_field(&w->ts->fields[tidx],
   json_rec->field_values[i], new_buf + ...)` (query_bulk.c:3718). Both the
   object-format and array-format top-level shapes funnel into this same
   `json_get_fields` call at line 3981 (the shape only affects how `key`/`data_str`
   are located per record, not how field values are extracted from `data_str`), so one
   fix covers both — but this plan's Task 1 tests both shapes explicitly since they are
   different parsing branches that could independently regress.
6. **`query_plan.c:1236`/`1238`**, `parse_one_criterion` (shared by `find`/`update`/
   `bulk_update`/`delete` criteria parsing) — `json_obj_strdup(&cobj, "value")` (1236)
   and `json_obj_strdup(&cobj, "value2")` (1238), neither unescaped, flow via
   `strncpy(c->value, v, ...)` (1303, and `c->value2` at line ~1381) into
   `SearchCriterion`, then into `compile_one`'s `cc->s1`/`cc->s2` (query_plan.c:279-284),
   which `match_typed`/`match_typed_varchar` compare directly against **decoded** record
   content. This is a **comparison-only** bug: it doesn't corrupt anything on disk, but
   it makes equality/range/like criteria silently fail to match any record whose field
   value legitimately contains a JSON-escapable character — and unlike sites 1-5/8, it
   does **not** require the field to be indexed. This is likely the single most
   production-visible symptom of this whole bug class, since it affects every unindexed
   `find`/`update`/`bulk_update`/`delete` criterion too, not just indexed lookups.
7. **`query_plan.c:1327-1357`**, `parse_one_criterion`'s `OP_IN`/`OP_NOT_IN` array-value
   splitter — manually walks `v_raw` (raw JSON span from `json_obj_strdup_raw`, brackets
   and quotes intact) char-by-char and builds each element via
   `malloc(len+1); memcpy(val, start, len)` — a raw copy of each array element's JSON
   text, never unescaped. Same comparison-only failure mode as site 6, scoped to `IN`/
   `NOT_IN` criteria specifically.

   **[review fix — the scanner itself is broken, not just "unescape missing"]** The
   inner scan that finds each quoted array element's end,
   `while (*ap && *ap != '"') ap++;` (query_plan.c ~1331), does **not** recognize an
   escaped quote (`\"`) as anything other than an ordinary character followed by a
   closing delimiter — it stops at the *first* `"` byte it sees, escaped or not. For
   input `["He said \"hi\"","Plain"]`, this truncates the first element's raw span at
   `He said \` (the backslash immediately before the escaped quote), not at the real
   end of the string. Appending an unescape pass *after* this truncation cannot recover
   the lost text — the span is already wrong before unescaping ever runs. The fix must
   correct the **boundary scan itself** (skip `\` + next-byte as a pair, never terminate
   on an escaped quote), then unescape the now-correctly-bounded raw span. See Task 2h.

**[review fix — a second hand-rolled scanner has the identical bug]** `parse_one_criterion`
also has a *second*, independent quote-scanning loop for the `OP_BETWEEN`/
`OP_LEN_BETWEEN` legacy array-form (`"value":["25","30"]`, query_plan.c ~1255-1266):

```c
if (*ap == '"') {
    ap++;
    start = ap;
    while (*ap && *ap != '"') ap++;   /* same bug as site 7 */
    plen = (size_t)(ap - start);
    if (*ap == '"') ap++;
}
```

This is the same shape as site 7's bug, independently reimplemented. `BETWEEN` is not
numeric-only — `match_typed_varchar`'s `OP_BETWEEN` case (query_plan.c:614-621) does a
lexicographic `memcmp` range check, so a varchar field legitimately supports `between`
with string bounds. A between-bounds value containing a literal quote (e.g. bounding a
varchar range by `"a\"b"`/`"c\"d"`) hits this exact truncation. This is call-site **7b**
in the inventory below, fixed by the same shared boundary-scan helper as site 7.

**[round 3 fix — two live parsers missing from the prior inventory]** Criteria also
have a backward-compatible simple-equality form, `{"field1":"value1"}`, implemented
twice without `parse_one_criterion`:

- **7c. `query_plan.c:1429-1477`, `parse_criteria_json` simple-equality branch** — used
  by CAS `if` conditions and simple-form aggregate `having`; it strips quotes and copies
  the raw escaped span directly into `SearchCriterion.value`.
- **7d. `query_plan.c:1833-1882`, `parse_criteria_tree` simple-equality branch** — used
  by `find`/`count`/`aggregate`/criteria-driven mutation requests; it performs the same
  raw copy into a leaf criterion.

Both must decode a quoted value after `json_skip_value` has bounded it and before the
value is committed to the fixed-size criterion buffer. Unquoted JSON scalars remain raw
as today. Task 2i adds literal patches for both branches, and Task 1 adds separate
simple-query and simple-CAS regression assertions. These hand-written copies do not
contain `json_get_fields`/`json_obj_strdup`, so the old Task 4 grep could never have
found them; Task 4 now includes a second `memcpy(...value...)` audit.

Sites 1-3 corrupt only the **index key** — the record itself stays correct (it's built
through the separate, correct `config.c` path), but any query that reads the
group/filter value *from the index* instead of the record now sees the wrong bytes.
Sites 4/5/8 are worse: they corrupt the **record itself**, which is what every future
`get`/`find`/`aggregate` on that field will decode, forever, until the field is
overwritten again through a path that happens to supply already-clean text. Sites 6/7/7b/7c/7d
corrupt nothing at rest — they make correctly-stored data invisible to certain queries.

**Confirmed impact of sites 1-3** (via a throwaway repro, not committed): with `category`
indexed and a record inserted with `category: He said "hi"`, `get` by key correctly
returns `{"category":"He said \"hi\""}` (record is fine), but
`find` with `{"field":"category","op":"eq","value":"He said \"hi\""}` returns `[]` — the
record is invisible to an indexed equality lookup on its own field, because the index
holds `He said \"hi\"` (literal backslashes) instead of `He said "hi"`. This also explains
the display bug found while executing the CSV plan: three separate aggregate code paths
(`agg_run_topn_stream`'s own inline JSON emission, and the `VS`/`IGB` index-walk fast
paths, which populate the *same* shared hash-table/emit pipeline the full-table-scan path
uses — confirmed via code comments at `query_aggregate.c:4676-4677`, "feeds the existing
having / order_by / limit / emit pipeline") all read the group value straight from the
(corrupted) index instead of the record, so any of them being engaged by an indexed
`group_by` field surfaces the same corruption in both JSON and CSV output. The shared
emit code itself (`csv_emit_cell`, `json_escape_const`) is correct — it's just faithfully
emitting corrupted input.

**Confirmed impact of site 6** (via a second throwaway repro, not committed, fully
independent of indexing): a plain, **unindexed** `FT_VARCHAR` field `category`, record
inserted with `category: He said "hi"`. `get` by key correctly returns
`{"category":"He said \"hi\""}`. `find` with
`{"field":"category","op":"eq","value":"He said \"hi\""}` on this unindexed field
**also** returns `[]` — proving the failure isn't only about corrupted index bytes; the
criterion's own comparison value is wrong before any index or record is even consulted.
This is a distinct bug from sites 1-3 (different code, different code path, no index
involved) that happens to produce the same user-visible symptom (`find` misses a record
that visibly has the value being searched for).

**Confirmed-safe, related call sites (no fix needed, verify during Task 7)**:
`build_index_key_from_record`/`build_index_key_from_record_into` (index.c, used by
`storage.c`'s update-diff path and several `query_bulk.c` diff/rebuild paths at lines
2296/2304/2820-2850/3182-3185/3656-3659/4227) read from the **already-decoded binary
record** via `typed_field_to_index_key`, not from JSON text — they reflect whatever is
actually stored, correctly, regardless of this bug.

**[review fix — dead code, confirmed, no action taken]** `index.c:563` (inside
`extract_field_value`) and `index.c:576`/`595` (inside `build_composite_value`) also
call `json_get_fields`/`json_obj_strdup` without unescaping — but both functions are
**confirmed dead code**: `grep -rn "extract_field_value\|build_composite_value"
src/db/*.c src/db/*.h` shows `extract_field_value` is declared in `types.h:692` and
defined in `index.c:554` with **zero callers** anywhere in `src/db`, and
`build_composite_value` (`index.c:581`) isn't even declared in a header (file-local by
convention, `zero` external callers, and no in-file caller either). Neither corrupts
anything in production today because neither ever runs. Removing dead code is a
separate, unrelated cleanup and out of scope for this bug-fix plan (no drive-by
refactors) — **this plan makes no change to either function**. Flagged here so it isn't
mistaken for an overlooked call site in a future audit.

## Field-type scope

Only **`FT_VARCHAR`** is definitely affected in `encode_field`/`encode_field_for_index`:
its case is a raw `memcpy` of the input bytes (config.c:1816-1823 for the index-key
encoder; `encode_field_len` mirrors this for record encoding). Every other type —
`FT_INT`/`FT_SHORT`/`FT_LONG`/`FT_DOUBLE`/`FT_FLOAT`/`FT_NUMERIC`/`FT_BOOL`/`FT_BYTE`/
`FT_DATE`/`FT_DATETIME`/`FT_DATETIMEMS`/`FT_TIME`/`FT_TIMESTAMP`/`FT_IPV4`/`FT_IPV6` —
parses via `atoi`/`atof`/`inet_pton` or explicit digit-scanning loops that only look at
`'0'-'9'` (or format punctuation like `:`), so a stray unescaped `\` in the input is
either rejected by the parser or silently ignored by the digit filter — it does not
corrupt the numeric/temporal result. These types are unaffected; the fix should not
touch them. The same reasoning applies to sites 6/7/7b/7c/7d's comparison path: comparing an
un-unescaped numeric criterion string against a decoded numeric field is still correct,
since neither side ever contains a backslash for those types.

**[review fix — FT_ENUM is explicitly out of scope for this plan, not "free to fix"]**
The original draft of this plan claimed fixing `FT_ENUM` the same way as `FT_VARCHAR`
"is free." That is wrong, and the plan is corrected here: `FT_ENUM`'s actual defect is
**upstream of every call site this plan inventories**, in `config.c`'s *insert-time
validation itself*, not in a missing-unescape-at-extraction issue this plan's "one
central rule" can absorb.

Confirmed by reading `typed_encode_defaults` (`config.c:2654-2689`): the strict enum
membership check runs **before** the unescape pass:

```c
const char *ev = vstart; size_t el = vlen;
if (el >= 2 && ev[0] == '"' && ev[el - 1] == '"') { ev++; el -= 2; }
if (el > 0) {
    /* Enum: strict membership check before the encode. ... */
    if (ts->fields[i].type == FT_ENUM &&
        enum_value_index(&ts->fields[i], ev, el) < 0) {
        fmt_unknown_enum_err(&ts->fields[i], ev, el, err_buf, err_buf_size);
        return -2;
    }
    /* Same JSON-unescape pass typed_encode does for varchars ... */
    if (ts->fields[i].type == FT_VARCHAR) { /* unescape happens here, AFTER the enum check */ }
```

`enum_value_index` (config.c:1188) does an exact `memcmp` of `ev`/`el` — still raw,
un-unescaped wire text at this point — against the schema's clean (never-escaped) enum
label list. If a schema legitimately declares an enum label containing a character that
requires JSON-escaping on the wire (a quote or backslash in the label text), every
client request naming that label would necessarily arrive as escaped wire text, fail
this `memcmp`, and get rejected by `insert` itself with "unknown enum value" — even
though the value is one of the declared labels. This is a real, confirmed (not merely
theoretical) defect, but fixing it correctly means **reordering validation logic inside
`typed_encode_defaults`** (and checking whether the plain `typed_encode`, config.c:2183,
has the same or a different code shape for its own enum case) — a change to insert-time
validation ordering, structurally unrelated to any of this plan's eight JSON-extraction
call sites, and risks its own regressions (e.g. does moving the unescape earlier change
what `fmt_unknown_enum_err` reports for a truly-unknown label?).

**Decision: FT_ENUM escaping is explicitly out of scope for this plan.** It is a
separate, pre-existing defect, filed here for a future, independently-scoped plan. This
plan's `encode_field`/`encode_field_for_index` and its eleven live extraction/parser sites are untouched
for `FT_ENUM` — Task 3 (below) is reduced to a documentation/confirmation step only, not
a fix.

**[round 2 fix — the descope was not actually honored by round 1's extraction design]**
Round 1's `json_get_fields_unescaped` decoded every extracted field unconditionally,
with no awareness of the field's schema type. That silently pulls `FT_ENUM` back into
scope through the back door: sites 4/5/8 (and, before this revision, sites 1/2 too)
would start unescaping enum values on `update`/`bulk_update`/`bulk_update_json`, while
`insert` (via `typed_encode_defaults`) still validates enum membership against raw,
un-unescaped text — a client sending an escaped enum label would be accepted by
`update` but rejected by `insert` for the exact same value. This revision makes
write/index extraction **type-aware**: every storage or index call site that reaches
for `json_get_fields_unescaped` or an inline unescape checks
`TypedField.type == FT_VARCHAR` first; every other stored/indexed type (including
`FT_ENUM`) retains its previous raw-text behavior. `parse_one_criterion` is the one
intentional exception: it parses protocol JSON before a schema/type is available and
therefore decodes JSON string syntax for every comparison operand. That can only make
an already-stored value comparable to the value the client actually sent; it does not
change enum insert validation, record encoding, or index encoding, and therefore does
not fix the separately-descoped “escaped enum label cannot be inserted” defect. See
Task 2's per-site subsections for the exact mechanisms — inline type-gating at
`index.c`'s sites, and a `field_types` parameter on
`json_get_fields_unescaped` for the index-aligned write sites (4/5/8).

**Bitmap and trigram indexes are covered, but through a different caller.**
`index_parallel` explicitly skips every non-btree field (`index.c:657-661`); fresh
bitmap and trigram inserts instead call `build_index_key_from_json` from
`storage.c:646-714`, then dispatch the resulting key through `update_idx_fn`.
Therefore Task 2d's single-field `build_index_key_from_json` fix is what corrects
fresh bitmap/trigram keys, while Task 2b corrects btree keys. Composite indexes remain
btree-only. No bitmap- or trigram-specific encoder change is required, but the test
coverage and rationale must not claim `index_parallel` itself writes those index types.

## Design decision — one central rule, not eight patches

The user's explicit ask driving this section: *"can we have one central place where we
escape [sic, unescape] varchar values before saving? whether its data, indexes."*

Two fix shapes were considered:

**(a) Push the unescape into the low-level encoders** (`encode_field`,
`encode_field_for_index`, `encode_field_len` in `config.c`) — rejected. A full grep of
every caller of these three functions across `index.c`, `query_bulk.c`, `query.c`,
`query_find.c`, `query_join.c`, `query_plan.c`, and `storage.c` (18+ call sites) shows
many pass values that were **never JSON text to begin with**: formatted timestamps
(`tbuf`), generated UUIDs, hex buffers, schema `default_val` strings, and — critically —
`query_join.c:268`'s re-encoding of an **already-decoded** record field for a join-key
lookup. Unescaping at this layer would corrupt any of these (e.g. a schema default of
`C:\temp` would become `C:temp`). The encoders have no way to know whether their caller's
input came from raw request JSON or from somewhere else, so they are the wrong layer to
enforce this rule.

**(b) Make the JSON-extraction boundary the one central place — chosen, with one
qualification** [review fix — see below]. Every one of the eleven live sites above
shares the same actual root cause: something read a field's *value* out of raw request
JSON using a "just strip quotes" primitive (`json_get_fields`, `json_obj_strdup`, or a
hand-rolled span copy) instead of the "strip quotes *and* decode escapes" primitive that
already exists and is already used correctly by the main `insert` path. The fix is not
eight unrelated patches; it is **one rule, applied everywhere a field value crosses the
JSON boundary**:

> Extracting a field's *value* (the user's data — what's going to be stored, indexed, or
> compared) from request JSON must always decode JSON escapes before that value is used.
> Extracting *protocol/control* JSON keys (`"mode"`, `"dir"`, `"object"`, `"field"`,
> `"op"`/`"operator"`) is unaffected and keeps using the plain extractor — those are
> never user data, never contain a client-supplied escape the schema cares about, and
> are matched against fixed internal keyword tables, not decoded record content.

**[review fix — the qualification]** For sites where a value is extracted as a single,
already-delimited scalar with no further structural parsing (sites 1-5/8), the rule
above is applied literally: decode at the extraction boundary, via one of two shared
extractors (below). But **sites 6/7/7b are different in kind**: `query_plan.c`'s
`value`/`value2` extraction can feed *further structural parsing* — the legacy
`OP_BETWEEN`/`OP_LEN_BETWEEN` two-element array form, and the `OP_IN`/`OP_NOT_IN`
array form, both of which locate element boundaries by scanning the *raw* JSON text for
structural `"`/`,`/`]` characters. Decoding at the extraction boundary, *before* that
structural scan runs, is actively wrong: `json_unescape_string` has no concept of JSON
array/string structure — given the raw span `["a\"b","c\"d"]`, it decodes the *entire*
span as one flat string (there's nothing stopping it from doing so; it just walks every
`\X` sequence it finds), turning it into `["a"b","c"d"]` — a span where the originally-
escaped inner quotes are now byte-for-byte indistinguishable from the array's own
structural quotes. Any subsequent element-boundary scan over that already-decoded text
is now unrecoverably confused about where one element ends and the next begins. For
these three sites, the rule instead becomes: **extract raw, run any structural
(array-form) parsing on the still-raw text using a boundary scanner that correctly
skips escaped delimiters, then decode each individually-bounded element immediately
before it is committed** to `c->value`/`c->value2`/`c->in_values[]`. Sites 7c/7d are
plain scalars but live in hand-written simple-equality parsers, so they decode their
already-bounded quoted span immediately before the bounded criterion-buffer copy. This
is still one rule applied everywhere a value crosses the boundary: after structural
parsing where structure exists, otherwise at extraction/commit. See Tasks 2g-2i.

Two extractors enforce the rule for the plain-scalar sites (1-5/8):

- **`json_obj_strdup_unescaped`** — already exists (`util.c:491-498`), already the
  correct pattern used by `config.c`'s `insert` path. Any single-field `json_obj_strdup`
  call that extracts a *value* (not a protocol key) becomes
  `json_obj_strdup_unescaped` — a pure rename at the call site, same signature shape
  plus one extra `NULL` arg for length-out. Used unmodified (`NULL`-on-failure,
  conflating "absent" and "malformed") at index-only site 3, where that conflation is
  harmless (see Task 2 rationale).
- **`json_get_fields_unescaped`** (new, this plan adds it) — the multi-field
  counterpart. Same *shape* as `json_get_fields` (same parameters, same per-field
  `out[i] = NULL` convention for "absent or malformed"), but **[review fix]** returns
  `int`, not `void`: `0` if every field that was present in the JSON decoded
  successfully, `-1` if at least one *present* field had a malformed escape sequence.
  This distinction — "field absent" vs. "field present but corrupt" — matters because a
  write-path caller (sites 4/5/8) that silently treated a present-but-corrupt field the
  same as an absent one would apply a **partial update**: every other field in the same
  request succeeds, and the corrupt field is silently left unchanged, with nothing in
  the response telling the client that the field they explicitly named didn't take
  effect. See Task 2's per-call-site policy for what each of sites 1/4/5/8 does with
  this return value — it is **not** uniform, and this plan states each one explicitly
  rather than leaving it to executor judgment.

  **[round 2 fix]** Signature is now
  `int json_get_fields_unescaped(const char *json, const char **keys, int nkeys, const enum FieldType *field_types, char **out)`
  — `field_types[i]` is the schema type of `keys[i]` (index-aligned, same convention as
  `keys`/`out`). Decode is gated to `field_types[i] == FT_VARCHAR`; every other type's
  `out[i]` is left exactly as `json_get_fields` produced it (raw, quotes stripped, no
  unescape), same as before this plan existed — this is what keeps `FT_ENUM` (and every
  numeric/temporal type) genuinely untouched. `field_types` is a required parameter, not
  optional — every call site in this plan already has a `TypedSchema`/`TypedField` array
  in scope to build it from, so there's no caller for which `NULL` would be meaningful.

**[round 2 fix — NUL rejection scoped correctly]** Round 1 planned to make
`json_unescape_string` itself reject an embedded `\u0000`. That was checked against
every actual caller and found to be too broad: `json_unescape_string` has exactly one
caller *outside* this plan's new code — `json_obj_strdup_unescaped` (`util.c:491`) —
which is itself called from `server.c:1720` for `bulk-insert-delimited`'s inline `data`
field. That call site is **deliberately** length-aware (`size_t data_len` is captured
and passed straight into `cmd_bulk_insert_delimited_string`, never `strlen()`'d), so an
embedded NUL there is not a latent bug the way it is everywhere else — rejecting it
would be a real, uninventoried behavior change to a caller this plan has no reason to
touch. Per the review's option (a), the fix is scoped narrower: a new wrapper,
`json_unescape_cstring`, calls `json_unescape_string` and additionally rejects (frees
the buffer, returns -1) when the decoded result contains an embedded NUL
(`strlen(*out_buf) != *out_len`). Every call site *this plan* introduces or modifies —
`json_get_fields_unescaped`, the inline decodes in `index.c`, `typed_encode_defaults`,
and `parse_one_criterion`'s commit-point decodes — uses `json_unescape_cstring`, not
`json_unescape_string` directly. `json_unescape_string` itself, and its one existing
caller `json_obj_strdup_unescaped`, are **left untouched** — the delimited-data path
keeps tolerating an embedded NUL exactly as it does today. See Task 2a for the
implementation and Task 2k for why this, combined with fixing `typed_encode_defaults`'s
fallback, is what actually makes Test 1g pass.

**[round 2 fix — shared boundary-scan helper promoted to util.c]** Round 1 added
`json_raw_string_end` (the parity-aware quote-boundary scanner) as a `static` helper
local to `query_plan.c`. That left the much more widely-used `json_skip_value`
(`util.c`) — which gates value-span detection for essentially every one of this plan's
eleven live sites, since `json_get_fields`/`json_get_fields_unescaped`/`json_obj_strdup`/
`typed_encode_defaults` all walk field values via it — with its own three internal
copies of the identical bug, unfixed. A varchar value ending in an even number of
backslashes (e.g. JSON `"ends\\\\"`, whose content is one literal trailing backslash)
would still have its value-span mis-detected before extraction ever reaches the
unescape step this plan adds — no amount of fixing the unescape step downstream can
recover a span that was already truncated wrong. `json_raw_string_end` is promoted to a
shared, non-`static` function in `util.c` (declared in `types.h`), and `json_skip_value`
is fixed to use it for all three of its internal string-boundary scans. See Task 2a.
Eight other, structurally identical copies of the same buggy pattern exist elsewhere
(field-name scans and record-key-array scans, not field *value* scans) — see Task 2a's
explicit descope note for why those are left alone.

This makes the rule mechanically checkable going forward: a `grep -n
'json_get_fields(\|json_obj_strdup(' src/db/*.c` after this plan lands should show only
protocol-key extractions (`"mode"`, `"field"`, `"op"`, `"dir"`, `"object"`, etc.),
genuinely-non-JSON re-encodes (like `query_join.c:268`'s already-decoded record field),
the two confirmed-dead functions in `index.c` (left untouched, see Root cause), and
`query_plan.c`'s deliberately-raw `v`/`v_raw`/`v2` extraction (which now decodes later,
per the qualification above, not at the extraction call itself). Task 4 runs that grep
and reviews what's left — this plan has already done that review once during planning
(see Task 4, which now reports the finding directly instead of asking the executor to
investigate it).

`json_obj_strdup_raw` (used at `query_plan.c:1237` to get the *raw*, quote-and-bracket-
intact span for `OP_IN`/`OP_NOT_IN` re-parsing) is deliberately **not** replaced with an
unescaped variant — its job is to hand back an unmodified span so the caller can find
array-element boundaries by scanning for `"`/`,`/`]`; unescaping would destroy exactly
the delimiters the caller needs to see, per the qualification above.

## Call-site inventory (production)

**[round 2 fix]** Two corrections from round 1: (a) `typed_encode_defaults` must
genuinely reject malformed input (Task 2k) before index maintenance; (b) row 7's policy
changes from "drop the malformed element" to
"reject the whole criterion" — dropping an element from a `NOT_IN` list silently
*broadens* the match set (a record the client meant to exclude could now match), which
is a correctness/security regression, not a harmless degradation. See Task 2h.

**[round 3 fix — decode failure is never field absence]** `json_unescape_cstring`
inherits `json_unescape_string`'s failure contract: malformed input, embedded NUL, and
allocation failure all return nonzero. Upstream validation makes malformed/NUL input
unreachable at the later index-maintenance calls, but it cannot make a second allocation
infallible. Sites 1-3 therefore fail the enclosing pre-commit on any late decode
failure; they never drop/delete an index key and continue. Site 5 decodes the shared
patch exactly once before dispatch and shares that read-only array with every worker,
eliminating the prior validate/free/re-decode race.

| # | File:line | Function | Extraction call (before) | Extraction call (after) | Feeds | Decode-failure policy |
|---|-----------|----------|---------------------------|--------------------------|-------|-------------------------|
| 1 | `index.c:648` | `index_parallel` | `json_get_fields` | `json_get_fields` (unchanged) + inline, type-gated `json_unescape_cstring` per unique key immediately after extraction | `encode_field_for_index` (composite ~689, single ~724) | Any decode failure cleans up, sets `errno`, and returns `-1`; the insert pre-commit aborts. Never commit a record with a missing index key. |
| 2 | `index.c:890` | `build_index_key_from_json` (composite) | `json_get_fields` | `json_get_fields_unescaped` with a type array | `encode_field_for_index` (~898) | Contract becomes tri-state: `1` key, `0` genuinely absent/empty, `-1` decode/allocation failure. All four production callers propagate `-1` to abort pre-commit. |
| 3 | `index.c:921` | `build_index_key_from_json` (single) | `json_obj_strdup` | type-gated: `json_obj_unquoted` + `json_unescape_cstring` if `FT_VARCHAR`, else `json_obj_strdup` unchanged | `encode_field_for_index` (~930) | Same tri-state contract as row 2; decode failure is `-1`, never “field absent.” |
| 4 | `storage.c:1167` | single-record `update` | `json_get_fields` | `json_get_fields_unescaped` (new `field_types` param) | `encode_field` (~1187) — **record bytes** | **Check return; reject the whole update with `{"error":...}` and apply nothing**, matching the existing varchar-length-cap error convention a few lines below. |
| 5 | `query_bulk.c:2898` | `bulk_upd_shard_worker_v2` | `json_get_fields` | removed; `cmd_bulk_update` decodes once into a shared read-only `field_vals` array before Phase 1 | `v2_bulk_upd_value_compute` → `encode_field` — **record bytes** | Reject before scan/dispatch on decode failure; workers perform no second allocation/decode and cannot diverge shard-by-shard. |
| 6 | `query_plan.c:1236`/`1238` | `parse_one_criterion` (`value`/`value2`) | `json_obj_strdup` | `json_obj_strdup` (unchanged — raw) + inline `json_unescape_cstring` right before `strncpy` into `c->value`/`c->value2`, **skipped entirely for `OP_IN`/`OP_NOT_IN`** (row 7 decodes independently) | `c->value`/`c->value2` → `compile_one` → `match_typed*` — **comparison only** | Malformed escape → `return -1` (matches this function's existing "invalid criterion" contract). |
| 7 | `query_plan.c:1327-1376` | `parse_one_criterion` (`IN`/`NOT_IN` element split, both the bracketed-array form and the legacy comma-separated form) | raw `memcpy` span, boundary scan stops on any `"`, no unescape | bracketed form: boundary scan uses shared `json_raw_string_end` (skips escaped quotes), then `json_unescape_cstring` once per element; comma form: `json_unescape_cstring` once per token | `c->in_values[]` → `match_typed*` — **comparison only** | **[round 2 fix]** Malformed escape → **reject the whole criterion** (`return -1`), for both the bracketed and comma-separated forms — not "drop that element" (round 1's policy; see rationale above the table). |
| 7b | `query_plan.c:1259-1288` | `parse_one_criterion` (`BETWEEN`/`LEN_BETWEEN` array-form split) | raw `memcpy` span, boundary scan stops on any `"` | same shared `json_raw_string_end` helper, then unescape `parts[0]`/`parts[1]` before `strdup` into `v`/`v2` | `v`/`v2` → same path as row 6 | Malformed escape → `return -1` (same as row 6, since `v`/`v2` end up going through the same final decode-before-commit as row 6 either way). |
| 7c | `query_plan.c:1429-1477` | `parse_criteria_json` simple equality | quote-strip + raw `memcpy` | decode quoted span with `json_unescape_cstring`, then bounded copy | CAS/simple `having` criteria | Decode failure rejects the whole criteria object and frees earlier leaves. |
| 7d | `query_plan.c:1833-1882` | `parse_criteria_tree` simple equality | quote-strip + raw `memcpy` | decode quoted span with `json_unescape_cstring`, then bounded copy | query/mutation criteria tree | Decode failure frees the new leaf and root, sets `err`, and rejects the whole tree. |
| 8 | `query_bulk.c:3981` | `bulk_upd_json_run` (feeds `v2_bulk_upd_json_value_compute`) | `json_get_fields` | `json_get_fields_unescaped` (new `field_types` param) | `encode_field` (query_bulk.c:3718) — **record bytes** | **Check return per record; skip that one record** (increment `skipped`, apply none of its fields), consistent with this function's existing per-record skip semantics for other validation failures (missing key/data, key exceeding `max_key`). This is a *different* granularity from row 5 because `bulk_upd_json_run`'s value_json is genuinely per-record (each array/object entry carries its own patch), unlike row 5's single shared patch applied to every matched record. |
| 9 | `config.c:2674` | `typed_encode_defaults` (the live `insert`/bulk-insert encoder) | `json_unescape_string`, falls back to storing raw escaped bytes on decode failure | `json_unescape_cstring`, **`return -2`** (existing error contract) on decode failure — no fallback | `encode_field_len` — **record bytes**, the write path every other site's "insert already validated this" reasoning depends on | **[round 2 fix, new site]** This is the foundational validation fix; see Task 2k. `typed_encode` (`config.c:2183`) has the identical bug but is confirmed dead code (zero callers); left untouched. |
| 10 | `util.c` (`json_skip_value`, 3 internal copies) | value-span boundary detection used transitively by nearly every site above | naive `!(*p=='"' && *(p-1)!='\\')` check, wrong for an even number of trailing backslashes | uses shared `json_raw_string_end` | **[round 2 fix, new site]** Not a malformed-escape case — a *valid* value ending in a literal backslash was mis-bounded before extraction. See Task 2a and Edge cases. |
| 11 | `query.c:5545` | `parse_cursor_object` (cursor-pagination `order_by` value) | `json_obj_strdup(&c, order_by)` | left as-is at extraction (schema isn't known yet at this point in the parse); decoded in place at the call site (`query.c` `find`'s cursor branch, ~line 6879) once `order_tf` is resolved, type-gated on `FT_VARCHAR`, via `json_unescape_cstring` | `cur.value`/`cur.vlen` → `encode_field_for_index` walk-bound seek | **[round 4 fix, new site]** Malformed escape → reject the cursor request with `{"error":...}` rather than seeking from a corrupted position. Non-varchar/composite order_by is unaffected (raw passthrough, unchanged — matches row 3's untyped-fallback precedent). |
| 12 | `storage.c:2192` | `cmd_get_multi` CSV branch | `json_obj_strdup(&value_obj, fs.ts->fields[fi].name)` on an already-JSON-serialized (`typed_decode`-produced) record string | type-gated: `json_obj_strdup_unescaped` if `FT_VARCHAR`, else `json_obj_strdup` unchanged | `csv_emit_cell` — **CSV output formatting**, not a write/comparison path | **[round 4 fix, new site]** Different failure shape from rows 1-10: this reads back a record this same fix already wrote correctly, so a decode failure here is not expected in practice; `json_obj_strdup_unescaped` returning `NULL` degrades to an empty CSV cell, identical to today's existing behavior for a missing field (no new error path needed). |

## Task 1 — Test-first: reproduce all symptoms as failing regression tests

**[review fix — literal test code, one dedicated new file, not prose]** All of the
following are added to a **new** file, `src/test/cases/test_index_varchar_unescape.c`
(the existing `test_json_escape.c` is scoped to output-escaping — read-side JSON
correctness of already-stored data — documented in its own file header comment; this
plan's tests are write/index/comparison-side, a different concern, and adding ten new
scenarios to a 295-line file would make it unwieldy). Register the new file in
`build.sh`'s test-source list.

Anchor (`build.sh`):

```
    src/test/cases/test_json_escape.c \
```

Replace with:

```
    src/test/cases/test_json_escape.c \
    src/test/cases/test_index_varchar_unescape.c \
```

New file, full contents:

```c
/* test-index-varchar-unescape — regression coverage for the JSON-unescape
 * gap in varchar record/index-key encoding and criteria comparison.
 * See docs/plans/2026-07-22-index-key-json-unescape.md for the full
 * root-cause writeup; this file covers every live write/index/criterion
 * parser family identified by that plan.
 */
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "types.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/* 1a. Indexed find-equality regression (sites 1-3: index-key corruption). */
static int test_idx_varchar_find_eq(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    /* Control: the record itself is stored correctly. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"idx1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "control: get returns correctly-escaped record");
    free(resp); resp = NULL;

    /* Bug: indexed equality find misses the record. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"idx1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "indexed find eq must match the record via its own index key");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1b. Single-record update record-corruption regression (site 4). */
static int test_update_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"],\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"upd1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"upd1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "update: record decodes to single-escaped form, not double-escaped");
    ASSERT_TRUE(strstr(resp, "\\\\\"hi\\\\\"") == NULL, "update: no double-escaping artifact");
    free(resp); resp = NULL;

    /* Same field is indexed — re-exercise the update-diff index path too. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"upd1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "update: post-update indexed find matches on the new value");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1c. bulk-update (criteria+value shape) record-corruption regression (site 5). */
static int test_bulk_update_criteria_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\",\"tag\":\"x\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\",\"tag\":\"x\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bu1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"x\"}],"
        "\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bu1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update k1: correctly single-escaped");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bu1\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update k2: correctly single-escaped");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1c2. bulk-update (records shape) record-corruption regression (site 8),
   covering both accepted top-level formats. */
static int test_bulk_update_json_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    /* Object-format records: {"key":{...partial fields...}}. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bj1\","
        "\"records\":{\"k1\":{\"category\":\"He said \\\"hi\\\"\"}}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bj1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update-json object-format: correctly single-escaped");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    /* Array-format records: [{"key":"...","value":{...}}]. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bj2\","
        "\"records\":[{\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bj2\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "bulk-update-json array-format: correctly single-escaped");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1d. Unindexed criteria-comparison regression (site 6) — highest-value test
   in this plan: no index involved at all, proves the bug is in criteria
   parsing, not storage. */
static int test_criteria_unindexed_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"un1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "He said \\\"hi\\\"", "control: record itself is stored correctly");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"un1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "unindexed find eq must match despite no index involvement");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1e. IN-list criteria regression (site 7) — the array-element boundary
   scan must not truncate on an escaped quote. */
static int test_criteria_in_list_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Plain\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"in1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"in\","
        "\"value\":[\"He said \\\"hi\\\"\",\"Plain\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "IN: escaped-quote element must still match k1");
    ASSERT_CONTAINS(resp, "\"k2\"", "IN: plain element must still match k2");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1f. BETWEEN array-form regression (site 7b) — varchar bounds containing a
   literal quote must not corrupt the split. */
static int test_criteria_between_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    /* Bound value is exactly the search value, with quotes on both sides so
       a lexicographic between of ["He said \"hi\"","He said \"hi\""] must
       include it. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"bt1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"between\","
        "\"value\":[\"He said \\\"hi\\\"\",\"He said \\\"hi\\\"\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "BETWEEN array-form with quoted varchar bounds must match");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1g. \u0000 must be rejected, not silently truncate downstream strlen()
   consumers (see Edge cases / invariants for why). */
static int test_varchar_embedded_nul_rejected(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"nul1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"nul1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"a\\u0000b\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "insert with \\u0000 in a varchar value is rejected, not silently truncated");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1h. A valid varchar ending in a literal backslash must be bounded,
   stored, indexed, and compared correctly (site 10 + sites 1/6). */
static int test_varchar_trailing_backslash(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"category:varchar:64\"],"
        "\"indexes\":[\"category\"]}", &resp);
    free(resp); resp = NULL;

    /* Wire JSON contains two backslashes before the structural quote;
       decoded varchar content ends in one literal backslash. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"ends\\\\\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL,
                "trailing-backslash insert succeeds");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"trail1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "ends\\\\", "get preserves the literal trailing backslash");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"trail1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"ends\\\\\"}]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "indexed criterion ending in backslash matches");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1i. Re-inserting an existing key drives build_index_key_from_json, not
   cmd_update_v2's record-based helpers. Cover its single-field, composite,
   bitmap, and trigram routes explicitly (sites 2/3). */
static int test_upsert_json_index_routes_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"region:varchar:64\","
        "\"kind:varchar:64\",\"bio:varchar:128\"],"
        "\"indexes\":[\"category\",\"category+region\",\"kind:bitmap\",\"bio:trigram\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create route-coverage object");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"routes1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"old\",\"region\":\"old\","
        "\"kind\":\"old\",\"bio\":\"old biography\"}}", &resp);
    free(resp); resp = NULL;

    /* Same insert key = upsert, entering v2_insert_pre_commit's JSON-key
       diff path for every index type. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"routes1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\","
        "\"region\":\"R\\\\D\",\"kind\":\"K\\\\Q\","
        "\"bio\":\"profile said \\\"hi\\\" today\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "\"error\"") == NULL, "upsert succeeds");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\","
        "\"value\":\"He said \\\"hi\\\"\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "single btree JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"eq\",\"value\":\"He said \\\"hi\\\"\"},"
        "{\"field\":\"region\",\"op\":\"eq\",\"value\":\"R\\\\D\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "composite JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"K\\\\Q\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "bitmap JSON-key route matches");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"routes1\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\","
        "\"value\":\"said \\\"hi\\\"\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "trigram JSON-key route matches");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1j. The backward-compatible simple-equality parsers bypass
   parse_one_criterion and therefore need their own coverage (sites 7c/7d). */
static int test_simple_criteria_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"simple1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"He said \\\"hi\\\"\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"simple1\","
        "\"criteria\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "simple query criterion matches decoded value");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\","
        "\"value\":{\"tag\":\"new\"},"
        "\"if\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    ASSERT_TRUE(resp && strstr(resp, "condition_not_met") == NULL,
                "simple CAS condition matches decoded value");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"simple1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"new\"", "simple CAS update committed");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1k. Malformed varchar escapes must be rejected at the documented
   granularity: insert/update/criteria bulk-update are atomic, per-record
   bulk-update skips only its bad record, and malformed IN/NOT_IN criteria
   reject rather than broadening a NOT_IN result set. Covers Tasks 2e, 2f,
   2h, 2i, and 2j. */
static int test_varchar_malformed_escape_policies(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\",\"tag:varchar:16\"]}", &resp);
    free(resp); resp = NULL;

    /* Site 9: insert must reject malformed JSON text instead of storing
       literal backslash bytes and leaving its index/write-side callers out
       of sync. The C literal's \\q produces the invalid JSON escape \q on
       the wire. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"bad\",\"value\":{\"category\":\"bad\\q\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed varchar insert is rejected");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 4: malformed one field rejects the complete single-record
       patch; the otherwise-valid tag field must not be changed. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\","
        "\"value\":{\"category\":\"bad\\q\",\"tag\":\"changed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "single update rejects malformed varchar patch atomically");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "single update leaves malformed field unchanged");
    ASSERT_CONTAINS(resp, "\"tag\":\"old\"", "single update leaves sibling field unchanged");
    free(resp); resp = NULL;

    /* Site 5: the shared criteria bulk-update patch is validated before
       worker dispatch, so no selected record receives even its valid field. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"criteria\":[{\"field\":\"tag\",\"op\":\"eq\",\"value\":\"old\"}],"
        "\"value\":{\"category\":\"bad\\q\",\"tag\":\"changed\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "criteria bulk-update rejects malformed shared patch atomically");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"tag\":\"old\"", "criteria bulk-update writes no partial update");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"key\":\"k2\",\"value\":{\"category\":\"Old\",\"tag\":\"old\"}}", &resp);
    free(resp); resp = NULL;

    /* Site 8: records carry independent patches, so only the bad entry is
       skipped while the valid peer still commits. */
    tc_request(tc,
        "{\"mode\":\"bulk-update\",\"dir\":\"vue\",\"object\":\"bad1\",\"records\":["
        "{\"key\":\"k1\",\"value\":{\"category\":\"bad\\q\"}},"
        "{\"key\":\"k2\",\"value\":{\"category\":\"Good\"}}]}", &resp);
    ASSERT_CONTAINS(resp, "\"matched\":2", "records bulk-update sees both entries");
    ASSERT_CONTAINS(resp, "\"updated\":1", "records bulk-update applies the valid entry");
    ASSERT_CONTAINS(resp, "\"skipped\":1", "records bulk-update skips only the malformed entry");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Old\"", "bad records entry leaves k1 unchanged");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"bad1\",\"key\":\"k2\"}", &resp);
    ASSERT_CONTAINS(resp, "\"category\":\"Good\"", "valid records entry updates k2");
    free(resp); resp = NULL;

    /* Site 7: malformed NOT_IN must reject the criterion, never drop the
       bad element and broaden the result set. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"bad1\","
        "\"criteria\":[{\"field\":\"category\",\"op\":\"not_in\",\"value\":[\"bad\\q\"]}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed NOT_IN element rejects the whole criterion");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1l. Cursor-pagination order_by regression (site 11, round 4): parse_cursor_object
   extracts the order_by value from the client-supplied cursor without decoding
   JSON escapes; resuming from a page whose order_by value contained an
   escaped character must not corrupt the walk's seek position. */
static int extract_cursor_obj(const char *resp, char *out, size_t out_sz) {
    if (!resp) return 0;
    const char *c = SAFE_STRSTR(resp, "\"cursor\":{");
    if (!c) return 0;
    const char *start = c + strlen("\"cursor\":");
    const char *q = start;
    int depth = 0, in_str = 0;
    for (; *q; q++) {
        if (in_str) {
            if (*q == '\\') { q++; continue; }
            if (*q == '"') in_str = 0;
            continue;
        }
        if (*q == '"') { in_str = 1; continue; }
        if (*q == '{') depth++;
        else if (*q == '}') { depth--; if (depth == 0) { q++; break; } }
    }
    size_t n = (size_t)(q - start);
    if (n + 1 > out_sz) n = out_sz - 1;
    memcpy(out, start, n); out[n] = '\0';
    return 1;
}

static int test_cursor_order_by_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"tag:varchar:16\"],\"indexes\":[\"tag\"]}", &resp);
    free(resp); resp = NULL;

    /* Ascending real-content order: "A" < 'Q"' < 'Q#' (0x41 < 0x51,0x22 <
       0x51,0x23). The raw (un-decoded) extraction of 'Q"' is 'Q\"' — an
       extra 0x5C byte landing before the real 0x22 — which sorts strictly
       after 'Q#' (0x51,0x23) too, so a corrupted seek built from that raw
       text skips 'Q#' entirely instead of resuming right after it. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_a\",\"value\":{\"tag\":\"A\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_b\",\"value\":{\"tag\":\"Q\\\"\"}}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"key\":\"k_c\",\"value\":{\"tag\":\"Q#\"}}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":null}", &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_a\"", "page 1 returns k_a");
    char cursor1[256];
    ASSERT_TRUE(extract_cursor_obj(resp, cursor1, sizeof(cursor1)), "page 1 emits a cursor");
    free(resp); resp = NULL;

    char req2[512];
    snprintf(req2, sizeof(req2),
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":%s}", cursor1);
    tc_request(tc, req2, &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_b\"", "page 2 returns k_b (the escaped-quote value)");
    char cursor2[256];
    ASSERT_TRUE(extract_cursor_obj(resp, cursor2, sizeof(cursor2)),
                "page 2 emits a cursor carrying the JSON-escaped order_by value");
    free(resp); resp = NULL;

    /* The bug: resuming from cursor2 (whose "tag" is still JSON-escaped,
       e.g. "Q\"") must decode back to the real value Q" before it's used
       as the walk's seek floor, or k_c gets skipped. limit:2 here (not 1)
       is deliberate: the server emits "cursor":null only when the walk
       drains before hitting the limit (query.c ~7295, `cc.printed <
       limit`), not whenever a page happens to be the last one — with
       only k_c remaining, printed(1) < limit(2) is what actually produces
       null. A limit:1 request would still correctly return k_c but would
       emit a same-position next-page cursor instead of null, since
       printed(1) >= limit(1); that's an existing, intentional cursor
       contract (see test_find_cursor.c's own last-page case, which also
       over-provisions limit for the same reason), not something this
       fix changes. */
    char req3[512];
    snprintf(req3, sizeof(req3),
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":2,"
        "\"cursor\":%s}", cursor2);
    tc_request(tc, req3, &resp);
    ASSERT_CONTAINS(resp, "\"key\":\"k_c\"", "page 3 resumes correctly and returns k_c, not skipped");
    ASSERT_CONTAINS(resp, "\"cursor\":null", "k_c is the last record; cursor closes");
    free(resp); resp = NULL;

    /* Malformed escape in a cursor's order_by value must reject the
       request outright (row 11's documented policy), not seek from a
       corrupted position. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"vue\",\"object\":\"curs1\","
        "\"criteria\":[],\"order_by\":\"tag\",\"order\":\"asc\",\"limit\":1,"
        "\"cursor\":{\"tag\":\"bad\\q\",\"key\":\"k_a\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "malformed cursor order_by escape is rejected");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 1m. Multi-get CSV output regression (site 12, round 4): cmd_get_multi's
   CSV branch extracts field text from an already-JSON-serialized record
   string; a varchar value containing a literal quote must come back
   RFC4180-quoted with the real character, not with its JSON escape bytes
   leaked into the CSV cell. */
static int test_get_multi_csv_varchar_unescape(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"vue\"}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"category:varchar:64\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"key\":\"k1\",\"value\":{\"category\":\"He said \\\"hi\\\"\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"vue\",\"object\":\"csvm1\","
        "\"keys\":[\"k1\"],\"format\":\"csv\"}", &resp);
    ASSERT_CONTAINS(resp, "\"He said \"\"hi\"\"\"",
                    "multi-get CSV decodes the JSON escape and RFC4180-quotes the real character");
    ASSERT_TRUE(strstr(resp, "\\\"hi\\\"") == NULL,
                "multi-get CSV must not leak raw JSON-escape bytes into the cell");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("idx-varchar-find-eq",              test_idx_varchar_find_eq)
TEST_REGISTER("update-varchar-unescape",          test_update_varchar_unescape)
TEST_REGISTER("bulk-update-criteria-varchar-unescape", test_bulk_update_criteria_varchar_unescape)
TEST_REGISTER("bulk-update-json-varchar-unescape",     test_bulk_update_json_varchar_unescape)
TEST_REGISTER("criteria-unindexed-varchar-unescape",   test_criteria_unindexed_varchar_unescape)
TEST_REGISTER("criteria-in-list-varchar-unescape",     test_criteria_in_list_varchar_unescape)
TEST_REGISTER("criteria-between-varchar-unescape",     test_criteria_between_varchar_unescape)
TEST_REGISTER("varchar-embedded-nul-rejected",         test_varchar_embedded_nul_rejected)
TEST_REGISTER("varchar-trailing-backslash",            test_varchar_trailing_backslash)
TEST_REGISTER("upsert-json-index-routes-unescape",     test_upsert_json_index_routes_unescape)
TEST_REGISTER("simple-criteria-varchar-unescape",      test_simple_criteria_varchar_unescape)
TEST_REGISTER("varchar-malformed-escape-policies",     test_varchar_malformed_escape_policies)
TEST_REGISTER("cursor-order-by-varchar-unescape",      test_cursor_order_by_varchar_unescape)
TEST_REGISTER("get-multi-csv-varchar-unescape",        test_get_multi_csv_varchar_unescape)
```

### Verification (before implementing Task 2)

Build with `SKIP_TESTS=1 rtk ./build.sh`, then run each of the fourteen test names above via
`rtk ./build/bin/shard-db-test run <name>` and paste the real failing output. Expected
failure shapes: `idx-varchar-find-eq`/`criteria-unindexed-varchar-unescape` — `find`
returns no match for `k1`; `update-varchar-unescape`/`bulk-update-criteria-varchar-
unescape`/`bulk-update-json-varchar-unescape` — `get` shows double-escaped or
literal-backslash content instead of `He said \"hi\"`; `criteria-in-list-varchar-
unescape`/`criteria-between-varchar-unescape` — `find` misses `k1`;
`varchar-embedded-nul-rejected` — insert currently succeeds (`"inserted"`, no error)
instead of rejecting, because the live insert encoder falls back to raw escaped bytes
when its current unescape attempt fails; `varchar-trailing-backslash` — the parser
mis-bounds a valid JSON string ending in a literal backslash; and
`varchar-malformed-escape-policies` — insert/update/criteria-bulk paths currently
accept or partially apply malformed varchar text, while records bulk-update does not
yet skip only the malformed entry and malformed `NOT_IN` does not reliably reject.
`upsert-json-index-routes-unescape` must fail on at least one of its single/composite/
bitmap/trigram assertions before the fix; `simple-criteria-varchar-unescape` must fail
its simple-query and simple-CAS assertions; **[round 4, new]**
`cursor-order-by-varchar-unescape` — page 3 either omits `k_c` or the malformed-cursor
request is accepted instead of rejected; `get-multi-csv-varchar-unescape` — the CSV cell
contains the raw `\"hi\"` escape bytes instead of the decoded, RFC4180-doubled quotes.

**Per the project's test-first requirement**: after Task 2 lands, re-run all fourteen names
and confirm they pass. Then temporarily revert the Task 2 code changes only (leave the
new tests in place), rebuild, run the same fourteen tests, and paste the expected failures.
Re-apply the exact Task 2 diff, rebuild, re-run the fourteen tests, and paste the passing
output. This post-implementation revert/fail/reapply/pass proof is required in addition
to the initial pre-fix failures; do not continue to Tasks 3–7 until it is complete.

**[round 4 note]** If Tasks 2a-2k (the original ten sites) already have this
revert/fail/reapply/pass proof on file from before this plan's scope expanded,
that proof does not need to be redone — round 4 only adds sites 11/12 (Tasks 2l/2m),
so the revert/reapply cycle for *those two* is what's new. The full fourteen-test
`run-all` pass at the end of this section still must include the pre-existing ten,
since 2l/2m's implementation must not regress them.

## Task 2 — Implement the fix

### 2a. Add `json_unescape_cstring`, the type-gated extractor, and the shared boundary-scan helper

**[round 2 fix]** This subsection replaces round 1's approach of modifying
`json_unescape_string` itself. That was checked against every actual caller and found
too broad — see the NUL-rejection paragraph in "Design decision" above for the full
reasoning. The fix here is narrower: a new wrapper, `json_unescape_cstring`, used only
by the new/changed extraction paths in this plan; `json_unescape_string` and its one
existing external caller (`json_obj_strdup_unescaped`, used by `server.c:1720`'s
bulk-insert-delimited `data` field) are untouched.

File: `src/db/util.c` — add `json_unescape_cstring` right after `json_unescape_string`
(~line 489). File: `src/db/types.h` — add the declaration right after
`json_unescape_string`'s prototype (~line 661):

```c
/* Same contract as json_unescape_string, plus one additional check: the
   decoded result must not contain an embedded NUL byte (i.e.
   strlen(*out_buf) == *out_len). This is the sanctioned decode call for
   any new extraction path whose result is subsequently treated as a
   plain NUL-terminated C string (via strlen(), strncpy(), strcmp(),
   etc.) rather than as an explicit (bytes, length) pair — every call
   site in this plan is exactly that shape. json_unescape_string itself
   is deliberately left unchanged: its one caller outside this plan,
   json_obj_strdup_unescaped (server.c's bulk-insert-delimited `data`
   field), is genuinely length-aware and must keep tolerating an
   embedded NUL. Returns 0 and sets *out_buf/*out_len on success; returns
   -1 (freeing any partial allocation) on a malformed escape sequence OR
   an embedded NUL. */
int json_unescape_cstring(const char *in, size_t in_len,
                          char **out_buf, size_t *out_len);
```

Implementation (`util.c`):

```c
int json_unescape_cstring(const char *in, size_t in_len,
                          char **out_buf, size_t *out_len) {
    if (json_unescape_string(in, in_len, out_buf, out_len) != 0) return -1;
    if (strlen(*out_buf) != *out_len) {
        free(*out_buf);
        *out_buf = NULL;
        *out_len = 0;
        return -1;
    }
    return 0;
}
```

**[round 2 fix — type-gated extractor]** File: `src/db/util.c` — add
`json_get_fields_unescaped` near `json_get_fields`/`json_obj_strdup_unescaped`. File:
`src/db/types.h` — add the declaration near the `json_get_fields` prototype (~line 691):

```c
/* Same contract as json_get_fields, but every extracted field whose
   schema type is FT_VARCHAR is additionally passed through
   json_unescape_cstring before being returned. Every other type's
   out[i] is left exactly as json_get_fields produced it (raw, quotes
   stripped, no unescape) — decoding a non-varchar type would silently
   re-include FT_ENUM (and every numeric/temporal type) in this plan's
   scope, which is explicitly out of scope; see "Field-type scope".
   field_types[i] is the schema type of keys[i] (index-aligned with
   keys/out) and is a required parameter, not optional — every call
   site in this plan already has a TypedSchema/TypedField array in
   scope to build it from.
   Returns 0 if every FT_VARCHAR field that was present in the JSON
   decoded successfully. Returns -1 if at least one PRESENT FT_VARCHAR
   field had a malformed escape sequence (including an embedded NUL) —
   that field's out[i] is still set to NULL (matching json_get_fields'
   "field not found" contract so existing NULL-guards keep working),
   but callers doing a WRITE must not treat a nonzero return the same as
   "field simply wasn't supplied": a present-but-corrupt field silently
   dropped would look to the client like a successful partial update of
   a field they explicitly named. Each call site in this plan documents
   its own policy for this return value; it is not uniform. */
int json_get_fields_unescaped(const char *json, const char **keys, int nkeys,
                              const enum FieldType *field_types, char **out);
```

Implementation (`util.c`):

```c
int json_get_fields_unescaped(const char *json, const char **keys, int nkeys,
                              const enum FieldType *field_types, char **out) {
    json_get_fields(json, keys, nkeys, out);
    int rc = 0;
    for (int i = 0; i < nkeys; i++) {
        if (!out[i]) continue;
        if (field_types[i] != FT_VARCHAR) continue;
        char *unesc = NULL; size_t ulen = 0;
        if (json_unescape_cstring(out[i], strlen(out[i]), &unesc, &ulen) != 0) {
            free(out[i]);
            out[i] = NULL;
            rc = -1;
            continue;
        }
        free(out[i]);
        out[i] = unesc;
    }
    return rc;
}
```

**[round 2 fix — shared boundary-scan helper, findings 3 and 5]** Round 1 put
`json_raw_string_end` locally in `query_plan.c` as a `static` helper for
`parse_one_criterion`'s own splitters. The review pointed out the *shared* JSON scanner
has the identical bug in three internal copies inside `json_skip_value` (`util.c:135`,
`:149`, `:160` — each a naive `!(*p=='"' && *(p-1)!='\\')` check, wrong for an even
number of trailing backslashes, e.g. a value ending in a literal `\`). `json_skip_value`
is the boundary-detection primitive `json_get_fields` and `parse_criteria_json` both
build on, so this fix promotes the helper into `util.c` as a shared, non-`static`
function and uses it inside `json_skip_value` too, instead of duplicating the fix.

File: `src/db/util.c` — add above `json_skip_value` (~line 131). File: `src/db/types.h`
— add the declaration right after `json_skip_value`'s prototype (~line 608).

Declaration (`types.h`):

```c
const char *json_raw_string_end(const char *p);
```

Implementation (`util.c`):

```c
/* Scans forward from p (which must point at the first byte after an
   opening '"') over a raw, still-JSON-escaped string span, stopping at
   the first UNESCAPED closing '"'. An escaped quote (\") is skipped as
   a pair, not mistaken for the closing delimiter — this correctly
   handles a run of trailing backslashes of any length (an odd count
   means the last one escapes the quote; an even count means the quote
   is unescaped), unlike the naive "previous byte != backslash" check it
   replaces. Returns a pointer to the closing '"' (or the terminating
   NUL if the span never closes). */
const char *json_raw_string_end(const char *p) {
    while (*p && *p != '"') {
        if (*p == '\\' && p[1]) p += 2;
        else p++;
    }
    return p;
}
```

Anchor (`util.c`, `json_skip_value`, current code):

```c
const char *json_skip_value(const char *p) {
    p = json_skip(p);
    if (*p == '"') {
        p++;
        while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
        if (*p == '"') p++;
        return p;
    }
    /* The inner string-skip (the `"` branch inside) walks until the closing
       quote OR end-of-buffer. If it ran out at end-of-buffer without finding
       a close, we MUST NOT then `p++` past the NUL terminator — that's a
       heap-buffer-overflow on the next loop check. The `if (!*p) break;`
       guard covers that. Found by libFuzzer; see fuzz/fuzz_json.c. */
    if (*p == '{') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            else if (*p == '"') { p++; while (*p && !(*p == '"' && *(p-1) != '\\')) p++; }
            if (!*p) break;
            p++;
        }
        return p;
    }
    if (*p == '[') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '[') depth++;
            else if (*p == ']') depth--;
            else if (*p == '"') { p++; while (*p && !(*p == '"' && *(p-1) != '\\')) p++; }
            if (!*p) break;
            p++;
        }
        return p;
    }
    while (*p && *p != ',' && *p != '}' && *p != ']' && *p != ' ' && *p != '\n') p++;
    return p;
}
```

Replace with:

```c
const char *json_skip_value(const char *p) {
    p = json_skip(p);
    if (*p == '"') {
        p++;
        p = json_raw_string_end(p);
        if (*p == '"') p++;
        return p;
    }
    /* The inner string-skip (the `"` branch inside) walks until the closing
       quote OR end-of-buffer. If it ran out at end-of-buffer without finding
       a close, we MUST NOT then `p++` past the NUL terminator — that's a
       heap-buffer-overflow on the next loop check. The `if (!*p) break;`
       guard covers that. Found by libFuzzer; see fuzz/fuzz_json.c. */
    if (*p == '{') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            else if (*p == '"') { p++; p = json_raw_string_end(p); }
            if (!*p) break;
            p++;
        }
        return p;
    }
    if (*p == '[') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '[') depth++;
            else if (*p == ']') depth--;
            else if (*p == '"') { p++; p = json_raw_string_end(p); }
            if (!*p) break;
            p++;
        }
        return p;
    }
    while (*p && *p != ',' && *p != '}' && *p != ']' && *p != ' ' && *p != '\n') p++;
    return p;
}
```

`parse_one_criterion` (`query_plan.c`) uses this same shared `json_raw_string_end` for
its own two hand-rolled splitters (the `IN`/`NOT_IN` element boundary scan and the
legacy `BETWEEN` two-element array-form split) — see Task 2h; no local `static` copy is
added there.

**Explicit descope — eight other duplicate-pattern occurrences.** The same
`!(*p=='"' && *(p-1)!='\\')` pattern also appears, unfixed, at: `util.c:188`
(`json_get_fields`'s field-*name* scan), `util.c:368` (a second field-name scan),
`config.c:2204` and `config.c:2643` (`typed_encode`/`typed_encode_defaults`'s field-name
scans), `query_bulk.c:2422` (a record-key array-element scan), `query_plan.c:1442` and
`query_plan.c:1857` (further field-name scans), and `storage.c:2048` (a record-key
array-element scan). All eight scan protocol *keys* (field names, record keys) rather
than field *values* — a key containing a trailing backslash is not a scenario this plan's
bug reports concern, and fixing all eight is a strictly larger, separable change (it
touches key-parsing in five different files' hot paths, not the value-escaping bug this
plan exists to fix). They are listed here, with file:line citations, so a future pass can
find and fix them without re-discovering the pattern from scratch — same treatment as
the FT_ENUM descope in Task 3.

### 2b. `index.c:648` (`index_parallel`)

**[round 2 fix]** Round 1 planned to swap the extraction call for
`json_get_fields_unescaped` directly. That is no longer correct on its own:
`json_get_fields_unescaped` (Task 2a) needs a `field_types` array, and building one here
requires resolving each unique key's `TypedField` first — which this function's
composite/single-field loops already do individually, via `typed_field_index`, further
down. Rather than resolve every field twice (once to build `field_types`, once again in
the existing loops), this fix decodes in place, once per unique key, immediately after
extraction, gated on the same `typed_field_index` lookup the rest of the function
already relies on. The existing composite-key loop and single-field branch (both further
down in this function) are completely unchanged — they keep consuming `extracted[j]`
exactly as before, they just now see already-decoded text for `FT_VARCHAR` fields.

Anchor:

```c
    char *extracted[MAX_FIELDS * 4];
    json_get_fields(value, unique_keys, unique_count, extracted);
```

Replace with:

```c
    char *extracted[MAX_FIELDS * 4];
    json_get_fields(value, unique_keys, unique_count, extracted);
    for (int j = 0; j < unique_count; j++) {
        if (!extracted[j]) continue;
        int fidx = ts ? typed_field_index(ts, unique_keys[j]) : -1;
        if (fidx < 0 || ts->fields[fidx].type != FT_VARCHAR) continue;
        char *unesc = NULL; size_t ulen = 0;
        errno = 0;
        if (json_unescape_cstring(extracted[j], strlen(extracted[j]), &unesc, &ulen) != 0) {
            /* The record encoder already validated this exact JSON before
               pre_commit. A later failure can still be OOM; it must abort
               the pre_commit, never silently omit this index key. */
            int decode_errno = (errno == ENOMEM) ? ENOMEM : EINVAL;
            for (int k = 0; k < unique_count; k++) free(extracted[k]);
            for (int k = 0; k < unique_count; k++) {
                int is_field = 0;
                for (int m = 0; m < nfields; m++)
                    if (unique_keys[k] == fields[m]) { is_field = 1; break; }
                if (!is_field) free((char *)unique_keys[k]);
            }
            errno = decode_errno;
            return -1;
        }
        free(extracted[j]);
        extracted[j] = unesc;
    }
```

### 2c. `index.c:890` (`build_index_key_from_json`, composite branch)

**[round 2 fix]** Same `field_types` requirement as Task 2b: build a per-sub-field type
array from the same `typed_field_index` lookup the loop just below already performs, so
a sub-field is decoded only when it's known to the schema *and* `FT_VARCHAR`. A
sub-field the schema doesn't know about (`ts == NULL`, or `fi < 0` for a legacy untyped
object) is left exactly as `json_get_fields` produced it — unchanged behavior for that
case, matching the existing "no schema → fall back to raw string" comment a few lines
below.

Anchor:

```c
        char *vals[16];
        json_get_fields(json, subs, nsub, vals);
```

Replace with:

```c
        char *vals[16];
        enum FieldType sub_types[16];
        for (int i = 0; i < nsub; i++) {
            int fi = ts ? typed_field_index(ts, subs[i]) : -1;
            /* FT_COUNT is a sentinel that can never equal FT_VARCHAR;
               unknown/legacy fields therefore retain json_get_fields'
               original raw-text behavior. */
            sub_types[i] = (fi >= 0) ? ts->fields[fi].type : FT_COUNT;
        }
        errno = 0;
        if (json_get_fields_unescaped(json, subs, nsub, sub_types, vals) != 0) {
            /* Upstream content validation cannot guarantee this second
               allocation succeeds. Preserve the index/record invariant by
               returning the tri-state error, not "field absent". */
            for (int i = 0; i < nsub; i++) free(vals[i]);
            if (errno != ENOMEM) errno = EINVAL;
            return -1;
        }
```

### 2d. `index.c:921` (`build_index_key_from_json`, single-field branch)

**[round 2 fix]** Round 1 swapped in `json_obj_strdup_unescaped` unconditionally — that
decodes regardless of field type, silently re-including `FT_ENUM` (and every other
type) in scope, and it uses the general-purpose (NUL-tolerant) decoder rather than
`json_unescape_cstring`. This fix instead moves the `typed_field_index` lookup (`fi`,
currently computed a few lines below, after extraction) up to *before* extraction, and
branches the extraction call itself on whether the resolved type is `FT_VARCHAR` — the
later, now-redundant `int fi = ts ? typed_field_index(ts, spec) : -1;` a few lines below
is removed (reuse the one computed here instead).

Anchor:

```c
    JsonObj jo;
    json_parse_object(json, strlen(json), &jo);
    char *txt = json_obj_strdup(&jo, spec);
    if (!txt || !txt[0]) { free(txt); return 0; }

    int fi = ts ? typed_field_index(ts, spec) : -1;
    if (fi >= 0) {
```

Replace with:

```c
    JsonObj jo;
    json_parse_object(json, strlen(json), &jo);
    int fi = ts ? typed_field_index(ts, spec) : -1;
    char *txt;
    if (fi >= 0 && ts->fields[fi].type == FT_VARCHAR) {
        const char *v; size_t vl;
        if (!json_obj_unquoted(&jo, spec, &v, &vl)) return 0;
        size_t ulen = 0;
        errno = 0;
        if (json_unescape_cstring(v, vl, &txt, &ulen) != 0) {
            if (errno != ENOMEM) errno = EINVAL;
            return -1;
        }
    } else {
        txt = json_obj_strdup(&jo, spec);
    }
    if (!txt || !txt[0]) { free(txt); return 0; }

    if (fi >= 0) {
```

(The later, now-redundant `int fi = ts ? typed_field_index(ts, spec) : -1;` a few lines
below this — right before `if (fi >= 0) { const TypedField *f = &ts->fields[fi]; ...`
in the original — is deleted; the `fi >= 0` branch below reuses the `fi` computed
above.)

**[round 3 fix — tri-state contract and all consumers]** Update the declaration comment
in `types.h` so callers cannot confuse decode/allocation failure with absence.

Anchor:

```c
   Returns 1 on success with malloc'd *out_val and *out_len set; caller
   frees. ts may be NULL (untyped object) — in that case every field is
   treated as raw varchar text. Returns 0 on missing/empty values. */
```

Replace with:

```c
   Returns 1 on success with malloc'd *out_val and *out_len set; caller
   frees. ts may be NULL (untyped object) — in that case every field is
   treated as raw varchar text. Returns 0 only for a genuinely missing or
   empty value. Returns -1 on decode/allocation failure with errno set; a
   pre-commit caller must abort rather than treating that as field absence. */
```

`build_index_key_from_json` has exactly four production call sites, all in
`storage.c`'s `v2_insert_pre_commit`: new and old keys in the upsert-diff branch, plus
fresh bitmap and trigram keys. Patch every one.

Anchor in the upsert-diff loop:

```c
            int have_new = build_index_key_from_json(c->idx_ts, c->value_json,
                                                     c->fields[i], &new_key, &new_len);
            int have_old = old_json
                ? build_index_key_from_json(c->idx_ts, old_json,
                                            c->fields[i], &old_key, &old_len)
                : 0;
```

Replace with:

```c
            int have_new = build_index_key_from_json(c->idx_ts, c->value_json,
                                                     c->fields[i], &new_key, &new_len);
            int have_old = old_json
                ? build_index_key_from_json(c->idx_ts, old_json,
                                            c->fields[i], &old_key, &old_len)
                : 0;
            if (have_new < 0 || have_old < 0) {
                free(new_key); free(old_key);
                for (int j = 0; j < n_args; j++) {
                    free(args[j].new_key);
                    free(args[j].old_key);
                }
                free(old_json);
                snprintf(c->err_buf, sizeof(c->err_buf),
                         "index-key decode failed during insert/update: %s",
                         strerror(errno ? errno : EIO));
                return -1;
            }
```

The existing boolean comparisons below are now safe because only `0` or `1` remains.

In the fresh bitmap loop, replace:

```c
                if (!build_index_key_from_json(c->idx_ts, c->value_json,
                                                c->fields[i], &nk, &nl))
                    continue;
```

with:

```c
                int key_rc = build_index_key_from_json(c->idx_ts, c->value_json,
                                                       c->fields[i], &nk, &nl);
                if (key_rc < 0) {
                    for (int j = 0; j < n_bm; j++) free(bm_args[j].new_key);
                    snprintf(c->err_buf, sizeof(c->err_buf),
                             "bitmap index-key decode failed during insert: %s",
                             strerror(errno ? errno : EIO));
                    return -1;
                }
                if (key_rc == 0) continue;
```

In the fresh trigram loop, replace its identical two-line call/continue block with:

```c
                int key_rc = build_index_key_from_json(c->idx_ts, c->value_json,
                                                       c->fields[i], &nk, &nl);
                if (key_rc < 0) {
                    for (int j = 0; j < n_tg; j++) free(tg_args[j].new_key);
                    snprintf(c->err_buf, sizeof(c->err_buf),
                             "trigram index-key decode failed during insert: %s",
                             strerror(errno ? errno : EIO));
                    return -1;
                }
                if (key_rc == 0) continue;
```

### 2e. `storage.c:1167` (single-record `update`) — atomic reject on malformed escape

Anchor:

```c
    const char *field_names[MAX_FIELDS];
    char *field_vals[MAX_FIELDS] = {0};
    for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;
    json_get_fields(partial_json, field_names, ts->nfields, field_vals);
```

Replace with:

```c
    const char *field_names[MAX_FIELDS];
    char *field_vals[MAX_FIELDS] = {0};
    enum FieldType field_types[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) {
        field_names[i] = ts->fields[i].name;
        field_types[i] = ts->fields[i].type;
    }
    if (json_get_fields_unescaped(partial_json, field_names, ts->nfields, field_types, field_vals) != 0) {
        /* At least one field the client explicitly named had a malformed
           JSON escape. Reject the whole update rather than silently
           applying every other field and dropping this one — a partial
           write here would look like success to the caller. */
        for (int i = 0; i < ts->nfields; i++) free(field_vals[i]);
        free(new_buf);
        OUT("{\"error\":\"malformed JSON escape in one or more field values\"}\n");
        return 1;
    }
```

Check the varchar length-cap validation a few lines below (`content_max` /
`vlen = strlen(field_vals[i])`) still reads `field_vals[i]` *after* this change —
it should, since it's the same array, now holding decoded content — decoded content
should generally be no longer than its escaped source, so this only makes the cap check
more accurate, never spuriously rejects something that was previously accepted.

### 2f. `query_bulk.c` — bulk-update (criteria+value shape): upfront validation + extractor swap

**[review fix — atomicity]** `bulk_upd_shard_worker_v2` parses the *shared* `value_json`
patch **once per shard worker** — the same patch is applied identically to every matched
record in that shard (confirmed: `json_get_fields(w->value_json, ...)` at
query_bulk.c:2898 runs inside the per-shard worker, not per-record). Because
`parallel_for_io` dispatches all shard workers **concurrently**, checking the decode
result only inside the worker cannot give true whole-request atomicity — by the time one
worker detects a malformed field, other workers may already have applied their portion.
The check must happen **once, upfront, in `cmd_bulk_update`**, before Phase 2 dispatch —
this function already has exactly this shape of upfront validation for `criteria` and
`if_json` (see the existing `{"error":"bad criteria: ...\"}"` and
`{"error":"invalid if condition\"}` checks).

Anchor (`query_bulk.c`, `cmd_bulk_update`; include the start of Phase 1 so
`FieldSchema fs` is definitely initialized before the new validation uses `fs.ts`):

```c
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0] &&
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        free_criteria_tree(tree);
        return 1;
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
```

Replace with:

```c
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0] &&
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        free_criteria_tree(tree);
        return 1;
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
```

After the existing timeout/budget/dry-run returns and immediately before Phase 2, decode
the shared patch once. Delaying allocation until here avoids adding cleanup to every
Phase-1 return while still guaranteeing no write worker has started.

Anchor:

```c
    TypedSchema *ts = load_typed_schema(db_root, object);
```

Replace with:

```c
    TypedSchema *ts = fs.ts;
    const char *value_field_names[MAX_FIELDS];
    enum FieldType value_field_types[MAX_FIELDS];
    char *shared_field_vals[MAX_FIELDS] = {0};
    for (int i = 0; i < ts->nfields; i++) {
        value_field_names[i] = ts->fields[i].name;
        value_field_types[i] = ts->fields[i].type;
    }
    if (json_get_fields_unescaped(value_json, value_field_names, ts->nfields,
                                  value_field_types, shared_field_vals) != 0) {
        for (int i = 0; i < ts->nfields; i++) free(shared_field_vals[i]);
        OUT("{\"error\":\"malformed JSON escape in one or more field values\"}\n");
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys);
        free_criteria_tree(tree);
        return 1;
    }
```

Add the shared decoded array to the worker struct. Anchor:

```c
    const char    *value_json;
    const char   (*idx_fields)[256];
```

Replace with:

```c
    const char    *value_json;
    char         **field_vals;  /* shared read-only decoded patch; cmd owns */
    const char   (*idx_fields)[256];
```

Anchor in the worker initialization loop:

```c
        workers[wi].value_json = value_json;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
```

Replace with:

```c
        workers[wi].value_json = value_json;
        workers[wi].field_vals = shared_field_vals;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
```

Finally, remove the worker's second parse/allocation. Anchor
(`bulk_upd_shard_worker_v2`):

Anchor (`query_bulk.c:2898`, inside `bulk_upd_shard_worker_v2`):

```c
    /* Parse value_json once for the whole worker. */
    const char *field_names[MAX_FIELDS];
    char       *field_vals[MAX_FIELDS];
    for (int fi = 0; fi < w->ts->nfields; fi++) field_names[fi] = w->ts->fields[fi].name;
    json_get_fields(w->value_json, field_names, w->ts->nfields, field_vals);
```

Replace with:

```c
    /* cmd_bulk_update decoded this shared patch exactly once before any
       worker was dispatched. Workers only read it. */
    char **field_vals = w->field_vals;
```

Keep `ctxs[ki].field_vals = field_vals;` unchanged. In the worker allocation-failure
branch, replace:

```c
        free(batch); free(ctxs); free(scratch);
        for (int fi = 0; fi < w->ts->nfields; fi++) free(field_vals[fi]);
        w->skipped += w->count;
        return NULL;
```

with:

```c
        free(batch); free(ctxs); free(scratch);
        w->skipped += w->count;
        return NULL;
```

At the worker epilogue, replace:

```c
    free(batch); free(ctxs); free(scratch);
    free(old_arena); free(new_arena);
    for (int fi = 0; fi < w->ts->nfields; fi++) free(field_vals[fi]);
    return NULL;
```

with:

```c
    free(batch); free(ctxs); free(scratch);
    free(old_arena); free(new_arena);
    return NULL;
```

After `parallel_for_io` returns and all workers are joined, add the owner cleanup in
`cmd_bulk_update` immediately after this anchor:

```c
    free(workers);
```

Replacement:

```c
    free(workers);
    for (int i = 0; i < ts->nfields; i++) free(shared_field_vals[i]);
```

### 2g. `query_plan.c:1236`/`1238` (`parse_one_criterion`, scalar `value`/`value2`)

**[review fix — extraction stays raw; decode moves to the commit point, after any
array-form structural parsing]** Do **not** change the extraction calls themselves —
`v`/`v_raw`/`v2` must stay raw so the `BETWEEN`/`LEN_BETWEEN` array-form split (which
runs on `v` a few lines below) sees intact structural delimiters. Anchor (unchanged
from today — confirming no edit here, listed for clarity):

```c
    char *f     = json_obj_strdup(&cobj, "field");
    char *o     = json_obj_strdup(&cobj, "op");
    if (!o) o = json_obj_strdup(&cobj, "operator");   /* alias */
    char *v     = json_obj_strdup(&cobj, "value");
    char *v_raw = json_obj_strdup_raw(&cobj, "value");
    char *v2    = json_obj_strdup(&cobj, "value2");
```

**No change here.** `f`/`o` (protocol/control keys) and `v`/`v_raw`/`v2` (raw, needed
for structural parsing below) are all correct as-is.

Now fix the `BETWEEN`/`LEN_BETWEEN` array-form split's inner quote scan (site 7b).
Anchor:

```c
            if (*ap == '"') {
                ap++;
                start = ap;
                while (*ap && *ap != '"') ap++;
                plen = (size_t)(ap - start);
                if (*ap == '"') ap++;
            } else {
```

Replace with:

```c
            if (*ap == '"') {
                ap++;
                start = ap;
                ap = json_raw_string_end(ap);
                plen = (size_t)(ap - start);
                if (*ap == '"') ap++;
            } else {
```

`parts[0]`/`parts[1]` now hold correctly-bounded but still-*raw* (escaped) sub-spans
when the array-form path is taken. The existing few lines after that already do
`v = strdup(parts[0]); v2 = strdup(parts[1]);` — unchanged, still raw at this point.

Finally, add **one** unescape step that covers both origins of `v`/`v2` — the
just-described array-form split, and the plain-scalar case where the array-form branch
was never entered at all. Anchor (immediately before the existing `strncpy(c->value,
v, ...)` line — locate via the "Validate value2" block a few lines above it):

```c
    /* Validate value2 — between/len_between need both bounds; a missing
       value2 must not silently fall back to an empty-string bound. */
    if ((c->op == OP_BETWEEN || c->op == OP_LEN_BETWEEN) && (!v2 || v2[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }
```

Replace with (adds the decode step right after this existing check, before `v`/`v2`
are committed to `c->value`/`c->value2`):

```c
    /* Validate value2 — between/len_between need both bounds; a missing
       value2 must not silently fall back to an empty-string bound. */
    if ((c->op == OP_BETWEEN || c->op == OP_LEN_BETWEEN) && (!v2 || v2[0] == '\0')) {
        free(v); free(v_raw); free(v2);
        return -1;
    }

    /* v/v2 are still raw JSON-escaped text here, regardless of whether
       they came from a plain scalar "value" or from the array-form
       BETWEEN split above. Decode now, in the one place both origins
       funnel through, right before they're committed to the criterion —
       [round 2 fix] except for OP_IN/OP_NOT_IN, whose `v` is never used
       for matching (in_values holds the decoded elements instead — see
       Task 2h) and is about to be re-consumed raw by the legacy
       comma-separated fallback branch a few lines below (`iv =
       strdup(v)`); decoding it here as well would double-decode that
       shape. json_unescape_cstring (not json_unescape_string) is used
       here — v/v2 are committed into c->value/c->value2, fixed-size
       C-string buffers compared with strcmp/strncmp elsewhere, so an
       embedded NUL must be rejected the same as a malformed escape.
       A malformed escape makes the whole criterion unparseable, same as
       any other invalid-criterion case this function already rejects.
       [round 3 fix — found by full-suite run, test-regex 7/24 failing]
       OP_REGEX/OP_NOT_REGEX must also be excluded: `c->value` for these
       ops is raw POSIX-regex source consumed by regcomp()/regexec()
       (query.c's criteria_match), not JSON string content. The wire
       protocol (documented in test_regex.c's header comment, predating
       this plan) has callers write regex metachars — `\.`, `\+`, `\d` —
       with a single literal backslash. None of those are valid JSON
       escapes, so running them through json_unescape_cstring either
       corrupts the pattern (e.g. `\+` silently becomes `+`, changing
       regex semantics) or, since 'unknown escape' is a hard reject,
       fails the whole criterion with "invalid criterion". Confirmed via
       run-all: 7 of test-regex's 24 assertions failed before this
       exclusion (all backslash-metachar patterns), 0 after. */
    if (v && c->op != OP_IN && c->op != OP_NOT_IN &&
        c->op != OP_REGEX && c->op != OP_NOT_REGEX) {
        char *unesc = NULL; size_t ulen = 0;
        if (json_unescape_cstring(v, strlen(v), &unesc, &ulen) != 0) {
            free(v); free(v_raw); free(v2);
            return -1;
        }
        free(v); v = unesc;
    }
    if (v2) {
        char *unesc2 = NULL; size_t ulen2 = 0;
        if (json_unescape_cstring(v2, strlen(v2), &unesc2, &ulen2) != 0) {
            free(v); free(v_raw); free(v2);
            return -1;
        }
        free(v2); v2 = unesc2;
    }
```

Note `f`/`o` (protocol/control keys — field name and operator keyword) are deliberately
**not** touched, per the "one central rule" in the Design decision section — they're
matched against schema field names and a fixed keyword table, not decoded record
content. `v_raw` is also deliberately unchanged — it must stay raw for the `IN`/
`NOT_IN` splitter below (2h), which runs independently of this scalar/`BETWEEN` path.
`v` for `OP_REGEX`/`OP_NOT_REGEX` is likewise left raw (round 3 fix above) — regex
pattern source, not decoded record content, is out of scope for this plan's JSON-
unescape rule by the same logic as `f`/`o`.

### 2h. `query_plan.c:1339`/`1355` (`parse_one_criterion`, `IN`/`NOT_IN` element split)

**[round 2 fix]** Round 1's design here had two bugs the review caught: (1) it decoded
the quoted-element branch a second time on top of Task 2g's (then-unconditional) decode
of `v`, and separately left the legacy comma-fallback branch's own unescape step
unspecified as prose rather than literal code; (2) "drop just the malformed element" is
wrong for `NOT_IN` — silently dropping one value from a `NOT_IN` list *broadens* the
match set (every record holding that value now incorrectly passes), which is a
correctness/security regression, not a graceful degradation. Task 2g now skips
decoding `v` entirely for `OP_IN`/`OP_NOT_IN` (so there is exactly one decode per
representation here), and both branches below now reject the **whole criterion** on any
malformed element, exactly like every other invalid-criterion case `parse_one_criterion`
already handles via `return -1`. This replaces the entire `OP_IN`/`OP_NOT_IN` block in
one piece (rather than as fragments) since the two branches share the reject-on-failure
cleanup and are easiest to get right read together.

Anchor — the entire block, current code:

```c
        if (c->op == OP_IN || c->op == OP_NOT_IN) {
            c->in_cap = 64;
            c->in_values = malloc(c->in_cap * sizeof(char *));
            const char *ap = v_raw ? v_raw : v;
            if (*ap == '"') ap++;
            if (*ap == '[') {
                ap++;
                while (*ap) {
                    while (*ap == ' ' || *ap == ',') ap++;
                    /* The skip-ws/comma loop can advance ap to the NUL
                       terminator if the input ends with a trailing comma
                       and no closing ']' (the upstream json_skip_value
                       can be tricked into truncating the span at an
                       embedded NUL — see fuzzer-found bug). Without this
                       guard, the else `ap++` below walks past NUL → OOB
                       read on next iteration. */
                    if (!*ap) break;
                    if (*ap == ']') break;
                    if (*ap == '"') {
                        ap++;
                        const char *start = ap;
                        while (*ap && *ap != '"') ap++;
                        size_t len = ap - start;
                        if (c->in_count >= c->in_cap) {
                            int new_cap = c->in_cap * 2;
                            char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                            if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                            c->in_values = t;
                            c->in_cap = new_cap;
                        }
                        char *val = malloc(len + 1);
                        memcpy(val, start, len); val[len] = '\0';
                        c->in_values[c->in_count++] = val;
                        if (*ap == '"') ap++;
                    } else {
                        const char *start = ap;
                        while (*ap && *ap != ',' && *ap != ']' && *ap != ' ') ap++;
                        size_t len = ap - start;
                        if (len > 0) {
                            if (c->in_count >= c->in_cap) {
                                int new_cap = c->in_cap * 2;
                                char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                                if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                                c->in_values = t;
                                c->in_cap = new_cap;
                            }
                            char *val = malloc(len + 1);
                            memcpy(val, start, len); val[len] = '\0';
                            c->in_values[c->in_count++] = val;
                        }
                    }
                }
            } else {
                char *iv = strdup(v);
                char *_tok_save = NULL; char *tok = strtok_r(iv, ",", &_tok_save);
                while (tok) {
                    if (c->in_count >= c->in_cap) {
                        int new_cap = c->in_cap * 2;
                        char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                        if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                        c->in_values = t;
                        c->in_cap = new_cap;
                    }
                    c->in_values[c->in_count++] = strdup(tok);
                    tok = strtok_r(NULL, ",", &_tok_save);
                }
                free(iv);
            }
        }
```

Replace with:

```c
        if (c->op == OP_IN || c->op == OP_NOT_IN) {
            c->in_cap = 64;
            c->in_values = malloc(c->in_cap * sizeof(char *));
            const char *ap = v_raw ? v_raw : v;
            if (*ap == '"') ap++;
            if (*ap == '[') {
                ap++;
                while (*ap) {
                    while (*ap == ' ' || *ap == ',') ap++;
                    if (!*ap) break;
                    if (*ap == ']') break;
                    if (*ap == '"') {
                        ap++;
                        const char *start = ap;
                        ap = json_raw_string_end(ap);
                        size_t len = ap - start;
                        char *raw = malloc(len + 1);
                        memcpy(raw, start, len); raw[len] = '\0';
                        char *val = NULL; size_t ulen = 0;
                        if (json_unescape_cstring(raw, len, &val, &ulen) != 0) {
                            /* Malformed escape in one IN/NOT_IN element:
                               reject the WHOLE criterion rather than
                               dropping just this element — dropping from
                               NOT_IN would silently broaden the match set,
                               a correctness/security regression, not a
                               graceful degradation. */
                            free(raw);
                            for (int _i = 0; _i < c->in_count; _i++) free(c->in_values[_i]);
                            free(c->in_values);
                            c->in_values = NULL;
                            c->in_count = 0;
                            c->in_cap = 0;
                            free(v); free(v_raw); free(v2);
                            return -1;
                        }
                        free(raw);
                        if (c->in_count >= c->in_cap) {
                            int new_cap = c->in_cap * 2;
                            char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                            if (!t) { free(val); c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                            c->in_values = t;
                            c->in_cap = new_cap;
                        }
                        c->in_values[c->in_count++] = val;
                        if (*ap == '"') ap++;
                    } else {
                        /* Unquoted array elements are bare numeric/keyword
                           tokens (JSON strings are always quoted) — they
                           can never contain a JSON escape sequence, so no
                           decode step is needed here. */
                        const char *start = ap;
                        while (*ap && *ap != ',' && *ap != ']' && *ap != ' ') ap++;
                        size_t len = ap - start;
                        if (len > 0) {
                            if (c->in_count >= c->in_cap) {
                                int new_cap = c->in_cap * 2;
                                char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                                if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                                c->in_values = t;
                                c->in_cap = new_cap;
                            }
                            char *val = malloc(len + 1);
                            memcpy(val, start, len); val[len] = '\0';
                            c->in_values[c->in_count++] = val;
                        }
                    }
                }
            } else {
                char *iv = strdup(v);
                char *_tok_save = NULL; char *tok = strtok_r(iv, ",", &_tok_save);
                while (tok) {
                    char *val = NULL; size_t ulen = 0;
                    if (json_unescape_cstring(tok, strlen(tok), &val, &ulen) != 0) {
                        /* Same reject-whole-criterion policy as the
                           bracketed-array branch above. */
                        free(iv);
                        for (int _i = 0; _i < c->in_count; _i++) free(c->in_values[_i]);
                        free(c->in_values);
                        c->in_values = NULL;
                        c->in_count = 0;
                        c->in_cap = 0;
                        free(v); free(v_raw); free(v2);
                        return -1;
                    }
                    if (c->in_count >= c->in_cap) {
                        int new_cap = c->in_cap * 2;
                        char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                        if (!t) { free(val); c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                        c->in_values = t;
                        c->in_cap = new_cap;
                    }
                    c->in_values[c->in_count++] = val;
                    tok = strtok_r(NULL, ",", &_tok_save);
                }
                free(iv);
            }
        }
```

The `xrealloc_or_free`-based OOM handling (the `if (!t) { ...; break; }` lines) is
unchanged from today's behavior — still a `break` that stops collecting further
elements rather than rejecting the criterion, since an allocator failure is a different
class of problem from a malformed client input and is out of scope for this plan. The
only addition there is `free(val)` immediately before the existing reset, since `val`
is now decoded (and therefore allocated) before that check runs, where previously it
wasn't yet allocated at that point — without it, hitting OOM exactly on a `realloc`
would leak the just-decoded `val`.

### 2i. `query_plan.c` simple-equality criteria branches (sites 7c/7d)

**[round 3 fix]** These two backward-compatible parsers do not call
`parse_one_criterion`; patch both explicitly. Only quoted JSON strings are unescaped.
Bare numeric/bool/null scalars retain their existing raw-copy behavior.

Anchor (`parse_criteria_json`, simple-equality branch):

```c
            /* Strip quotes from value */
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') {
                vlen -= 2; vstart++;
            }
            if (vlen > sizeof(c->value) - 1) vlen = sizeof(c->value) - 1;
            memcpy(c->value, vstart, vlen);
            c->value[vlen] = '\0';
```

Replace with:

```c
            int quoted = (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"');
            if (quoted) { vlen -= 2; vstart++; }
            if (quoted) {
                char *decoded = NULL; size_t decoded_len = 0;
                if (json_unescape_cstring(vstart, vlen, &decoded, &decoded_len) != 0) {
                    free_criteria(criteria, n);
                    *out = NULL;
                    *count = 0;
                    return -1;
                }
                size_t copy_len = decoded_len < sizeof(c->value) - 1
                    ? decoded_len : sizeof(c->value) - 1;
                memcpy(c->value, decoded, copy_len);
                c->value[copy_len] = '\0';
                free(decoded);
            } else {
                if (vlen > sizeof(c->value) - 1) vlen = sizeof(c->value) - 1;
                memcpy(c->value, vstart, vlen);
                c->value[vlen] = '\0';
            }
```

Anchor (`parse_criteria_tree`, simple-equality branch):

```c
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') { vlen -= 2; vstart++; }
            if (vlen > sizeof(leaf->leaf.value) - 1) vlen = sizeof(leaf->leaf.value) - 1;
            memcpy(leaf->leaf.value, vstart, vlen);
            leaf->leaf.value[vlen] = '\0';
```

Replace with:

```c
            int quoted = (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"');
            if (quoted) { vlen -= 2; vstart++; }
            if (quoted) {
                char *decoded = NULL; size_t decoded_len = 0;
                if (json_unescape_cstring(vstart, vlen, &decoded, &decoded_len) != 0) {
                    free_criteria_tree(leaf);
                    free_criteria_tree(root);
                    if (err) *err = "invalid JSON escape in simple criterion value";
                    return NULL;
                }
                size_t copy_len = decoded_len < sizeof(leaf->leaf.value) - 1
                    ? decoded_len : sizeof(leaf->leaf.value) - 1;
                memcpy(leaf->leaf.value, decoded, copy_len);
                leaf->leaf.value[copy_len] = '\0';
                free(decoded);
            } else {
                if (vlen > sizeof(leaf->leaf.value) - 1) vlen = sizeof(leaf->leaf.value) - 1;
                memcpy(leaf->leaf.value, vstart, vlen);
                leaf->leaf.value[vlen] = '\0';
            }
```

### 2j. `query_bulk.c:3981` (`bulk_upd_json_run`) — per-record skip on malformed escape

Anchor (the pre-naming step run once before the parse loop):

```c
    /* Pre-name the typed fields once so we can reuse the names array per
       record without rebuilding it. */
    const char *field_names[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;
```

Replace with:

```c
    /* Pre-name the typed fields once so we can reuse the names array per
       record without rebuilding it. */
    const char *field_names[MAX_FIELDS];
    enum FieldType field_types[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) {
        field_names[i] = ts->fields[i].name;
        field_types[i] = ts->fields[i].type;
    }
```

Anchor (inside the per-record parse loop):

```c
        /* Pull out every typed-field name from `data`. Fields not present in
           `data` come back NULL → not touched. */
        char *vals_buf[MAX_FIELDS];
        json_get_fields(data_str, field_names, ts->nfields, vals_buf);

        int n_touched = 0;
        for (int i = 0; i < ts->nfields; i++) if (vals_buf[i]) n_touched++;
```

Replace with:

```c
        /* Pull out every typed-field name from `data`. Fields not present in
           `data` come back NULL → not touched. */
        char *vals_buf[MAX_FIELDS];
        if (json_get_fields_unescaped(data_str, field_names, ts->nfields, field_types, vals_buf) != 0) {
            /* This record's data_str named a field with a malformed JSON
               escape. Unlike site 5 (a single value_json patch shared by
               every matched record, validated once upfront), each record
               here carries its own independent data_str — so the correct
               granularity is to skip just this one record, matching the
               existing per-record skip semantics a few lines above for
               missing key/data or an oversized key, rather than aborting
               the whole batch over one bad record. */
            for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
            skipped++;
            free(key);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        int n_touched = 0;
        for (int i = 0; i < ts->nfields; i++) if (vals_buf[i]) n_touched++;
```

### 2k. `config.c:2674` (`typed_encode_defaults`) — reject instead of falling back to raw bytes

**[round 2 fix, new]** This is the fix underlying round 2 findings 1 and 2. Round 1's
plan never touched `typed_encode_defaults`, on the assumption that "insert already
validates the value, so index_parallel/build_index_key_from_json only ever see
already-clean data." That assumption was checked against the actual code and found
false: `typed_encode_defaults`'s current `FT_VARCHAR` branch calls
`json_unescape_string`, but on failure it does **not** reject — it falls back to
encoding the raw, still-escaped bytes (`did_unesc ? unesc_len : el` / the `else` arm of
`if (did_unesc) { ... } else { encode_field_len(&ts->fields[i], ev, el, ...); }`) and
returns success. This is the same fallback-to-raw-bytes pattern as the dead-code
`typed_encode`, just in the function that's actually called. Two consequences: (1) a
NUL-embedding escape is never actually rejected at insert time regardless of any
NUL-handling fix elsewhere, since this function accepts the malformed input and
commits it to storage anyway; (2) sites 1-3 (`index_parallel`,
`build_index_key_from_json`) are reachable with a still-escaped, uncommitted-looking
value that in fact WAS committed to the record — "insert already validated this" was
never true. Fixing this one function resolves both findings at once.

Anchor (`config.c`, inside `typed_encode_defaults`'s `FT_VARCHAR` branch):

```c
                    if (ts->fields[i].type == FT_VARCHAR) {
                        char *unesc = NULL; size_t unesc_len = 0;
                        int did_unesc =
                            (json_unescape_string(ev, el, &unesc, &unesc_len) == 0);
                        size_t stored_len = did_unesc ? unesc_len : el;
                        int content_max = ts->fields[i].size - 2;
                        if ((int)stored_len > content_max) {
                            if (err_buf && err_buf_size > 0)
                                snprintf(err_buf, err_buf_size,
                                    "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                                    ts->fields[i].name, stored_len, content_max);
                            if (did_unesc) free(unesc);
                            return -2;
                        }
                        if (did_unesc) {
                            encode_field_len(&ts->fields[i], unesc, unesc_len,
                                              out + ts->fields[i].offset);
                            free(unesc);
                        } else {
                            encode_field_len(&ts->fields[i], ev, el,
                                              out + ts->fields[i].offset);
                        }
                    } else {
```

Replace with:

```c
                    if (ts->fields[i].type == FT_VARCHAR) {
                        char *unesc = NULL; size_t unesc_len = 0;
                        if (json_unescape_cstring(ev, el, &unesc, &unesc_len) != 0) {
                            /* Malformed escape (including an embedded NUL)
                               — reject rather than silently falling back
                               to raw, still-escaped bytes. The old
                               fallback both defeated NUL-rejection (the
                               whole point of json_unescape_cstring) and
                               meant "insert already validated this value"
                               was never actually true for index_parallel/
                               build_index_key_from_json (sites 1-3). */
                            if (err_buf && err_buf_size > 0)
                                snprintf(err_buf, err_buf_size,
                                    "malformed JSON escape in value for field '%s'",
                                    ts->fields[i].name);
                            return -2;
                        }
                        int content_max = ts->fields[i].size - 2;
                        if ((int)unesc_len > content_max) {
                            if (err_buf && err_buf_size > 0)
                                snprintf(err_buf, err_buf_size,
                                    "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                                    ts->fields[i].name, unesc_len, content_max);
                            free(unesc);
                            return -2;
                        }
                        encode_field_len(&ts->fields[i], unesc, unesc_len,
                                          out + ts->fields[i].offset);
                        free(unesc);
                    } else {
```

Both live callers already treat `-2` as an abort/skip signal with no further changes
needed: `storage.c:776` aborts the single insert and reports the error to the client;
`query_bulk.c:1083` treats it as a per-record skip in bulk insert. `typed_encode`
(`config.c:2183-2250`) has the identical fallback-to-raw-bytes bug but zero callers
(confirmed via a full-codebase grep) — it is dead code and deliberately left untouched;
noted here so a future reader doesn't wonder why an apparently-identical bug next door
wasn't fixed too.

### 2l. `query.c:5545`-area (`parse_cursor_object` / `find`'s cursor branch) — decode the cursor's order_by value before it seeds the walk

**[round 4 fix, new site]** `parse_cursor_object` (`query.c:5508`) extracts the cursor's
`order_by` value via a plain `json_obj_strdup(&c, order_by)` (~line 5545) with no
unescape — it can't type-gate at that point because it doesn't have the schema; it only
has the field *name*. The schema-aware `TypedField` (`order_tf`) is resolved later, in
`find`'s cursor branch, right after the "cursor requires order_by field to be indexed"
check. That is where this fix decodes `cur.value` in place, before it feeds
`encode_field_for_index`'s walk-bound seek.

Anchor (`query.c`, right after `order_tf`/`order_field_idx` are resolved, before the
`cur.present` walk-bounds block):

```c
        /* Resolve order_by's TypedField for encoding + value extraction. */
        const TypedField *order_tf = NULL;
        int order_field_idx = -1;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_tf = &driver_fs.ts->fields[i];
                    order_field_idx = i;
                    break;
                }
            }
        }

        /* Encode cursor value bytes for walk bounds. If cursor absent (page 1),
           walk from start (ASC) or end (DESC); else walk from cursor position,
           with tiebreak happening inside the callback. */
        uint8_t cur_value_buf[1024];
```

Replace with:

```c
        /* Resolve order_by's TypedField for encoding + value extraction. */
        const TypedField *order_tf = NULL;
        int order_field_idx = -1;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_tf = &driver_fs.ts->fields[i];
                    order_field_idx = i;
                    break;
                }
            }
        }

        /* [round 4 fix] cur.value/cur.vlen came from parse_cursor_object's
           raw json_obj_strdup — still JSON-escaped text, not decoded
           content, because that function has no schema access. Decode
           in place now that order_tf is known, before it seeds the walk's
           seek bound; a malformed escape rejects the cursor request
           rather than seeking from a corrupted position. cur.value is a
           fixed 1024-byte buffer and json_unescape_cstring never grows
           its input, so the in-place copy is always in bounds. */
        if (cur.present && order_tf && order_tf->type == FT_VARCHAR) {
            char *unesc = NULL; size_t ulen = 0;
            if (json_unescape_cstring(cur.value, cur.vlen, &unesc, &ulen) != 0) {
                OUT("{\"error\":\"cursor order_by value has a malformed JSON escape\"}\n");
                free_joins(joins, njoins); free_excluded(&excluded);
                return -1;
            }
            memcpy(cur.value, unesc, ulen + 1);
            cur.vlen = ulen;
            free(unesc);
        }

        /* Encode cursor value bytes for walk bounds. If cursor absent (page 1),
           walk from start (ASC) or end (DESC); else walk from cursor position,
           with tiebreak happening inside the callback. */
        uint8_t cur_value_buf[1024];
```

Non-varchar and composite/unknown `order_by` fields are unaffected — this only guards
the `FT_VARCHAR` case, matching row 3's existing precedent that untyped/non-varchar
fields keep their raw passthrough unchanged. `cur.key`/`cur.klen` (the cursor's primary
key, used only for `compute_hash_raw` tiebreak) is deliberately left untouched: every
top-level request's `"key"` field is extracted via raw `json_obj_strdup` uniformly
across the whole codebase (`server.c:1443/1476/1517/1539/1558`, `query.c:5533`) — the
primary key has never gone through unescape anywhere, so decoding it here alone would
make the cursor's key handling inconsistent with every other command instead of fixing
anything; it is out of scope for this plan, which is about `FT_VARCHAR` **value**
encoding, not key handling.

### 2m. `storage.c:2192` (`cmd_get_multi` CSV branch) — decode before handing the cell to `csv_emit_cell`

**[round 4 fix, new site]** `cmd_get_multi`'s CSV branch parses `result_json` (the
`typed_decode`-produced, correctly JSON-escaped serialization of the fetched record)
back into a `JsonObj` purely to pull out one field's text at a time, then hands that
text straight to `csv_emit_cell`. Because `json_obj_strdup` doesn't decode, a varchar
value containing a literal quote comes back with its JSON escape bytes intact (e.g. the
two bytes `\"` instead of the one real byte `"`), so `csv_emit_cell`'s quoting logic —
which only recognizes a literal `"` as needing RFC 4180 doubling — never sees it, and
the raw backslash leaks into the CSV cell instead. This is the same defect class as the
aggregate CSV top-N bug fixed in `2026-07-17-agg-csv-topn-format-bug.md`, but at a call
site that fix never touched (this reads back an already-committed record via
`cmd_get_multi`, not an in-flight aggregate). `find`'s own CSV path (`csv_emit_row` /
`query_find.c`) is unaffected — it decodes via `decode_field`/`typed_get_field_str`
directly off the raw stored bytes and never re-parses JSON, so it was never subject to
this bug.

Anchor (`storage.c`, inside `cmd_get_multi`'s CSV branch):

```c
            if (fs.ts) {
                for (int fi = 0; fi < fs.ts->nfields; fi++) {
                    if (fs.ts->fields[fi].removed) continue;
                    char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                    char *pv = have_value ? json_obj_strdup(&value_obj, fs.ts->fields[fi].name) : NULL;
                    csv_emit_cell(pv, csv_delim);
                    free(pv);
                }
            }
```

Replace with:

```c
            if (fs.ts) {
                for (int fi = 0; fi < fs.ts->nfields; fi++) {
                    if (fs.ts->fields[fi].removed) continue;
                    char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                    /* [round 4 fix] result_json is typed_decode's own valid
                       JSON serialization — json_obj_strdup here returns
                       still-escaped text for FT_VARCHAR, not the real
                       content. Decode it for CSV so a literal quote in the
                       stored value gets real-RFC4180 doubling instead of
                       leaking JSON escape bytes into the cell. Non-varchar
                       fields never contain backslash escapes in valid JSON
                       (numbers/bools/null), so they're left on the
                       unchanged raw path — matches row 3's type-gating
                       precedent. json_obj_strdup_unescaped returning NULL
                       (missing field or OOM) degrades to an empty cell,
                       identical to today's existing behavior for a
                       missing field — no new error path needed for a
                       read-only output-formatting site. */
                    char *pv = !have_value ? NULL :
                        (fs.ts->fields[fi].type == FT_VARCHAR
                         ? json_obj_strdup_unescaped(&value_obj, fs.ts->fields[fi].name, NULL)
                         : json_obj_strdup(&value_obj, fs.ts->fields[fi].name));
                    csv_emit_cell(pv, csv_delim);
                    free(pv);
                }
            }
```

## Task 3 — Document the behavior change and confirm the FT_ENUM descope

**[review fix — reduced from a fix task to a documentation/confirmation task]** Per the
"FT_ENUM is explicitly out of scope" decision above, this task does **not** modify any
enum-related code. It exists only to leave a durable, explicit trail:

- Confirm `test_enum.c` (already present in `build.sh`'s test list) does not currently
  cover an enum label containing a JSON-escapable character — read the file and note
  whether it does or doesn't; do not add a new test for this plan (there is no fix to
  regression-test).
- This repo has no `docs/plans/TODO.md`/`BACKLOG.md` or similar deferred-work tracking
  file (confirmed: `find docs -iname "*todo*" -o -iname "*backlog*"` returns nothing) —
  do not create one solely for this. Instead, the note lives here, in this plan, as the
  durable record: "FT_ENUM strict membership validation in `typed_encode_defaults`
  (config.c:2654-2689) compares raw wire text before JSON-unescaping, so an enum label
  containing a quote or backslash character is unreachable from a client (see the
  Field-type scope section above) — needs its own plan if ever prioritized." No further
  action is required for this bullet; just confirm in the execution summary that this
  was checked.

Add the externally observable malformed-escape/NUL rejection to the maintained
changelog. Anchor (`docs/reference/changelog.md`, first lines under Unreleased/Fixes):

```md
## Unreleased

### Fixes
```

Replace with:

```md
## Unreleased

### Fixes

- **JSON-escaped varchar values are decoded consistently before storage, indexing,
  and criteria comparison** — partial update and bulk-update paths no longer store
  literal escape bytes or build mismatched index keys. Valid values ending in a
  backslash are parsed correctly. Malformed JSON escapes and decoded `\u0000` are now
  rejected for varchar inserts/updates instead of being accepted as ambiguous C-string
  data; per-record bulk-update JSON skips only the malformed record and reports it in
  `skipped`.
- **Cursor pagination and multi-get CSV export now decode JSON-escaped varchar
  values too** - resuming a cursor whose `order_by` value contained an escaped
  character (e.g. a quote) no longer risks skipping the next record; `get` with
  `"keys"` and `"format":"csv"` no longer leaks raw JSON escape bytes into a CSV
  cell for a varchar value containing a quote.
```

## Task 4 — Audit the remaining call sites and confirm the "one central rule" holds

**[review fix — this audit was already performed during planning, not deferred]**
`index.c:563` (inside `extract_field_value`) and `index.c:576`/`595` (inside
`build_composite_value`) were flagged during the original investigation as
not-yet-traced. They have now been traced (see Root cause section, "dead code,
confirmed, no action taken"): both functions have zero callers anywhere in `src/db`,
confirmed via `grep -rn "extract_field_value\|build_composite_value" src/db/*.c
src/db/*.h`. No further investigation is needed for these three lines; no code change
is made to them by this plan.

The executor's job for this task is narrower than the original draft: run

```
rtk grep -n 'json_get_fields(\|json_obj_strdup(' src/db/*.c
```

after Task 2's edits land, and confirm every remaining hit is one of: (a) a
protocol/control key (`"mode"`, `"dir"`, `"object"`, `"field"`, `"op"`/`"operator"`, or
similar non-data key), (b) a genuinely-non-JSON re-encode already identified as unsafe
to route through the unescaped variant (`query_join.c:268`), (c) the two confirmed-dead
functions in `index.c` (`extract_field_value`/`build_composite_value`, left
intentionally untouched), (d) `query_plan.c`'s deliberately-raw `v`/`v_raw`/`v2`
extraction (which now decodes after structural parsing, per Task 2g/2h, not at the
extraction call itself), or (e) **[round 4, new bucket]** `query.c:5545` (inside
`parse_cursor_object`) and `storage.c:2192` (inside `cmd_get_multi`'s CSV branch) —
both still call the raw, non-decoding helper at the grepped line, but are now decoded
immediately afterward: `query.c`'s hit is decoded at the `find` cursor call site once
`order_tf` is known (Task 2l), and `storage.c`'s hit was switched to
`json_obj_strdup_unescaped` directly at that line for `FT_VARCHAR` fields (Task 2m) —
if this grep still shows `storage.c:2192` calling plain `json_obj_strdup` unconditionally
(not the type-gated ternary from Task 2m), Task 2m did not land correctly. If a hit
doesn't fit any of these five buckets, **stop and ask** — per this plan's execution
rules, do not decide unilaterally whether to expand scope; this is exactly the kind of
uncovered decision that requires the human's input rather than executor judgment (this
round's own two new sites are the precedent: they were raised exactly this way, and
folding them into the plan was the human's call, not the executor's).

**[round 4 note]** This same grep also surfaces `index.c:973`'s
`else { txt = json_obj_strdup(&jo, spec); }` inside `build_index_key_from_json`'s
single-field branch (Task 2d). That `else` only runs when `fi < 0` (no schema/legacy
field) or the field's type isn't `FT_VARCHAR` — i.e. it's the same type-gated
"non-varchar/untyped stays raw" fallback Task 2d's `if (fi >= 0 && ts->fields[fi].type
== FT_VARCHAR)` branch is paired with. This is expected and correct as-is: it's the
by-design non-varchar side of the type gate (row 3), not an overlooked site. Confirmed
during this round's investigation; no action needed, but call it out explicitly here so
a future audit doesn't re-flag it as a sixth unexplained bucket.

Paste the grep output and a one-line disposition for each remaining hit.

Because sites 7c/7d are hand-written span copies and do not call either searched helper,
also run:

```
rtk grep -n 'memcpy(.*value\|memcpy(c->value' src/db/query_plan.c
```

Confirm the two simple-equality branches now decode quoted spans before their bounded
copy. Disposition every other hit as binary/compiled-criterion copying or stop and ask
if another raw request-JSON-to-criterion copy is found.

## Task 5 — Full regression suite

`rtk ./build/bin/shard-db-test run-all` — paste full output. Pay particular attention to any
existing test exercising `update`/`bulk_update`/`bulk_update_json`/`find` on varchar
fields with unusual content (quotes, backslashes already present in fixtures elsewhere
in the suite) in case this fix changes their expected output or match results from a
previously-broken value to the now-corrected one — if so, that test's assertion was
itself asserting the bug and should be corrected, not treated as a regression.

## Task 6 — Required dynamic-safety gates

This diff changes allocation ownership and failure cleanup in shared parsers and worker
inputs, so this repo's object-lifetime gate requires both ASan+UBSan and TSan locally.
Run the exact commands below and paste the full outputs; do not defer them to CI:

```bash
BUILD_MODE=asan SKIP_TESTS=1 rtk ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" rtk ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 rtk ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" rtk ./build/bin/shard-db-test run-all --jobs 1
```

Any new finding must be root-caused and fixed or handled under AGENTS.md's explicit
defer-and-document process; never add a blanket suppression.

## Task 7 — Confirm the "safe" call sites stay untouched

Spot-check (do not need new tests, just re-confirm by reading) that
`build_index_key_from_record`/`build_index_key_from_record_into` and their callers in
`storage.c` and `query_bulk.c` (lines 2296/2304/2820-2850/3182-3185/3656-3659/4227,
per the root-cause section) are unaffected by this change — they take pre-decoded
binary records, not JSON text, and should need no modification. Also confirm
`query_join.c:268`'s join-key re-encode (identified in the Design decision section as a
caller that must NOT be routed through an unescaped extractor) was not touched. This
task exists to catch anywhere this plan's edits accidentally applied unescaping to
already-decoded input by mistake.

## Edge cases / invariants

- **Malformed escape sequences** (e.g. `\q`, truncated `\u12`):
  `json_unescape_cstring` returns -1. Per-call-site policy (see the inventory table and
  Task 2): insert rejects the record before index maintenance (site 9); index-only
  sites 1-3 abort pre-commit on any late decode/allocation failure and never equate it
  with field absence; site 4 rejects the whole single-record update; site 5 decodes the
  shared patch once before worker dispatch; site 8 skips only the independently
  malformed record; and comparison sites 6/7/7b/7c/7d reject the whole
  criterion. `IN` and `NOT_IN` never drop only the malformed element, because doing so
  would change the requested set and can broaden `NOT_IN` results.
- **Fields with no escapable characters** (the overwhelming common case):
  `json_unescape_string` is a no-op copy (loop body never takes the `\\` branch), so this
  fix has no observable effect on the common path beyond one extra malloc/free per field
  per request/criterion — not expected to be measurable, but worth a quick sanity check
  in Task 5's full-suite run (no new timeouts).
- **NUL bytes in decoded varchar content** (`\u0000`):
  `json_unescape_string` remains length-aware and unchanged. The new
  `json_unescape_cstring` wrapper rejects decoded embedded NUL only for paths that
  immediately store the result in ordinary C strings (`strlen`/`strncpy`/`strcmp`).
  `typed_encode_defaults` switches to that wrapper and now rejects `\u0000` instead of
  accepting it; the length-aware bulk-insert-delimited `data` path continues using the
  original decoder unchanged. This is an intentional tightening of varchar write
  semantics and is recorded in the Unreleased changelog by Task 3. Test 1g exercises
  the insert contract; the malformed-write atomicity tests exercise the other write
  granularities.
- **This plan does not touch `agg_run_topn_stream`, `VS`, or `IGB`** — those all read
  whatever bytes are actually in the index, correctly; fixing what gets *written* into
  the index (this plan) is what makes their *reads* correct, with no changes needed on
  the read side. Confirm this by re-running the CSV plan's own Task 1 checkpoint (on
  its `tcsv` object, or by re-adding a quote-bearing case if desired) after this plan
  lands, as a cross-plan integration check.
- **`OP_BETWEEN`/`OP_LEN_BETWEEN` array-form splitting** (query_plan.c:1259-1288, the
  block that splits `"value":["25","30"]` into `v`/`v2`) now correctly handles varchar
  bounds containing a literal quote character, per Task 2g/7b's fix — test 1f exercises
  this directly with quoted varchar bounds, superseding the original draft's incorrect
  assumption that this path was numeric-only and therefore a no-op to leave broken.
- **FT_ENUM write behavior is explicitly out of scope** — see Field-type scope. The
  storage/index extractors are type-gated and do not change enum validation or
  encoding. Criteria operands are decoded as JSON strings before comparison because
  `parse_one_criterion` has no schema type; this does not make escaped enum labels
  insertable and does not resolve the separately documented enum defect.
- **[round 4]** `FindCursor.key`/`.klen` (the cursor's primary-key tiebreak,
  `query.c:5533`'s `json_obj_strdup(&c, "key")`) is deliberately left raw — see Task
  2l's disposition note. Only `.value`/`.vlen` (the `order_by` field's value) is
  decoded, and only when that field is `FT_VARCHAR`.
- **[round 4]** `cmd_get_multi`'s non-CSV (dict/wrapped JSON) response branch is
  unaffected by Task 2m — it streams `result_json` straight through
  (`storage.c:2251-2253`, `OUT("\"%s\":%s", out_key, entries[i].result_json)`) without
  ever re-parsing it into a `JsonObj`, so there is no second, already-escaped copy of
  the text to double-decode; the bug and its fix are specific to the CSV branch's
  per-field re-extraction.

## Documentation sync

The request/response shapes and supported operators do not change, so the protocol
reference needs no edit. The newly explicit rejection of malformed JSON escapes and
decoded `\u0000` in varchar write paths is externally observable, however, so Task 3
adds an exact bullet under `docs/reference/changelog.md` → `Unreleased` → `Fixes`.
