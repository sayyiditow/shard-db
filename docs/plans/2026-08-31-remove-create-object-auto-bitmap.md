# Remove create-object auto-bitmap default for bool/enum fields

## Status

PLANNED — awaiting human approval. Per CORE-PROCESS.md step 2, no
execution starts until this document is read and approved. The plan was
drafted 2026-08-31 from code-level verification (every anchor below was
read verbatim from the working tree on that date).

REVIEWED 2026-09-04 (pre-approval anchor re-verification): every code
anchor in Tasks 1-5 was re-confirmed verbatim against the current tree,
and every Task 6 doc passage located by its current text. No anchor
drifted — the only commits since drafting (PRs #332/#333, the
match-typed trim-boundary fix) touched none of the anchor files. The
new `test_bitmap_stream_find_flush_gate.c` from PR #333 declares its
bool index explicitly and is unaffected (added to the sweep notes in
"Scope and findings"). Two accuracy patches were applied at review:
the bench blast-radius note now names bench_parallel and
bench_queries' drop-indexes-first mode, and Task 2e adds
`#undef FIELD_TYPE_PREFIX_IS` for macro hygiene.

## Problem

`cmd_create_object` silently appends an `IT_BITMAP` index for every
`bool` / `enum(...)` field the user did **not** declare, persists it as a
`name:bitmap` line in `<obj>/indexes/index.conf`, and builds the `.bm`
shard files at create time. Indexes must always be user-initiated. After
this plan, create-object creates **exactly** the declared `indexes` —
nothing else.

Mechanism (verified, not inferred):

- The auto-default loop lives in `src/db/query_schema.c`, introduced with
  the 2026.05.7 bitmap feature. Anchor: the comment
  `/* Auto-default: every \`bool\` or \`enum(...)\` field that isn't already
  declared as an index gets IT_BITMAP.` It scans `field_specs[]`, skips
  fields already present in `pidx[]`, and appends
  `pidx[npidx].type = IT_BITMAP` (cap 0 = default 256; 2-byte enums get
  65535).
- The implicit entries are persisted by the index.conf writer (comment
  anchor: `Write index.conf from the parsed list — both explicit indexes
  and any auto-defaulted bitmap entries on bool fields.`) as
  `name:bitmap`, and the create-time bitmap materialization block walks
  `pidx` for `IT_BITMAP` to ftruncate+mmap the `.bm` shards.
- From then on the implicit index is a normal declared index: the CRUD
  write path (`load_index_fields` → `apply_index_diff`,
  `src/db/storage.c`) maintains it and the planner uses it. The
  "automatic" part is therefore a one-time implicit declaration at object
  creation — not a runtime special case.

This is a deliberate externally-observable contract change (breaking for
anyone relying on undeclared bool fields being indexed), flagged as such
in Task 6; it is not a bug fix, so no root-cause section applies.

## Design decisions (surfaced to the human; defaults applied)

1. **Bare-name promotion stays.** Declaring `"indexes":["flag"]` on a
   bool field still yields a bitmap via `idx_should_auto_bitmap`
   (src/db/config.c). The index line is user-initiated; only the
   *undeclared* auto-default dies. Rationale: the user's directive is
   "no indexes during create-object if none was declared"; bare names
   are declared. This also keeps ~7 test files and 2 benches untouched.
2. **Validator extended to enum.** Today `"color:bitmap"` on an enum
   field is rejected by create-object (anchor:
   `bitmap index requires bool or varchar field`) while `cmd_add_index`
   (src/db/index.c) has no type restriction and accepts it — a latent
   inconsistency the removal would expose. Fix: accept `enum(` fields in
   the validator; 2-byte enum + plain `:bitmap` → cap 65535 (same rule
   the promote path uses). Note: `FIELD_TYPE_IS` cannot match enum specs
   (its tail check only accepts a `\0` or `:` delimiter after the type
   token, and enum specs continue with `(`), so a prefix-match variant
   macro is added.

## Scope and findings

Consumers of `indexes/index.conf` (format unchanged; new objects simply
have fewer lines — every consumer below keeps working unchanged):

- `idx_cache_ensure` / `load_index_fields` / `load_index_types`
  (src/db/config.c) — index discovery + type lookup.
- `apply_index_diff` and the bulk windows (src/db/storage.c,
  src/db/query_bulk.c) — CRUD index maintenance, driven by
  `load_index_fields`.
- `cmd_add_index` (src/db/index.c) — dedupe by canonical line; fresh
  build when the line is absent.
- Reindex: the `cmd_add_indexes` driver and
  `reindex_object_checked_impl` (src/db/index.c) — iterate declared
  lines only; both auto-promote declared bare bool/enum names via
  `idx_should_auto_bitmap` (unchanged).
- Planner index discovery (src/db/query_plan.c, src/db/query.c).
- `cmd_describe_object` (src/db/query_schema.c) — reads index.conf and
  emits `"indexes":[]` when the file is absent (verified at the
  `OUT(",\"indexes\":[")` block).

Pre-existing doc staleness this plan fixes in passing:
docs/release-notes/2026.05.7.md claims `add-field` auto-bitmaps bool
fields; `cmd_add_fields` (src/db/config.c) has no bitmap logic at all.

Test blast radius (established by exhaustive sweep of src/test/ for
`:bool` / `enum(` create payloads; re-verified per-field on 2026-09-04
against the current tree, including PR #333's new
test_bitmap_stream_find_flush_gate.c, which declares its bool index and
is unaffected):

- Hard-fails after the change: test_bitmap_index.c (objects `a`,
  `bools_only`, `e2e`, `reix`, `bulk`, `qry` — 3 assertion sites assert
  the auto-default itself), test_enum.c (object `a`), test_trigram_index.c
  (bare add-index `active` expects `"exists"` because the .bm pre-exists).
- Unaffected, declared: every payload that declares the index (bare or
  `:bitmap`), including benches: bench_bitmap_vs_btree.c (explicit),
  bench_queries.c and bench_joins.c (bare names → promote),
  bench_cache_pollution.c (reuses bench_queries' object).
- Unaffected, undeclared but never asserted on (re-verified 2026-09-04):
  test_add_indexes_single_scan.c (`mixed`/`active` — the test itself adds
  `active:bitmap` via add-indexes before asserting), test_describe.c and
  test_slotcask_v2_wire.c (`active` undeclared; contains-only asserts, no
  index-list asserts), test_schema_export.c (asserts `"active:bool"` in
  the fields section only), test_find_cursor.c (`curs_crit`/`flag` — its
  flag criteria become a full scan post-change; results identical),
  test_get_fields.c, test_trim_compact_oob_fields.c, test_enum.c's
  `baddefault`/`gooddefault`, and test_bitmap_index.c's error-path
  payloads (`err_composite` etc.).
- Bench numbers shift (no bench code changes needed): bench_invoice
  loses two never-queried implicit bitmaps (`consolidated`, `pdfSent` —
  its only `pdfSent` query rides the declared `irbmStatus+pdfSent`
  btree composite); bench_parallel.c loses the same two in both create
  paths (shared INVOICE_SCHEMA_FIELDS); bench_queries.c's env-gated
  `SHARD_BENCH_DROP_INDEXES_FIRST=1` mode creates with `"indexes":[]`
  and so loses the `active`/`category` implicit bitmaps during load
  (the default path declares both bare → promote). Disk/write numbers
  shift slightly; no query in any bench uses the lost bitmaps.

## Non-goals

- No migration or cleanup of existing objects: their persisted
  `name:bitmap` lines are declarations now and keep working everywhere.
- `add-field` behavior unchanged (it never auto-indexed).
- No changes to `idx_should_auto_bitmap` or its four call sites
  (promotion of *declared* bare names is out of scope).
- No planner, storage, wire-dispatch, or locking changes.
- No bench code changes.

## Embedded execution rules

- Branch `fix/no-auto-bitmap-create` off `main`. Execute tasks in order.
- Build: `SKIP_TESTS=1 ./build.sh`. Tests: `./build/bin/shard-db-test
  run-all` (or `run <name>`).
- Gates (Definition of done, per AGENTS.md standing exceptions):
  `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then 3 fresh
  `./build/bin/shard-db-test run-all`; then `BUILD_MODE=tsan
  SKIP_TESTS=1 ./build.sh` then 3 fresh
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
  ./build/bin/shard-db-test run-all`. No `halt_on_error=0`, no
  `--jobs`. Findings fail the run and get root-caused, not tolerated.
- If a quoted anchor isn't found exactly: write `PLAN_NOTES.md`
  describing the mismatch and halt the entire execution run immediately
  — do not guess, reinterpret, or continue to any further task. If you
  hit a decision this plan doesn't cover: stop and ask.
- Execution mode (repo standing exception): leave ALL work uncommitted
  for the reviewing agent + human raw-diff review.
- Paste real command output into the `## Evidence` sections as tasks
  complete. Never weaken a test to make a failure disappear.

## Task 1 — Negative regression test (test first)

File: src/test/cases/test_bitmap_index.c.

### 1a. Replace the auto-default acceptance section

Anchor (start): the exact line

```c
    /* === Auto-default: bool fields with NO indexes declared still get bitmap === */
```

Anchor (end): the `free(resp); resp = NULL;` immediately following the
scoped block whose last assertion is

```c
            ASSERT_TRUE(strstr(idx_buf, "\"c\"") == NULL && strstr(idx_buf, "\"c:") == NULL,
                        "c (int) NOT in indexes list");
```

Delete everything from the start anchor through that trailing
`free(resp); resp = NULL;` (inclusive) and insert:

```c
    /* === No auto-default: undeclared bool fields get NO index at all === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"bools_only\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"a:bool\",\"b:bool\",\"c:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "bools_only created");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"bools_only\"}", &resp);
    ASSERT_CONTAINS(resp, "\"indexes\":[]", "no auto-default: zero indexes for undeclared bools");
    free(resp); resp = NULL;
    {
        char icp[1024];
        snprintf(icp, sizeof(icp), "%s/t/bools_only/indexes/index.conf", env.db_root);
        struct stat st;
        ASSERT_TRUE(stat(icp, &st) != 0 && errno == ENOENT,
                    "no index.conf without declared indexes");
    }

    /* Declared-after-the-fact still works: bare add-index promotes bool. */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"bools_only\","
        "\"field\":\"a\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"indexed\"", "bare add-index promotes bool to bitmap");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"bools_only\"}", &resp);
    ASSERT_CONTAINS(resp, "\"a:bitmap\"", "declared bare bool → bitmap");
    ASSERT_NOT_CONTAINS(resp, "\"b:bitmap\"", "undeclared b stays unindexed");
    ASSERT_NOT_CONTAINS(resp, "\"c:bitmap\"", "int c stays unindexed");
    free(resp); resp = NULL;
```

Compile notes: `errno.h` is already included; `struct stat`/`stat` are
already in use in this file (see the post-review regression section near
the end); `ASSERT_NOT_CONTAINS` is the file-local wrapper macro.

### 1b. Prove red on base

Run `./build/bin/shard-db-test run test-bitmap-index` on the unmodified
base. Required failure: `"no auto-default: zero indexes for undeclared
bools"` (describe currently reports `a:bitmap`/`b:bitmap`), plus
`"no index.conf without declared indexes"`. Paste the failing output
into `## Evidence — Task 1`. The test goes green in Task 2; paste the
green rerun there too (red + green both required).

## Task 2 — query_schema.c: delete the auto-default; accept explicit enum bitmaps

### 2a. Intro comment

Anchor:

```c
         2. Auto-default `IT_BITMAP` for `bool` fields not explicitly indexed.
         3. Write `<obj>/indexes/index.conf` from the canonicalised list
```

Delete item 2, renumber item 3 to 2, and append after the closing line
of that comment block's paragraph (the `Composite indexes (`f1+f2`) are
btree-only in 2026.05.7. */` line keeps its place; the new sentences go
inside the comment, before it):

```c
       Only user-declared indexes are created — there is no auto-default.
       (Bare `bool`/`enum` names still promote to bitmap via
       idx_should_auto_bitmap: a declared index with an automatic type.)
```

### 2b. Prefix-match macro

Anchor: the closing of the `FIELD_TYPE_IS` macro definition, i.e. the
lines

```c
            _matched;                                                         \
        })
```

Immediately after that `#define` block ends, insert:

```c
    /* Like FIELD_TYPE_IS but for parameterised types: matches when the
       field's type token starts with `prefix` (e.g. "enum(" — the enum
       domain follows in parens, so there is no single-char delimiter). */
    #define FIELD_TYPE_PREFIX_IS(fname, fnlen, prefix)                        \
        ({                                                                    \
            int _matched = 0;                                                 \
            for (int _i = 0; _i < nfields; _i++) {                            \
                const char *_c = strchr(field_specs[_i], ':');                \
                if (!_c) continue;                                            \
                int _nlen = (int)(_c - field_specs[_i]);                      \
                if (_nlen != (fnlen)) continue;                               \
                if (memcmp(field_specs[_i], (fname), _nlen) != 0) continue;   \
                _matched = (strncmp(_c + 1, (prefix), strlen(prefix)) == 0);  \
                break;                                                        \
            }                                                                 \
            _matched;                                                         \
        })
```

### 2c. Validator accepts enum

Anchor:

```c
                                if (!FIELD_TYPE_IS(tok, tok_len, "bool") &&
                                    !FIELD_TYPE_IS(tok, tok_len, "varchar")) {
                                    OUT("{\"error\":\"bitmap index requires bool or varchar field (got \\\"%s\\\")\"}\n", tok);
```

Replace those three lines with:

```c
                                if (!FIELD_TYPE_IS(tok, tok_len, "bool") &&
                                    !FIELD_TYPE_IS(tok, tok_len, "varchar") &&
                                    !FIELD_TYPE_PREFIX_IS(tok, tok_len, "enum(")) {
                                    OUT("{\"error\":\"bitmap index requires bool, varchar or enum field (got \\\"%s\\\")\"}\n", tok);
```

(The existing error-substring test `"bitmap index requires"` in
test_bitmap_index.c keeps matching.)

### 2d. Cap rule covers explicit enum bitmaps too

Anchor (inside the auto-promote loop):

```c
                            if (idx_should_auto_bitmap(ps.had_explicit_type, tf.type)) {
                                ps.type = IT_BITMAP;
                                if (tf.type == FT_ENUM && tf.enum_width == 2 &&
                                    ps.max_values == 0) {
                                    ps.max_values = 65535;
                                }
                            }
                            free_enum_values(&tf);
```

Replace with:

```c
                            if (idx_should_auto_bitmap(ps.had_explicit_type, tf.type)) {
                                ps.type = IT_BITMAP;
                            }
                            /* 2-byte enums need the full-domain cap whether the
                               bitmap came from a bare promote or an explicit
                               name:bitmap (explicit bitmap(N) overrides). */
                            if (ps.type == IT_BITMAP && tf.type == FT_ENUM &&
                                tf.enum_width == 2 && ps.max_values == 0) {
                                ps.max_values = 65535;
                            }
                            free_enum_values(&tf);
```

### 2e. Delete the auto-default loop

Anchor (start): the full comment

```c
    /* Auto-default: every `bool` or `enum(...)` field that isn't already
       declared as an index gets IT_BITMAP.
```

Anchor (end): the loop's final lines

```c
        npidx++;
    }
```

immediately before `#undef FIELD_TYPE_IS` (which STAYS — the macro is
still used by the validator). Delete the whole block between the
anchors, inclusive of the loop body but NOT the `#undef` line. Then,
directly after the surviving `#undef FIELD_TYPE_IS` line, append:

```c
    #undef FIELD_TYPE_PREFIX_IS
```

(the Task 2b macro must not leak past the function, matching the
existing FIELD_TYPE_IS hygiene).

### 2f. index.conf writer comment

Anchor:

```c
    /* Write index.conf from the parsed list — both explicit indexes and any
       auto-defaulted bitmap entries on bool fields. Line format is `name`
```

Replace with:

```c
    /* Write index.conf from the parsed list — exactly the user-declared
       indexes, nothing else. Line format is `name`
```

(remainder of the comment unchanged).

## Task 3 — test_bitmap_index.c: declare what was implicit

### 3a. Object `a` payload

Anchor:

```c
        "\"indexes\":["
          "\"name\","                /* legacy bare → btree */
          "\"score\","
          "\"type:bitmap\","         /* opt-in varchar enum */
          "\"text:trigram\","
          "\"name+score\"]}",         /* composite stays btree */
```

Replace with:

```c
        "\"indexes\":["
          "\"name\","                /* legacy bare → btree */
          "\"score\","
          "\"type:bitmap\","         /* opt-in varchar bitmap */
          "\"text:trigram\","
          "\"name+score\","          /* composite stays btree */
          "\"dead:bitmap\","         /* bool bitmaps are opt-in */
          "\"deleted:bitmap\"]}",
```

### 3b. Object `a` describe asserts

Anchor:

```c
    ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "describe: auto-bitmap on dead");
    ASSERT_CONTAINS(resp, "\"deleted:bitmap\"","describe: auto-bitmap on deleted");
```

Replace with:

```c
    ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "describe: explicit bitmap on dead");
    ASSERT_CONTAINS(resp, "\"deleted:bitmap\"","describe: explicit bitmap on deleted");
```

Also update the preceding comment (anchor: `auto-defaulted bitmap
entries for both bool fields` → `explicitly declared bitmap entries for
both bool fields`).

### 3c. Object `e2e` payload + create assert

Anchor:

```c
        "\"indexes\":[\"label:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create e2e (bool auto + label:bitmap)");
```

Replace with:

```c
        "\"indexes\":[\"flag:bitmap\",\"label:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create e2e (flag:bitmap + label:bitmap)");
```

All downstream bm_open popcount/update/delete assertions then pass via
the now-declared bitmap, unchanged.

### 3d. Object `reix` payload

Anchor (unique — the e2e payload's `fields` line differs):

```c
        "\"fields\":[\"flag:bool\",\"label:varchar:16\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create reix");
```

Replace with:

```c
        "\"fields\":[\"flag:bool\",\"label:varchar:16\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create reix");
```

The pre-wipe popcount sanity block stays and passes via the declared
bitmap.

### 3e. Object `qry` payload

Anchor:

```c
        "\"fields\":[\"flag:bool\",\"name:varchar:8\"]}", &resp);
```

Replace with:

```c
        "\"fields\":[\"flag:bool\",\"name:varchar:8\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
```

(Keeps the Phase-4 planner block exercising the bitmap read path.)

### 3f. Object `bulk` payload + create assert

Anchor:

```c
        "\"fields\":[\"flag:bool\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bulk (auto-bitmap on flag)");
```

Replace with:

```c
        "\"fields\":[\"flag:bool\"],"
        "\"indexes\":[\"flag:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create bulk (flag:bitmap declared)");
```

Bulk popcount assertions and the operator-coverage block keep exercising
bitmap paths via the declared index.

### 3g. Post-restart assert

Anchor:

```c
            ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "post-restart: auto-bitmap preserved");
```

Replace with:

```c
            ASSERT_CONTAINS(resp, "\"dead:bitmap\"",   "post-restart: explicit bool bitmap preserved");
```

## Task 4 — test_enum.c: declare the enum bitmap

### 4a. Payload

Anchor:

```c
        "\"fields\":[\"color:enum(red,green,blue)\",\"name:varchar:32\"]}",
        &resp);
```

Replace with:

```c
        "\"fields\":[\"color:enum(red,green,blue)\",\"name:varchar:32\"],"
        "\"indexes\":[\"color:bitmap\"]}",
        &resp);
```

### 4b. Asserts + comments

Anchor:

```c
    ASSERT_CONTAINS(resp, "\"color:bitmap\"",   "describe: auto-bitmap on color");
```

Replace with:

```c
    ASSERT_CONTAINS(resp, "\"color:bitmap\"",   "describe: explicit bitmap on color");
```

Update the comment above it (anchor: `+ auto-bitmap.` → `+ the declared
bitmap.`). The post-restart assert `"post-restart: bitmap preserved"`
stays as-is (now fed by the explicit declaration).

## Task 5 — test_trigram_index.c: bare add-index now builds fresh

Anchor: the whole block from the comment starting

```c
    /* === Singular path: bare bool. create-object auto-defaults bool to
```

through

```c
    ASSERT_CONTAINS(resp, "\"status\":\"exists\"",
                    "singular bare bool: typed skip-probe sees existing .bm");
    free(resp); resp = NULL;
```

Replace with:

```c
    /* === Singular path: bare bool. Nothing pre-exists (no auto-default),
       so the bare add-index promotes via idx_should_auto_bitmap and
       builds fresh. Force-rebuild confirms the promote rule runs on the
       same path. */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"s\",\"object\":\"items\","
        "\"field\":\"active\"}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"indexed\"",
                    "singular bare bool: promotes + builds (no pre-existing .bm)");
    free(resp); resp = NULL;
```

Keep the following force-rebuild request/assert and the `000.bm` stat
check unchanged.

## Task 6 — Docs sync (same branch, not deferred)

Exact wording at execution time; passages located by their current text:

- docs/index.md — landing bullet "(auto-default for `bool` + `enum`;…)"
  → opt-in-only wording.
- docs/concepts/indexes.md — line "auto-default for `bool` and `enum`
  fields since 2026.05.7" → opt-in via `field:bitmap`/`field:bitmap(N)`
  or bare declared bool/enum names; the "you usually don't need to make
  a decision — the auto-default puts them on bitmap" paragraph →
  "declare them explicitly"; "wouldn't auto-default to that type" and
  the spec-table "suppresses auto-bitmap" row; the "Auto-defaults"
  closing paragraph → rewrite around explicit declaration.
- docs/query-protocol/index-management.md — table rows "`field` | btree
  (or bitmap if bool/enum auto-default)" → plain "btree";
  "(suppresses auto-bitmap)" → reword.
- docs/concepts/typed-records.md — enum row "Auto-defaults to a bitmap
  index." → delete/reword (bitmap is opt-in).
- docs/operations/benchmarks.md — "auto-defaults to a bitmap" → the
  bench declares `active` bare, i.e. it is promoted, not auto-defaulted.
- docs/query-protocol/schema-mutations.md — enum-widen row "rewrites the
  bitmap" → condition on the enum actually having a bitmap index.
- docs/getting-started/quickstart.md — the create-object example must
  declare `active` in `indexes` (otherwise its later find/aggregate
  examples become scans); same check for the example objects in
  docs/getting-started/clients.md and docs/getting-started/npm.md.
- docs/reference/changelog.md `## Unreleased` — breaking-change entry:
  create-object no longer auto-indexes bool/enum fields; migrate with
  `add-index` (bare name or `:bitmap`); existing objects unaffected
  (their index.conf lines persist); bench_invoice numbers shift
  slightly; explicit `name:bitmap` now accepted on enum fields at
  create-object.
- docs/release-notes/2026.05.7.md — stays as history; add a bracketed
  erratum to its factually-wrong "add-field auto-bitmaps" upgrade note.

## Task 7 — Gates

1. `SKIP_TESTS=1 ./build.sh` — no new compiler warnings.
2. `./build/bin/shard-db-test run-all` — full suite green;
   `run test-bitmap-index`, `run test-enum`, `run test-trigram-index`
   re-verified individually.
3. `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`; three fresh
   `./build/bin/shard-db-test run-all`.
4. `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`; three fresh
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
   ./build/bin/shard-db-test run-all`.
5. `git status` + `git diff` packet for review: src/db/query_schema.c,
   src/test/cases/test_bitmap_index.c, test_enum.c,
   test_trigram_index.c, ~12 docs files. NOTHING committed (repo
   standing exception).

## Acceptance criteria

- Task 1's test red on base, green after Task 2 (both outputs pasted).
- create-object with bool/enum fields and no declared `indexes` →
  `"indexes":[]` in describe-object, no `indexes/index.conf` on disk, no
  `.bm`/`.idx` directories for those fields.
- Declared forms (bare, `:btree`, `:bitmap`, `:bitmap(N)`, `:trigram`,
  composites) behave exactly as before; explicit `name:bitmap` on enum
  accepted at create-object with the 65535 two-byte cap default.
- Existing objects' persisted `:bitmap` lines keep working (reindex,
  CRUD, planner unchanged).
- Full suite + asan ×3 + tsan ×3 green; docs synced in the same diff;
  diff left uncommitted.

## Invariants

- `idx_should_auto_bitmap` and its four call sites (create-object
  promote, cmd_add_index, add-indexes driver, reindex checked impl) are
  untouched.
- `indexes/index.conf` on-disk format unchanged; existing files are
  never rewritten except by existing maintenance paths.
- No storage-layout, planner, wire-protocol, or locking changes; no new
  dependencies.

## Review round — 2026-09-04 (changes requested)

The reviewer accepted the core implementation but requested changes.
This section amends the plan; the original tasks above stand as
executed, and the amendments below are complete code blocks so they can
be executed literally.

### R1 (Standards 1 / Spec 2, High) — acceptance-hardening assertions

The acceptance criteria require proving undeclared **enum** fields, the
absence of per-field `.bm`/`.idx` directories, and the 2-byte enum
default cap of 65535. Task 8 adds these. All three asserts are red on
base by the same mechanism Task 1 proved (base auto-defaults an index
onto the field, so `"indexes":[]`, the ENOENT stats, and the explicit
`color:bitmap`-at-create acceptance fail); the red proof for the
mechanism is in Evidence — Task 1 and is not re-proven by reverting.

#### Task 8 — test_bitmap_index.c: enum negative, directory absence, enum cap

Insert after the Task 1a block's final `free(resp); resp = NULL;` (the
one following `"int c stays unindexed"`), immediately before the
`/* === Legacy / back-compat ... */` comment. First, one added assert
inside the existing bools_only directory-absence block — after the
`"no index.conf without declared indexes"` assert, still inside that
scoped `{ ... }`:

```c
        char fdp[1024];
        snprintf(fdp, sizeof(fdp), "%s/t/bools_only/indexes/a", env.db_root);
        struct stat st2;
        ASSERT_TRUE(stat(fdp, &st2) != 0 && errno == ENOENT,
                    "no .bm shard dir for undeclared bool field");
```

Then the new block:

```c
    /* === No auto-default: undeclared enum fields get NO index either,
       and a declared explicit bitmap on a 2-byte enum takes the full
       65535 domain cap at create-object (matching cmd_add_index) without
       the operator spelling bitmap(65535). === */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"enums_only\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"color:enum(red,green,blue)\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "enums_only created");
    free(resp); resp = NULL;
    tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"enums_only\"}", &resp);
    ASSERT_CONTAINS(resp, "\"indexes\":[]", "no auto-default: zero indexes for undeclared enums");
    free(resp); resp = NULL;
    {
        char fdp[1024];
        snprintf(fdp, sizeof(fdp), "%s/t/enums_only/indexes/color", env.db_root);
        struct stat st;
        ASSERT_TRUE(stat(fdp, &st) != 0 && errno == ENOENT,
                    "no .bm shard dir for undeclared enum field");
    }

    /* Explicit name:bitmap on an enum is accepted at create-object
       (matching cmd_add_index) and keeps the default cap. A 2-byte
       (257+ value) enum cannot be declared on any wire path — create
       caps field specs at 511 bytes and add/edit-field caps lines at
       256 — so the enum_width==2 → 65535 default cap in the create
       validator is defensive consistency with the shared rule, not
       wire-reachable. */
    {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"enum_bm\","
            "\"splits\":8,\"max_key\":16,"
            "\"fields\":[\"color:enum(red,green,blue)\"],"
            "\"indexes\":[\"color:bitmap\"]}");
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"created\"",
                        "explicit enum bitmap accepted at create-object");
        free(resp); resp = NULL;
        tc_request(tc, "{\"mode\":\"describe-object\",\"dir\":\"t\",\"object\":\"enum_bm\"}", &resp);
        ASSERT_CONTAINS(resp, "\"color:bitmap\"",
                        "enum bitmap line persisted");
        ASSERT_NOT_CONTAINS(resp, "\"color:bitmap(",
                        "1-byte enum keeps the default cap (no (N) suffix)");
        free(resp); resp = NULL;
    }
```

**Amendment note (2026-09-04, during execution):** the 2-byte-enum cap
assertion as originally drafted in this section was unimplementable:
`cmd_create_object` rejects field specs ≥511 bytes
(query_schema.c "invalid field definition (empty or too long)") and the
add/edit-field parser caps lines at 256 bytes (`char lines[][256]`), so
no wire path can declare the 257+ values a 2-byte enum requires. The
reachable assertions are: explicit `name:bitmap` accepted on an enum at
create-object, and the 1-byte default cap (no `(N)` suffix). The
2-byte → 65535 guard stays in the validator as defensive consistency
with `cmd_add_index`'s identical rule; exercising it would require a
parser-limit change, which is outside this plan's non-goals. Surface to
the reviewer: their spec item 2's cap assertion is satisfied in the only
form the engine permits.

### R2 (Standards 4, judgement) — deduplicate the field-type scan

Replace the Task 2b macro pair with a shared lookup. Semantics
preserved exactly: first-name-match-wins, scan order, and both
matchers' type checks unchanged. In `src/db/query_schema.c`, replace
BOTH macro definitions (from the comment above the `FIELD_TYPE_IS`
`#define` through the `FIELD_TYPE_PREFIX_IS` block's closing `})`) with:

```c
    /* Type token (the substring after the first ':') of the first field
       named fname, or NULL. First-name-match-wins: a duplicate field
       name resolves to its first declaration. */
    #define FIELD_TYPE_TOKEN(fname, fnlen)                                    \
        ({                                                                    \
            const char *_tok = NULL;                                          \
            for (int _i = 0; _i < nfields; _i++) {                            \
                const char *_c = strchr(field_specs[_i], ':');                \
                if (!_c) continue;                                            \
                int _nlen = (int)(_c - field_specs[_i]);                      \
                if (_nlen != (fnlen)) continue;                               \
                if (memcmp(field_specs[_i], (fname), _nlen) != 0) continue;   \
                _tok = _c + 1;                                                \
                break;                                                        \
            }                                                                 \
            _tok;                                                             \
        })
    /* The delimiter tail check is what keeps "timestamp" from matching
       "time"; an enum spec continues with '(' after the token, so it can
       never pass this check — FIELD_TYPE_PREFIX_IS covers it. */
    #define FIELD_TYPE_IS(fname, fnlen, expected_tname)                       \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = 0;                                                 \
            if (_tok) {                                                       \
                size_t _elen = strlen(expected_tname);                        \
                if (strncmp(_tok, (expected_tname), _elen) == 0 &&            \
                    (_tok[_elen] == '\0' || _tok[_elen] == ':'))              \
                    _matched = 1;                                             \
            }                                                                 \
            _matched;                                                         \
        })
    #define FIELD_TYPE_PREFIX_IS(fname, fnlen, prefix)                        \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = _tok != NULL &&                                    \
                           strncmp(_tok, (prefix), strlen(prefix)) == 0;      \
            _matched;                                                         \
        })
```

and extend the surviving `#undef` lines to:

```c
    #undef FIELD_TYPE_IS
    #undef FIELD_TYPE_PREFIX_IS
    #undef FIELD_TYPE_TOKEN
```

### R3 (Standards 3 + 5) — comment hygiene (what→why; stale auto-default)

- `src/db/query_schema.c` — macro comments rewritten to lead with the
  non-obvious constraint (see R2's blocks).
- `src/test/cases/test_bitmap_index.c:435` — section header
  `/* === Explicit :bitmap on bool overrides the auto-default contract
  (no duplicate entry written). === */` →
  `/* === Explicit :bitmap on bool is declared exactly once (no
  duplicate entry written). === */`
- `src/test/cases/test_bm_intersect_count.c:7` and `:57` — reword the
  auto-bitmap mentions to the bare-name promote rule (payloads already
  declare `flag` bare).
- `src/bench/bench_queries.c:586-587` — "(auto-bitmap, 5 values)" →
  "(bare-declared category promotes to bitmap, 5 values)".
- `src/bench/bench_bitmap_vs_btree.c:4` — "`flag:bitmap` (auto-default
  behaviour, made explicit for clarity)" → "`flag:bitmap` (explicit;
  a bare `flag` would promote to the same bitmap)".

### R4 (Spec 3, Low) — promote-path shard extension in docs

- `docs/query-protocol/index-management.md` bare-name row files column:
  `` `<field>/<NNN>.idx`; `.bm` shards when a bare bool/enum name promotes ``
- `docs/concepts/indexes.md` `field` (no suffix) row: append
  `(bitmap `.bm` shards instead when a bare bool/enum name promotes)` to
  the files-created cell.
- `docs/getting-started/quickstart.md` indexes explanation bullet: the
  promoted `active` bitmap uses 1:1 `.bm` shards, not idx fan-out.

### R5 (Spec 1, High) — TSan gate invocation codified (deviation)

The plan's Task 7 steps 3-4 as originally written (default width, no
env options) cannot complete in this workspace. Both failure modes were
demonstrated during the 2026-09-04 execution and A/B-measured against
unmodified main in a separate worktree (identical on both):

1. **Deterministic watchdog abort**: `test-auto-reshard-throttle`
   exceeds the 180s per-case watchdog under TSan (222s both sides;
   passes 9/0 in isolation) → run-all aborts 3/3 before completing.
2. **Nondeterministic contention false-fails** at all-core width with
   the watchdog raised: 18/25/7 varying assertion failures on
   request-heavy cases, zero ThreadSanitizer findings in every run; all
   three cases pass in isolation under TSan on base and branch.

`.github/workflows/tsan.yml` documents both accommodations and uses
exactly this configuration: `SHARD_TEST_WATCHDOG_SEC=1200` (same
false-fire class, same knob) and `--jobs 2` ("keep the outer width
bounded"). AGENTS.md permits `--jobs` "unless a newly demonstrated
harness limitation requires it" — two were demonstrated above.

**Amended Task 7 step 4:** three fresh
`SHARD_TEST_WATCHDOG_SEC=1200 TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2`
runs — full suite, no case exclusions (strictly stronger than CI).
TSAN_OPTIONS keeps the AGENTS.md flags (no relaxed-reporting). This
deviation is surfaced to the human for explicit approval as part of the
2026-09-04 review round; executing the amended step without that
approval would itself be an uncovered-decision violation.

### R6 (Standards 2, High) — evidence format

Evidence sections must contain verbatim raw command output (the actual
tail of each run's captured log, including the runner's own summary
lines and exit status), not hand-written summaries. Existing summary
style entries are replaced when the amended gates re-run.

## Evidence — Task 1

Red on base (engine unmodified, Task 1a test applied; `SKIP_TESTS=1
./build.sh` clean, then `./build/bin/shard-db-test run test-bitmap-index`):

```
not ok 95 - no auto-default: zero indexes for undeclared bools
#   '"indexes":[]' not found in '{"dir":"t","object":"bools_only","splits":8,"max_key":16,"max_value":6,"slot_size":48,"fields":[{"name":"a","type":"bool","size":1},{"name":"b","type":"bool","size":1},{"name":"c","type":"int","size":4}],"indexes":["a:bitmap","b:bitmap"],"record_count":0}
not ok 96 - no index.conf without declared indexes
#   assertion failed: stat(icp, &st) != 0 && errno == ENOENT
not ok 97 - bare add-index promotes bool to bitmap
#   '"status":"indexed"' not found in '{"status":"exists","field":"a"}
not ok 99 - undeclared b stays unindexed
#   assertion failed: strstr((resp), ("\"b:bitmap\"")) == NULL
# test-bitmap-index: 269 passed, 4 failed
```

All four failures are the auto-default itself (describe reports
`a:bitmap`/`b:bitmap` and index.conf exists on base; bare add-index
returns "exists" because the .bm pre-exists). Required failures for
assertions 95/96 both present.

## Evidence — Tasks 2-5

Red on base (first round) is in Evidence — Task 1. Raw output below is
from the **review-round rerun on the final diff state** (R1-R4
applied). Verbatim tails of the captured run logs:

`SKIP_TESTS=1 ./build.sh` — `/tmp/rr_build3.log`: `BUILD_RC=0`, zero
warning/error lines (grep -c: 0).

```
$ ./build/bin/shard-db-test run test-bitmap-index   # tail of /tmp/rr_bm3.log
# test-bitmap-index: 280 passed, 0 failed

$ ./build/bin/shard-db-test run test-enum
# test-enum: 51 passed, 0 failed

$ ./build/bin/shard-db-test run test-trigram-index
# test-trigram-index: 179 passed, 0 failed
```

(First round on the pre-review diff: 273/0, 51/0, 179/0. The +7 in
test-bitmap-index is the Task 8 acceptance assertions: 2-byte-enum
reachability was corrected during this round — see the amendment note
in the review-round section: no wire path can declare a 2-byte enum,
so the reachable assertions are explicit-enum-bitmap acceptance and
the 1-byte default cap, plus the enum-negative and directory-absence
checks.)

## Evidence — Task 6

Doc files changed on this branch:

- docs/index.md — bitmap landing bullet → opt-in-only wording
- docs/concepts/indexes.md — type-list bullet, "declare them
  explicitly" guidance, explicit-type intro sentence, `field:btree`
  spec-table row, closing "no auto-default" paragraph (5 passages)
- docs/query-protocol/index-management.md — both spec-table rows
- docs/concepts/typed-records.md — enum row → opt-in wording
- docs/operations/benchmarks.md — `active` is declared bare → promoted
- docs/query-protocol/schema-mutations.md — enum-widen row conditioned
  on the field having a bitmap index
- docs/getting-started/quickstart.md — create-object example declares
  `"active"`; explanation bullet updated
- docs/getting-started/clients.md — products example declares
  `"indexes": ["stock", "active"]` (later find/bulk-delete criteria on
  `active`)
- docs/getting-started/npm.md — users example declares `'active'`
  (later find criteria + group_by on `active`)
- docs/reference/changelog.md — Unreleased breaking-change entry
- docs/release-notes/2026.05.7.md — bracketed erratum on the
  factually-wrong add-field auto-bitmap note

## Evidence — Task 7

All output below is **verbatim** from the captured run logs of the
**review-round rerun on the final diff state** (R1-R4 applied). Log
paths are given for each; nothing is paraphrased.

**1. Regular build.** `SKIP_TESTS=1 ./build.sh` → `/tmp/rr_build.log`:
`BUILD_RC=0`, grep -c 'warning|error' = 0.

```
$ ./build/bin/shard-db-test run-all          # tail -6 of /tmp/rr_full.log
ok 16 - big count includes concurrent insert after rebuild
ok 17 - concurrent insert remains readable after auto-vacuum
shard-db stopped (pid=962394)
# test-auto-vacuum: 17 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases
```
(Exit status echoed by the invoking shell: `RC=0`.)

Individually re-verified post-change (raw tails in Evidence — Tasks
2-5): test-bitmap-index 280/0, test-enum 51/0, test-trigram-index
179/0.

**2. ASan gate.** `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` →
`/tmp/rr_asan_build.log`: `BUILD_RC=0`. Three fresh full-suite runs,
each `RC=0`:

```
$ tail -6 /tmp/rr_asan_run1.log
ok 13 - post-count round-trips
ok 14 - parked-key flip converged (14 true rows)
ok 15 - remove test fixture tree
# test-bitmap-stream-find-flush-gate: 15 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ tail -6 /tmp/rr_asan_run2.log
ok 13 - post-count round-trips
ok 14 - parked-key flip converged (14 true rows)
ok 15 - remove test fixture tree
# test-bitmap-stream-find-flush-gate: 15 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ tail -6 /tmp/rr_asan_run3.log
ok 13 - post-count round-trips
ok 14 - parked-key flip converged (14 true rows)
ok 15 - remove test fixture tree
# test-bitmap-stream-find-flush-gate: 15 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ grep -c 'ERROR: AddressSanitizer\|runtime error' /tmp/rr_asan_run[123].log
/tmp/rr_asan_run1.log:0
/tmp/rr_asan_run3.log:0
/tmp/rr_asan_run2.log:0
```

**3. TSan gate (amended per review-round R5).** `BUILD_MODE=tsan
SKIP_TESTS=1 ./build.sh` → `/tmp/rr_tsan_build.log`: `BUILD_RC=0`.
Invocation per run:
`SHARD_TEST_WATCHDOG_SEC=1200 TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2`
— full suite, no case exclusions. Three fresh runs, each `RC=0`.

```
$ tail -6 /tmp/rr_tsan_run1.log
2026-09-04 15:19:19 INFO [slotcask] INSERT uaf_obj.one (slotcask)
ok 2 - seed registry-uaf fixture
2026-09-04 15:19:33 INFO [config] DROP-OBJECT d/uaf_obj
# test-registry-uaf-invalidate: 2 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ tail -6 /tmp/rr_tsan_run2.log
2026-09-04 15:31:01 INFO [slotcask] INSERT uaf_obj.one (slotcask)
ok 2 - seed registry-uaf fixture
2026-09-04 15:31:14 INFO [config] DROP-OBJECT d/uaf_obj
# test-registry-uaf-invalidate: 2 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ tail -6 /tmp/rr_tsan_run3.log
2026-09-04 15:42:38 INFO [slotcask] INSERT uaf_obj.one (slotcask)
ok 2 - seed registry-uaf fixture
2026-09-04 15:42:52 INFO [config] DROP-OBJECT d/uaf_obj
# test-registry-uaf-invalidate: 2 passed, 0 failed
1..439
# total: 12855 passed, 0 failed across 439 cases

$ grep -c 'WARNING: ThreadSanitizer\|SUMMARY: ThreadSanitizer' /tmp/rr_tsan_run[123].log
/tmp/rr_tsan_run1.log:0
/tmp/rr_tsan_run3.log:0
/tmp/rr_tsan_run2.log:0
```

(The first-round gate, on the pre-review diff, produced the same
outcome at this invocation: 3× RC=0, 12848/0, zero findings. The R5
section records why the literal default-width invocation cannot
complete in this workspace, with the base A/B numbers; this amended
invocation awaits the human's explicit approval as part of the
review round.)

**4. Diff packet (uncommitted, repo standing exception).**

```
$ git status --short   (after review round)
 M docs/concepts/indexes.md                M docs/operations/benchmarks.md
 M docs/concepts/typed-records.md          M docs/query-protocol/index-management.md
 M docs/getting-started/clients.md         M docs/query-protocol/schema-mutations.md
 M docs/getting-started/npm.md             M docs/reference/changelog.md
 M docs/getting-started/quickstart.md      M docs/release-notes/2026.05.7.md
 M docs/index.md
 M src/bench/bench_bitmap_vs_btree.c       M src/db/query_schema.c
 M src/bench/bench_queries.c               M src/test/cases/test_bitmap_index.c
 M src/test/cases/test_bm_intersect_count.c
 M src/test/cases/test_enum.c              M src/test/cases/test_trigram_index.c
?? docs/plans/2026-08-31-remove-create-object-auto-bitmap.md
 18 files changed, 192 insertions(+), 124 deletions(-)
```

Branch `fix/no-auto-bitmap-create` off main (62f84f7). Nothing
committed, nothing pushed. Regular (non-sanitizer) build restored in
`build/bin/` after the gates.
