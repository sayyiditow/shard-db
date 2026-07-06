# Refactor: split the 28k-line query.c into per-concern translation units

## Nature of this plan

This is a **mechanical, behavior-preserving** refactor — no logic changes. It
cannot be expressed as anchored code hunks (it moves thousands of lines), so it
is a phased procedure with a hard **suite-green gate after every phase** and a
**review checkpoint** (per this repo's CLAUDE.md, Sonnet reviews the diff before
the next phase). Do exactly one file-extraction per phase; never batch.

The goal: `query.c` (28,289 lines, ~40% of source, the file that concurrent
branches collide on and reviewers can't hold in their heads) becomes a handful of
focused files behind the **existing** declarations in `types.h`. No public API
changes; the build output and test results must be identical.

## Execution rules (read first)

- Branch off `main`: `git checkout -b refactor/split-query-c`.
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run-all`
  after **every** phase. If any phase is not `# total: N passed, 0 failed`,
  STOP, do not proceed, and write `PLAN_NOTES.md`.
- Do not change any function body. Moving a function verbatim is allowed;
  editing its logic is not. If a move seems to require a logic change, STOP.
- Leave uncommitted. Stop for review after each phase if running under the
  plan→review loop.

## Target file seams (from CLAUDE.md's own description)

Create these under `src/db/`, one per phase:

| New file | Contents (by concern) |
|----------|-----------------------|
| `query_find.c`     | `cmd_find*`, `cmd_fetch`, `cmd_keys`, `cmd_count*`, scan helpers |
| `query_aggregate.c`| `cmd_aggregate*`, agg hash-table, group-by, having, top-N |
| `query_join.c`     | join planning + execution (`cmd_find` + `join`) |
| `query_bulk.c`     | `cmd_bulk_insert*`, `cmd_bulk_delete*`, `cmd_bulk_update*` |
| `query_plan.c`     | criteria tree, planner (`plan_filter`), `compile_criteria`, `free_compiled_criteria`, `match_typed*`, `cmd_explain*` (calls `parse_criteria_tree` / `compile_criteria_tree` / `plan_filter` directly — keeping it here avoids externalizing those planner internals) |
| `query_schema.c`   | `cmd_create_object`, `drop-object`, add/remove/edit/rename field, add/remove index, `describe-object`, `list-objects` |
| `query_maint.c`    | `cmd_vacuum`, `truncate`, `backup`, `restore`, `recount`, `rebuild_kf`, `reindex`, `estimate_index`, `size`, `orphaned`, `cmd_sequence` (self-contained file/lock-based metadata op) |

Whatever remains stays in `query.c` (or rename the remainder to `query_core.c`
if cleaner). Exact assignment is the executor's call **per function**, but keep
each function whole and in exactly one file.

Executor note on `cmd_edit_fields`: it calls `cmd_add_indexes` internally.
`cmd_add_indexes` is already public (declared in `types.h`), so the cross-file
call needs no `query_internal.h` entry — but expect this function to pull on
index helpers; if any of those are `static`, externalize them per the seam rule
below rather than duplicating.

## The one real hazard: `static` helpers shared across seams

`query.c` has many `static` helpers. When callers and callee land in different
new files, the `static` breaks the build. Handle it with an internal header:

1. Create `src/db/query_internal.h` (once, in Phase 1):
   ```c
   #ifndef QUERY_INTERNAL_H
   #define QUERY_INTERNAL_H
   #include "types.h"
   /* Prototypes for helpers shared across the query_*.c translation units.
      These were `static` inside the monolithic query.c; splitting the file
      forces them to external linkage. Keep this header private to src/db. */
   /* ... prototypes added here as needed, phase by phase ... */
   #endif
   ```
2. When a moved function needs a helper now living in another file: remove that
   helper's `static`, add its prototype to `query_internal.h`, and `#include
   "query_internal.h"` in both files.
3. A helper used within only **one** new file stays `static` there — do not
   externalize needlessly (keeps LTO's inlining surface and avoids namespace
   noise).
4. The same discipline applies to file-scope `static` **data** (lookup tables,
   caches, mutexes, once-flags). If two functions that share a static datum
   would land in different files, do NOT duplicate the definition — a
   duplicated mutex or cache is two locks/caches guarding one resource: a
   silent race, not a link error. Either keep every user of that datum in the
   same file (preferred — let the datum drive the function assignment), or
   externalize it deliberately: drop `static`, keep the definition in exactly
   one `.c`, and add an `extern` declaration to `query_internal.h`. Note each
   externalized datum in `PLAN_NOTES.md`.

## Phase 0 — Baseline

```
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all
ls -l build/bin/shard-db
```
Record the size and `# total:` line. Every later phase must match the test line.

## Phases 1–7 — one file each

For each target file in the table, in the order listed (start with the most
self-contained — `query_maint.c` and `query_schema.c` tend to have the fewest
cross-seam statics; `query_plan.c` has the most, do it last):

1. Add the new `.c` to `build.sh`'s source lists. There are exactly **four**
   lists that include `src/db/query.c` — add the new file next to `query.c` in
   all four:
   - the daemon binary (`gcc ... -o shard-db ...`),
   - the `LIB_SRCS` embedded static-library list (`libshard-db.a`) — the
     easiest to miss; forgetting it only breaks embedded-mode users,
   - the test binary (`gcc ... -o shard-db-test ...`),
   - the bench binary (`gcc ... -o shard-db-bench ...`).
   Verify with `grep -c 'query_<name>\.c' build.sh` → must print 4.
2. Move the functions for that concern from `query.c` into the new file
   verbatim, with the includes the file needs (`#include "types.h"`, plus
   `query_internal.h` if it uses shared helpers).
3. Externalize any now-cross-file `static` helpers via `query_internal.h`.
4. `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all`.
5. Confirm `# total:` matches Phase 0. If not, STOP + `PLAN_NOTES.md`.
6. (Under the review loop) stop for a diff review before the next phase.

## Definition of done

- `query.c` (or `query_core.c`) is materially smaller; each new file is a
  coherent concern.
- `run-all` matches the Phase-0 line exactly.
- No function body changed (a `git log -p` / diff review shows only moves +
  `static` removals + `#include`/build.sh additions).
- Binary size within noise of baseline (this refactor is for maintainability,
  not size — size is plan 07's job).
- Leave uncommitted.

## Why this precedes plan 06

The `TypeDescriptor` table refactor (plan 06) edits the field-type switches that
are scattered through what is currently `query.c`. Doing that after the split
means each switch lives in a small, reviewable file (`query_plan.c` etc.) instead
of the monolith — far lower risk. Land this first.
