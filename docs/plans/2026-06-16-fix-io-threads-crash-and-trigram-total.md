# Fix: startup NULL-deref on IO_THREADS, and find total:true returning 0 for trigram leaves

## Execution rules (read before doing anything)

- Branch off `main`: `git checkout -b fix/io-threads-crash-and-trigram-total`.
- Do the two tasks below in order. They are independent and touch different files; either could be done first, but do Task 1 first since it blocks starting the daemon at all.
- Build with `SKIP_TESTS=1 ./build.sh`. Once both tasks are done, build and run the full suite: `./build/bin/shard-db-test run-all`. Do not claim a step passed without pasting the actual command output (specifically the final `# total: N passed, 0 failed` line). If any test fails, stop and report — do not "fix" it by weakening a test.
- Leave all changes **uncommitted** in the working tree when done. Do not `git commit`, `git push`, or open a PR. Do not touch `main`.
- Every edit below is located by **quoted anchor text**, not line numbers (line numbers drift). If a quoted anchor is not found verbatim in the target file, STOP — do not guess or reinterpret the surrounding code. Instead write a `PLAN_NOTES.md` at the repo root describing exactly what you searched for and what you found instead, and stop there.
- Do not refactor, rename, or "clean up" anything beyond what each task specifies.

---

## Background

Two unrelated bugs, both in the C query/config engine, found via live reproduction + gdb + git archaeology:

1. **Startup crash (SIGSEGV) whenever `db.env` has an active `IO_THREADS=` line.** `g_io_threads` is a macro (`#define g_io_threads (g_db->io_threads)` in `src/db/shard_db_internal.h`) that dereferences the global `ShardDb *g_db` instance. `load_db_root()` in `src/db/config.c` runs and parses `IO_THREADS=` **before** `g_db` is allocated (confirmed in `src/db/main.c`, where `load_db_root()` is called for `start`/`server`/`stop`/`status`/`query` before any `ShardDb` is created). Every other field in the same parsing function that backs a `ShardDb`-struct member is guarded with `if (g_db) ...`; this one line was missed when the embedded-mode refactor (commit `3539192`) converted these globals into `g_db->`-backed macros. Result: 100% reproducible NULL-pointer dereference / SIGSEGV on every single invocation of the daemon as long as `db.env` contains `export IO_THREADS=0` (which is the shipped default).

2. **`find` with `"total":true` returns `0` instead of the real count, specifically when the query's matching index is a trigram index** (e.g. `icontains`/`contains`/`not_contains` on a trigram-indexed varchar field), even though the returned rows themselves are correct. `count` with the identical criteria returns the correct value. Root cause: `idx_count_for_leaf()` in `src/db/query.c` (used by `find`'s total-count path via `fp_compute_total()`) builds a synthetic `CriteriaNode leaf_node` by hand for its `IT_TRIGRAM` branch, but never runs it through `compile_one()` (the function that populates `CriteriaNode.compiled`, normally invoked by `compile_criteria_tree()` when criteria are first parsed). The match logic that verifies each trigram KeySet candidate against the real criterion checks `if (!n->compiled) return 0;` before calling `match_typed()` — so a node with `compiled == NULL` always fails to match, and every single candidate is rejected, yielding a count of `0`. `cmd_count`'s equivalent trigram path does not hit this bug because it passes the real, already-compiled `tree` (compiled once up front by `compile_criteria_tree()` when criteria were parsed) instead of a freshly-built, uncompiled node.

---

## Task 1: Fix the `g_io_threads` NULL-deref in `load_db_root()`

**File:** `src/db/config.c`

**Anchor** (exact, locate this block):

```c
        } else if (strncmp(p, "THREADS=", 8) == 0) {
            g_max_threads = atoi(p + 8);
        } else if (strncmp(p, "IO_THREADS=", 11) == 0) {
            g_io_threads = atoi(p + 11);
        } else if (strncmp(p, "POOL_CHUNK=", 11) == 0) {
            g_pool_chunk = atoi(p + 11);
```

Replace with (only the `IO_THREADS=` branch changes — `g_max_threads` and `g_pool_chunk` are plain `extern int` globals, not `ShardDb`-backed macros, so they do not need guarding and must NOT be changed):

```c
        } else if (strncmp(p, "THREADS=", 8) == 0) {
            g_max_threads = atoi(p + 8);
        } else if (strncmp(p, "IO_THREADS=", 11) == 0) {
            if (g_db) g_io_threads = atoi(p + 11);
        } else if (strncmp(p, "POOL_CHUNK=", 11) == 0) {
            g_pool_chunk = atoi(p + 11);
```

### Why this is correct

`g_io_threads` expands to `g_db->io_threads` (see `src/db/shard_db_internal.h`: `#define g_io_threads (g_db->io_threads)`). `load_db_root()` is called to determine `DB_ROOT` (and incidentally parses every other `db.env` key) before the `ShardDb` instance is allocated for CLI-driven commands (`start`, `server`, `stop`, `status`, `query` — see `src/db/main.c`). On a second pass, after `g_db` is allocated, the same env file is (or may be) re-parsed with `g_db` non-NULL, at which point the value is correctly applied. This mirrors the exact pattern already used for every other `ShardDb`-backed field in this function, e.g. `TIMEOUT=`, `PORT=`, `LOG_LEVEL=`, `LOG_RETAIN_DAYS=`, `WORKERS=`, `GLOBAL_LIMIT=`, `MAX_CONCURRENT_QUERIES=` (all visible a few lines above this anchor in the same function, all wrapped in `if (g_db) ...`).

### Invariant / edge case

- Do not add the `if (g_db)` guard to `THREADS=` or `POOL_CHUNK=` — confirm via `grep -n "^int g_max_threads\|^int g_pool_chunk" src/db/types.h` that these remain plain externs (not macros) before/after the change. If that grep shows they are *not* plain externs anymore (i.e. someone has since converted them to `ShardDb`-backed macros too), stop and write `PLAN_NOTES.md` — do not guess whether they need guarding.

### How to verify this task locally (manual, optional but recommended)

After building, run `./build/bin/shard-db start` (with `db.env`'s shipped `export IO_THREADS=0` line active/uncommented) and confirm it does not crash (exit code 0, daemon comes up, `./build/bin/shard-db status` reports running). Then `./build/bin/shard-db stop`.

---

## Task 2: Fix `find` total:true returning 0 for trigram-indexed leaves

**File:** `src/db/query.c`

**Anchor** (exact, locate this function — note the comment immediately above the function signature is part of the anchor):

```c
static size_t idx_count_for_leaf(const char *db_root, const char *object,
                                 const Schema *sch, const FieldSchema *fs,
                                 SearchCriterion *leaf, QueryDeadline *dl) {
    int picked = pick_index_for_leaf(db_root, object, leaf);
    const TypedField *tf = resolve_idx_field(fs->ts, leaf->field);

    if (picked == IT_TRIGRAM) {
        KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                   sch->splits, leaf, dl);
        if (!tg_ks) return 0;
        CollectedHash *entries = NULL;
        size_t n = 0;
        keyset_to_collected_hashes(tg_ks, sch->splits, &entries, &n);
        /* Build a single-leaf criteria node around `leaf` so
           parallel_indexed_count verifies only that leaf (no tree). */
        CriteriaNode leaf_node = { .kind = CNODE_LEAF, .leaf = *leaf,
                                   .children = NULL, .n_children = 0 };
        size_t cnt = parallel_indexed_count(db_root, object, sch,
                                            entries, (int)n,
                                            &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
        free(entries);
        keyset_free(tg_ks);
        return cnt;
    }
```

Replace with:

```c
static size_t idx_count_for_leaf(const char *db_root, const char *object,
                                 const Schema *sch, const FieldSchema *fs,
                                 SearchCriterion *leaf, QueryDeadline *dl) {
    int picked = pick_index_for_leaf(db_root, object, leaf);
    const TypedField *tf = resolve_idx_field(fs->ts, leaf->field);

    if (picked == IT_TRIGRAM) {
        KeySet *tg_ks = build_keyset_from_trigram(db_root, object,
                                                   sch->splits, leaf, dl);
        if (!tg_ks) return 0;
        CollectedHash *entries = NULL;
        size_t n = 0;
        keyset_to_collected_hashes(tg_ks, sch->splits, &entries, &n);
        /* Build a single-leaf criteria node around `leaf` so
           parallel_indexed_count verifies only that leaf (no tree).
           Must compile it (compile_one) before use: parallel_indexed_count's
           per-record verification checks node->compiled and treats NULL as
           "never matches", so an uncompiled node here silently zeroes every
           count. Heap-allocate cc: free_compiled_criteria() calls free() on
           the array pointer itself (not just inner buffers), so it must be
           paired with calloc, never a stack variable. */
        CompiledCriterion *cc = calloc(1, sizeof(CompiledCriterion));
        compile_one(cc, leaf, fs->ts);
        CriteriaNode leaf_node = { .kind = CNODE_LEAF, .leaf = *leaf,
                                   .compiled = cc,
                                   .children = NULL, .n_children = 0 };
        size_t cnt = parallel_indexed_count(db_root, object, sch,
                                            entries, (int)n,
                                            &leaf_node, (FieldSchema *)fs, dl, NULL, 1);
        free_compiled_criteria(cc, 1);
        free(entries);
        keyset_free(tg_ks);
        return cnt;
    }
```

### Why this is correct

`compile_one(CompiledCriterion *cc, const SearchCriterion *c, const TypedSchema *ts)` (defined earlier in `src/db/query.c`) is the exact function `compile_criteria_tree()` calls per-leaf to populate `CriteriaNode.compiled` for every criterion parsed from a request. The trigram branch here was building a `CriteriaNode` by hand (bypassing `compile_criteria_tree()` entirely, since there's no parse step — the leaf already exists as a `SearchCriterion*`), and never ran it through `compile_one()`, so `.compiled` stayed `NULL` (zero-initialized by the designated initializer). Downstream, `parallel_indexed_count()` verifies each KeySet candidate by walking the criteria node and calling its match path, which contains:

```c
if (!n->compiled) return 0;
return match_typed(rec, n->compiled, fs);
```

So with `.compiled == NULL`, every candidate is rejected, and the only quantity ever returned is `0` — independent of how many real matches exist. This exactly matches the live-reproduced symptom (`find` returns correct rows but `total:0`; `count` — which calls `parallel_indexed_count` with the real pre-compiled `tree` from `compile_criteria_tree()`, not a hand-built node — returns the correct number).

`free_compiled_criteria(CompiledCriterion *arr, int n)` (declared in `src/db/types.h`, defined in `src/db/query.c`) frees both the inner buffers `compile_one` may have allocated (e.g. for composite-field or regex criteria) **and the array pointer itself** via a trailing `free(arr)` — every existing caller (`free_criteria_tree`, `recompile_criteria_tree`) pairs it with a prior `calloc`, never a stack variable. Call it on `cc` with `n=1` before returning, on every return path out of the `IT_TRIGRAM` branch. There is only one return path (the `return cnt;` at the end of the branch) since the two earlier `return 0;` statements (`if (!tg_ks) return 0;`) are before `cc` is allocated, so no other call site needs the free.

### Invariants / edge cases

- `cc` **must be heap-allocated** via `calloc(1, sizeof(CompiledCriterion))`, never a stack-local `CompiledCriterion` — `free_compiled_criteria()` ends with `free(arr)`, so calling it on a stack address is undefined behavior. Check the `calloc` return for `NULL` is unnecessary here only in the sense that the codebase's existing convention doesn't null-check this particular allocation pattern (see `compile_criteria_tree`'s own `calloc` calls) — match that convention, don't add new error handling beyond what's shown.
- `leaf_node.compiled = cc;` then `free_compiled_criteria(cc, 1);` after `parallel_indexed_count` returns — `cc` must not be freed or reused after this point.
- Do not change the `IT_BITMAP` or `IT_BTREE` (default) branches of this function — they do not go through `CriteriaNode`/`.compiled` at all (`IT_BITMAP` uses `bm_popcount_for_crit`/`bm_popcount_generic_for_crit` directly on the `SearchCriterion*`; `IT_BTREE` uses `idx_count_cb` via `btree_dispatch`), so they are unaffected by this bug and need no change.
- Confirm `compile_one` and `free_compiled_criteria` are visible at this point in the file (both are defined earlier in `src/db/query.c`, well before `idx_count_for_leaf`, so no forward-declaration is needed) — if the build fails with an implicit-declaration warning/error for either symbol, stop and write `PLAN_NOTES.md` rather than adding a forward declaration yourself.

### How to verify this task locally (manual, optional but recommended)

Create an object with a trigram-indexed varchar field, insert rows where some contain a substring and some don't, then run e.g.:

```bash
./build/bin/shard-db query '{"mode":"find","dir":"...","object":"...","criteria":[{"field":"title","op":"icontains","value":"honda"}],"total":true}'
```

Confirm `"total"` in the response matches the row count from:

```bash
./build/bin/shard-db count <dir> <obj> '[{"field":"title","op":"icontains","value":"honda"}]'
```

---

## Final step

Run the full suite and paste the real output:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Confirm the final line reads `# total: N passed, 0 failed` with no failures. Leave the branch checked out with changes uncommitted — do not commit, push, or open a PR.
