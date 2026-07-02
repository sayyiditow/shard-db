# Fix: add-index/remove-index missing exclusive lock

## Execution rules (read first)

- Branch off `main` before making any changes: `git checkout -b fix/add-index-exclusive-lock`.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- **Never claim the task passed without pasting the real command output.** "# total: N passed, 0 failed" from the actual test binary is the only acceptable evidence it's done.
- The edit below is located by **quoted anchor text** from the current file, not line numbers (line numbers drift; another model may be working concurrently on a separate branch). If the anchor is not found character-for-character in the target file, **stop immediately** and write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead — do not guess, do not reinterpret, do not improvise a fix.
- Leave all changes **uncommitted** on the branch when done. Do not `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, done by the user.

## Background (why this matters — do not re-derive, just fix as specified)

`add-index` and `remove-index` are the only two commands that destructively `unlink()` and rebuild live index files (`.tg`/`.idx`/`.bm`) while classified under `mode_is_write` (shared per-object rdlock) instead of `mode_is_schema` (exclusive per-object wrlock). Every other command that does this same style of unlink-and-rebuild (`vacuum`, `truncate`, offline `reindex`, `restore`) is correctly wrapped in an exclusive wrlock. Because `insert`/`bulk-insert` (which auto-maintain indexes on-the-fly via `btree_idx_insert`) are *also* only `mode_is_write` (shared), nothing prevents a concurrent insert's on-the-fly index write from racing against `add-index`'s `bt_stream_build_open()`, which does `btree_cache_invalidate(path); unlink(path);` then rebuilds the file via raw, non-locked page writes (`bt_stream_build_add`). The btree cache keys entries purely by path string with no inode check, so a concurrent insert can either have its write silently discarded (landing on the soon-to-be-unlinked old file) or corrupt the B-tree structure the rebuild is actively writing. This was confirmed as the root cause of a production trigram-search bug (index files contained plausible byte volume but returned drastically undercounted results for common search terms) that got dramatically worse the longer the rebuild took (multi-field rebuilds run longer than single-field ones, giving concurrent catchup-ingestion writes a bigger window to corrupt the build).

Note: a separate `remove-index` bare-field-name-vs-typed-suffix mismatch (`"title"` not matching a stored `"title:trigram"` line) was investigated and explicitly rejected as an engine bug — a field can legitimately have two coexisting indexes (e.g. `body` btree + `body:trigram`), so the engine correctly requires the exact stored index name and cannot safely guess between them. That's a caller/docs issue, not something this plan touches.

---

## Task 1 — Fix the locking gap (server.c)

### File: `src/db/server.c`

Find this exact block:

```c
/* Commands that mutate data (insert/delete/update/bulk/add-index/put-file/sequence).
   Take per-object rdlock during dispatch so rebuild (wrlock) blocks them briefly. */
static int mode_is_write(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "insert") == 0 || strcasecmp(m, "update") == 0 ||
           strcasecmp(m, "delete") == 0 || strcasecmp(m, "bulk-insert") == 0 ||
           strcasecmp(m, "bulk-insert-delimited") == 0 || strcasecmp(m, "bulk-delete") == 0 ||
           strcasecmp(m, "bulk-update") == 0 || strcasecmp(m, "bulk-update-delimited") == 0 ||
           strcasecmp(m, "add-index") == 0 || strcasecmp(m, "remove-index") == 0 ||
           strcasecmp(m, "put-file") == 0 ||
           strcasecmp(m, "delete-file") == 0 ||
           strcasecmp(m, "sequence") == 0;
}
/* Schema/rebuild commands — take exclusive wrlock. */
static int mode_is_schema(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "rename-field") == 0 || strcasecmp(m, "remove-field") == 0 ||
           strcasecmp(m, "add-field") == 0 || strcasecmp(m, "edit-field") == 0 ||
           strcasecmp(m, "vacuum") == 0 ||
           strcasecmp(m, "truncate") == 0 ||
            strcasecmp(m, "migrate-storage-version") == 0 ||
            strcasecmp(m, "migrate") == 0;
}
```

Replace it with:

```c
/* Commands that mutate data (insert/delete/update/bulk/put-file/sequence).
   Take per-object rdlock during dispatch so rebuild (wrlock) blocks them briefly. */
static int mode_is_write(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "insert") == 0 || strcasecmp(m, "update") == 0 ||
           strcasecmp(m, "delete") == 0 || strcasecmp(m, "bulk-insert") == 0 ||
           strcasecmp(m, "bulk-insert-delimited") == 0 || strcasecmp(m, "bulk-delete") == 0 ||
           strcasecmp(m, "bulk-update") == 0 || strcasecmp(m, "bulk-update-delimited") == 0 ||
           strcasecmp(m, "put-file") == 0 ||
           strcasecmp(m, "delete-file") == 0 ||
           strcasecmp(m, "sequence") == 0;
}
/* Schema/rebuild commands — take exclusive wrlock. add-index/remove-index
   belong here, not in mode_is_write: both unlink() and rebuild index files
   in place (bt_stream_build_open in btree.c does
   btree_cache_invalidate(path); unlink(path); then rebuilds via raw,
   non-locked page writes). The btree cache keys entries by path string
   with no inode check, so a concurrent per-record insert's on-the-fly
   index update (btree_idx_insert, same shared rdlock class as insert)
   can land on the same path mid-rebuild and either get silently
   discarded or corrupt the pages bt_stream_build_add is writing.
   Exclusive wrlock here serialises against that, same as
   vacuum/truncate/reindex already do for the identical unlink+rebuild
   pattern. */
static int mode_is_schema(const char *m) {
    if (!m) return 0;
    return strcasecmp(m, "rename-field") == 0 || strcasecmp(m, "remove-field") == 0 ||
           strcasecmp(m, "add-field") == 0 || strcasecmp(m, "edit-field") == 0 ||
           strcasecmp(m, "vacuum") == 0 ||
           strcasecmp(m, "truncate") == 0 ||
           strcasecmp(m, "add-index") == 0 || strcasecmp(m, "remove-index") == 0 ||
            strcasecmp(m, "migrate-storage-version") == 0 ||
            strcasecmp(m, "migrate") == 0;
}
```

**That is the entire change.** Both the JSON dispatch (`took_wrlock = mode_is_schema(mode)` / `took_rdlock = !took_wrlock && mode_is_write(mode)`) and the legacy fast-path dispatch (`fast_wr = mode_is_schema(cmd)` / `fast_rd = !fast_wr && mode_is_write(cmd)`) read these two predicate functions as their single source of truth — no other call site needs editing. `is_write` (used for graceful-shutdown drain tracking) is `mode_is_write(m) || mode_is_schema(m)`, so moving `add-index`/`remove-index` between the two lists does not change whether they're tracked as in-flight writes — only which lock class they take. Token permission checks (`is_authorized` / `mode_admin_level` / `mode_is_data_write`) are a completely separate classification system in this file and are unaffected by this change — do not touch them.

### Invariant this restores

While `add-index`/`remove-index` run, no concurrent `insert`/`update`/`delete`/`bulk-*`/`put-file`/`delete-file`/`sequence`/other `add-index`/`remove-index` call can proceed against the same object — they block until the rebuild's wrlock releases (identical to how `vacuum`/`truncate` already behave). This means `add-index`/`remove-index` now take noticeably longer to become available under write load (they queue behind in-flight writes, then hold the object exclusively) — this is a deliberate, correct tradeoff for correctness, not a regression.

---

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no compile errors or warnings in `server.c`.
2. `./build/bin/shard-db-test run-all` — paste the real output; must show `# total: N passed, 0 failed` with N equal to the pre-change total (no existing test should start failing — this change only affects lock class, not any command's output format).

Do not report this plan as complete without pasting the actual output of step 2.
