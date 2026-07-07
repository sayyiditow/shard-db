# Coverity: resource leaks (missing cleanup on error/early-return paths)

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-resource-leaks main`.
- Do the four tasks below **in order**. They touch four different files and are independent of each other.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all four tasks are done: `./build/bin/shard-db-test run-all`.
- Every insertion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

This plan fixes four Coverity-flagged resource leaks, each an error/early-return path that forgets a cleanup step its sibling paths in the same function already do correctly. Full triage context: `docs/coverity-triage-2026-07.md`.

**Two related CIDs are intentionally NOT touched by this plan**: CID 1696460 (`cmd_not_exists`) and CID 1696409 (`cmd_exists_multi`) are both marked "TP (minor, already ack'd)" in the triage doc — their OOM leak paths are already reviewed and accepted as-is (the code has a comment documenting this). Do not modify either function as part of this plan.

The four CIDs fixed here:

1. **CID 1696481** — `warmup_thread` (`src/db/server.c`): a `realloc` failure jumps past a `closedir(dd)` that every other path through the loop reaches, leaking a `DIR *`.
2. **CID 1696456 + CID 1696441** — `dispatch_nql_query` (`src/db/server.c`): the very first parse-failure branch returns without calling `nql_free_command`, unlike the two later branches in the same function. Two Coverity reports of the same root cause (different fields — `filter`/`having`/`aggs` — depending on which point in `nql_parse_command` failed), one fix.
3. **CID 1696478** — `cmd_get_multi` (`src/db/storage.c`): an OOM `realloc` failure during key parsing leaks every previously-parsed entry's `key`/`wire_key` strings, and — worse — leaves `entries` NULL while `key_count` is still nonzero, so the function falls through into dereferencing a NULL `entries` array instead of hitting the `key_count == 0` early-return.
4. **CID 1696472** — `bulk_ins_run` (`src/db/query_bulk.c`): the query-timeout cleanup branch frees `records`/`arena`/`idx_pairs*` but not `wire_keys`, unlike the two sibling cleanup blocks (validation-failure and keygen-failure) a few lines above and below it.

---

## Task 1 — `warmup_thread` leaks a `DIR *` on realloc failure (CID 1696481)

### The bug

`src/db/server.c`'s `warmup_thread` collects per-shard warmup tasks while walking each tenant directory. On `realloc` failure while growing `kf_tasks`, it jumps to `done_collect`, skipping the `closedir(dd)` call that every other path through the loop (including the natural loop-bottom fallthrough) reaches:

```c
        DIR *dd = opendir(dir_path);
        if (!dd) continue;
        struct dirent *de;
        while ((de = readdir(dd)) && server_running) {
            if (de->d_name[0] == '.') continue;

            SlotcaskDb *sdb = warmup_object_open(a->db_root,
                                                 dirs_copy[di], de->d_name);
            if (!sdb) continue;
            objects++;

            /* Per-shard kf tasks for this object */
            for (int s = 0; s < sdb->num_shards; s++) {
                if (n_kf == kf_cap) {
                    size_t new_cap = kf_cap ? kf_cap * 2 : 64;
                    WarmupKfTask *nt = realloc(kf_tasks, new_cap * sizeof(WarmupKfTask));
                    if (!nt) goto done_collect;
                    kf_tasks = nt;
                    kf_cap = new_cap;
                }
                kf_tasks[n_kf++] = (WarmupKfTask){
                    .sdb = sdb,
                    .shard_idx = s,
                    .kf_count = &kf_count,
                };
            }

            /* Recursively collect index file paths for this object */
            char idx_dir[PATH_MAX];
            snprintf(idx_dir, sizeof(idx_dir), "%s/%s/indexes",
                     dir_path, de->d_name);
            warmup_collect_idx(idx_dir, idx_ext, &idx_tasks, &n_idx, &idx_cap,
                               &idx_count);
        }
        closedir(dd);
    }

done_collect:
```

The `goto done_collect` exits both the `while (readdir)` loop and the outer `for (di ...)` loop in one jump, so `closedir(dd)` never runs for the directory handle open at the time of the OOM. This is a background thread that runs once at startup (and potentially again — check call sites — so it can leak more than once), and the leaked `DIR *` holds an open file descriptor for the process lifetime.

### The fix

In `src/db/server.c`, find this exact line:

```c
                    WarmupKfTask *nt = realloc(kf_tasks, new_cap * sizeof(WarmupKfTask));
                    if (!nt) goto done_collect;
```

Replace it with:

```c
                    WarmupKfTask *nt = realloc(kf_tasks, new_cap * sizeof(WarmupKfTask));
                    if (!nt) { closedir(dd); goto done_collect; }
```

### Regression test

None. This is a background best-effort cache-warming thread; forcing a `realloc` failure deterministically inside it (without a fault-injection harness this codebase doesn't have) isn't practical, and the fix is a single-line addition of a call already made on every other path through this same loop. Covered by the full test suite continuing to pass unchanged (no behavior change on the non-OOM path, which every test that starts a daemon already exercises via the normal warmup-thread startup path).

---

## Task 2 — `dispatch_nql_query` leaks `NqlCommand` fields on first parse failure (CID 1696456, CID 1696441)

### The bug

`src/db/server.c`'s `dispatch_nql_query` calls `nql_parse_command`, which `memset`s its output struct to zero up front but can then allocate `cmd.filter`, `cmd.having`, and/or `cmd.aggs` at various points before a later parse failure returns -1 (e.g. filter parses successfully, then a following flag/agg-spec fails to parse). The function's very first failure branch does not call `nql_free_command`, even though the two later failure branches in the same function do:

```c
static void dispatch_nql_query(const char *raw_db_root, const char *line,
                                const char *client_ip) {
    NqlCommand cmd;
    if (nql_parse_command(line, &cmd) < 0) {
        OUT("{\"error\":\"%s\"}\n", cmd.err);
        return;
    }

    /* Auth — same precedence as JSON path */
    if (!is_ip_trusted(client_ip)) {
        if (!cmd.auth[0] || !is_authorized(cmd.auth, cmd.dir, cmd.obj, "find")) {
            LOG_AUDIT(LOG_SUB_AUTH, "AUTH failed: ip=%s nql_mode=%d", client_ip, cmd.mode);
            OUT("{\"error\":\"auth failed\"}\n");
            nql_free_command(&cmd);
            return;
        }
    }

    if (!is_valid_object(cmd.obj)) {
        OUT("{\"error\":\"invalid object name\"}\n");
        nql_free_command(&cmd);
        return;
    }
```

Any criteria tree (`cmd.filter`/`cmd.having`) or agg-spec array (`cmd.aggs`) allocated before the failing token is parsed leaks on every request that trips this specific branch (e.g. `find users where name = "x" && bogus_flag`).

### The fix

In `src/db/server.c`, find this exact block:

```c
    NqlCommand cmd;
    if (nql_parse_command(line, &cmd) < 0) {
        OUT("{\"error\":\"%s\"}\n", cmd.err);
        return;
    }
```

Replace it with:

```c
    NqlCommand cmd;
    if (nql_parse_command(line, &cmd) < 0) {
        OUT("{\"error\":\"%s\"}\n", cmd.err);
        nql_free_command(&cmd);
        return;
    }
```

This is safe unconditionally: `nql_parse_command` always `memset`s `cmd` to zero before parsing (so `cmd.filter`/`cmd.having`/`cmd.aggs` are NULL if nothing was allocated yet), `free_criteria_tree` (called by `nql_free_command` on `cmd.filter`/`cmd.having`) is NULL-safe (`src/db/query_plan.c`: `void free_criteria_tree(CriteriaNode *n) { if (!n) return; ... }`), and `free(NULL)` (called on `cmd.aggs`) is a no-op.

### Regression test

None — see Background/Task 1 rationale style: this is an additive cleanup call with no effect on the non-error path (every successful NQL query in the suite already exercises `nql_parse_command` succeeding and reaching the existing `nql_free_command(&cmd)` call at the function's tail). Reliably triggering *this specific* parse-failure sub-branch with a filter/having/aggs already allocated isn't necessary to prove correctness — the fix mirrors the two already-correct sibling branches in the same function verbatim, and AddressSanitizer/leak-detection is out of scope for this test suite's existing idioms (none of the other Coverity resource-leak fixes in this repo's plans add ASan-based tests either). Covered by the full suite continuing to pass unchanged.

---

## Task 3 — `cmd_get_multi` leaks parsed entries and risks a NULL-`entries` fallthrough on OOM (CID 1696478)

### The bug

`src/db/storage.c`'s `cmd_get_multi` parses each key into a growable `entries` array. On `realloc` failure while growing it, `xrealloc_or_free` (which frees the **old** buffer when the new allocation fails — see `src/db/types.h`) has already released the array holding every previously-parsed entry's `key`/`wire_key` pointers before the caller can free them:

```c
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiGetEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) { entries = NULL; break; }
                entries = t;
            }
```

Two problems:

1. **Leak**: every entry already stored at indices `0..key_count-1` had its `key`/`wire_key` heap strings allocated by `parse_multi_key` a few lines above. Once `xrealloc_or_free` frees the array on failure, those strings become unreachable — never freed.
2. **NULL-deref risk**: after the `break`, `entries` is NULL but `key_count` is still nonzero (it was incremented for every entry parsed before this failure). The function's only NULL guard is:

   ```c
   if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }
   ```

   which does not trigger (since `key_count != 0`), so execution falls through into `for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;` — a NULL pointer dereference on a real (if rare) OOM.

### The fix

In `src/db/storage.c`, find this exact block:

```c
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiGetEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) { entries = NULL; break; }
                entries = t;
            }
```

Replace it with:

```c
            if (key_count >= key_cap) {
                key_cap *= 2;
                /* Plain realloc (not xrealloc_or_free): on failure we still
                   need the old `entries` array intact to free each already-
                   parsed entry's key/wire_key before dropping the array
                   itself (CID 1696478). Resetting key_count to 0 here also
                   makes the `key_count == 0` early-return below fire
                   correctly instead of falling through to a NULL entries[]
                   dereference. */
                MultiGetEntry *t = realloc(entries, key_cap * sizeof(*t));
                if (!t) {
                    for (int j = 0; j < key_count; j++) {
                        free(entries[j].key);
                        free(entries[j].wire_key);
                    }
                    free(entries);
                    entries = NULL;
                    key_count = 0;
                    break;
                }
                entries = t;
            }
```

This relies on the existing early-return a few lines below being unchanged:

```c
    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }
```

With `key_count` now reset to 0 and `entries` NULL, this branch fires correctly (`free(NULL)` is a no-op) instead of the function falling through to use a NULL `entries`.

### Regression test

None. Forcing this specific `realloc` to fail deterministically (after `key_cap` has already doubled at least once, i.e. after more than 256 keys have been parsed) requires a fault-injection harness this codebase doesn't have. Covered by the full test suite continuing to pass unchanged — `cmd_get_multi`'s non-OOM path (including the >256-key growth path itself) is already exercised by the existing get-multi test cases, and this fix changes nothing on that path (identical growth behavior on success, `entries = t` unchanged).

---

## Task 4 — `bulk_ins_run` timeout cleanup omits `wire_keys` (CID 1696472)

### The bug

`src/db/query_bulk.c`'s `bulk_ins_run` has three cleanup blocks that all run before any write phase: a validation-failure bail, a keygen-failure bail, and a query-timeout bail. The first two correctly free `wire_keys` (including each per-record `strdup`'d wire-key string); the timeout bail does not:

```c
    /* Validation failure: refuse the whole batch up front so we don't
       half-write some records with mangled keys. */
    if (validation_failed_idx >= 0) {
        if (wire_keys) {
            for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        }
        free(records); free(wire_keys);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"bulk-insert validation failed at record %d: malformed key for auto_key mode\"}\n",
            validation_failed_idx);
        return 1;
    }
```

```c
        if (keygen_failed) {
            free(uuid_pool);
            if (wire_keys) {
                for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
            }
            free(records); free(wire_keys);
            arena_free(arena);
            for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
            free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
            OUT("{\"error\":\"bulk-insert key generation failed: %s\"}\n",
                auto_key_mode == AK_UUID ? "random source unavailable"
                                         : "sequence unavailable");
            return 1;
        }
```

```c
    /* If parse tripped the deadline, abort before any write phase. Same
       cleanup order as the OOM bail below. */
    if (dl.timed_out) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"query_timeout\"}\n");
        return 1;
    }
```

On an `auto_key`-enabled object, a bulk-insert that trips the per-request timeout mid-parse leaks every already-`strdup`'d `wire_keys[i]` string plus the `wire_keys` array itself.

### The fix

In `src/db/query_bulk.c`, find this exact block:

```c
    /* If parse tripped the deadline, abort before any write phase. Same
       cleanup order as the OOM bail below. */
    if (dl.timed_out) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"query_timeout\"}\n");
        return 1;
    }
```

Replace it with:

```c
    /* If parse tripped the deadline, abort before any write phase. Same
       cleanup order as the OOM bail below. */
    if (dl.timed_out) {
        if (wire_keys) {
            for (size_t i = 0; i < rec_count; i++) free(wire_keys[i]);
        }
        free(records); free(wire_keys);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        OUT("{\"error\":\"query_timeout\"}\n");
        return 1;
    }
```

This mirrors the validation-failure block above it exactly (same `wire_keys` free loop, same `free(records); free(wire_keys);` pairing).

### Regression test

None. Deterministically tripping the per-request timeout *specifically inside the parse loop* of a bulk-insert on an `auto_key`-enabled object (so `wire_keys` is non-NULL) would require either a sub-millisecond `timeout_ms` racing real parse work (flaky) or a fault-injection hook this codebase doesn't have for `query_deadline_tick`. Covered by the full test suite continuing to pass unchanged — the fix is scoped to a branch only reached on timeout, with zero effect on the non-timeout path already exercised by every existing auto-key bulk-insert test.

---

## Final verification

After all four tasks are complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** showing `# total: N passed, 0 failed`, with N unchanged from the pre-fix baseline (no new test cases were added in this plan). Do not consider this plan done, and do not report success, without pasting this real output.
