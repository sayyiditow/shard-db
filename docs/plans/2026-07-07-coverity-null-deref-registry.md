# Coverity: unchecked NULL from slotcask_registry_get

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-null-deref-registry main`.
- Do the two tasks below **in order** (they're independent but small).
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after both tasks are done: `./build/bin/shard-db-test run-all`.
- Every insertion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

`slotcask_registry_get()` can return `NULL` in a handful of cases (`src/db/slotcask.c`): the caller's `SlotcaskSchemaInfo` is malformed (`splits`/`slot_size`/`streams` <= 0), the fixed-size registry table is full (`SLOTCASK_REG_BUCKETS = 1024` buckets, open-addressed — exhausting it would require ~1024 concurrently-registered distinct objects), or the underlying `slotcask_open()` fails (e.g. the object's on-disk directory was removed from under a live query). Three Coverity findings — CID 1696455, CID 1696443, CID 1696442 — flag two call sites in `src/db/query.c` that use the returned `SlotcaskDb *` unconditionally, without the NULL check that the sibling function `bulk_delete_phase1_indexed` (same file) already has. Full triage context: `docs/coverity-triage-2026-07.md`.

CID 1696443 and CID 1696442 are two Coverity reports of the *same* unchecked use (`keyset_emit_find`) — one fix in Task 2 below resolves both.

**No new dedicated tests are added.** Reliably forcing `slotcask_registry_get` to return NULL from these two call sites would require either filling the 1024-bucket registry with distinct live objects (slow, disproportionate to the fix, and not truly deterministic since it depends on hash distribution across buckets) or racing a `drop-object` against an in-flight query between its earlier successful open and this later call (inherently timing-dependent, not something this test suite has an idiom for). Both fixes mirror an already-proven-correct sibling pattern (`bulk_delete_phase1_indexed`'s existing `if (!sdb) { keyset_free(ks); return 0; }`), so correctness rests on that precedent plus the full suite continuing to pass unchanged (these are pure add-a-check edits with no behavior change on the non-NULL path).

---

## Task 1 — `keyset_count_from_or` unchecked NULL (CID 1696455)

### The bug

`src/db/query.c`'s `keyset_count_from_or`, in its hybrid AND+OR batch-fetch branch, calls `slotcask_registry_get` and immediately uses the result without checking for NULL:

```c
        size_t idx = 0;
        for (size_t b = 0; b < ks->cap; b++)
            if (ks->state[b] == 2)
                memcpy(hashes[idx++], ks->keys[b], 16);

        SlotcaskSchemaInfo sinfo = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

        OrCountCtx cb_ctx;
        memset(&cb_ctx, 0, sizeof(cb_ctx));
        cb_ctx.tree = tree;
        cb_ctx.fs   = fs;
        cb_ctx.dl   = dl;

#define CB_BATCH 1024
        size_t processed = 0;
        while (processed < n_hashes) {
            size_t batch_n = n_hashes - processed;
            if (batch_n > CB_BATCH) batch_n = CB_BATCH;
            slotcask_bulk_resolve_and_fetch(sdb, hashes + processed,
                                             batch_n, &cb_ctx, or_count_batch_cb);
            processed += batch_n;
        }
#undef CB_BATCH

        free(hashes);
        keyset_free(ks);
        return cb_ctx.count;
```

If `sdb` is NULL, `slotcask_bulk_resolve_and_fetch(NULL, ...)` dereferences a NULL `SlotcaskDb *` internally — a crash. The sibling function `bulk_delete_phase1_indexed` (same file) already guards this exact call with `if (!sdb) { keyset_free(ks); return 0; }`.

### The fix

In `src/db/query.c`, find this exact block:

```c
        SlotcaskSchemaInfo sinfo = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

        OrCountCtx cb_ctx;
```

Replace it with:

```c
        SlotcaskSchemaInfo sinfo = {
            .splits = sch->splits, .slot_size = sch->slot_size,
            .streams = sch->streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
        if (!sdb) {
            /* Mirrors bulk_delete_phase1_indexed's existing guard on the
               same call (CID 1696455): a NULL sdb here would otherwise
               reach slotcask_bulk_resolve_and_fetch() unchecked below. */
            free(hashes);
            keyset_free(ks);
            return 0;
        }

        OrCountCtx cb_ctx;
```

### Regression test

None — see Background for rationale. Covered by the full test suite continuing to pass unchanged (this is an additive guard with no effect on the non-NULL path already exercised by every OR-criteria count test, e.g. `test-reindex-spill-collision` and the OR-logic test cases).

---

## Task 2 — `keyset_emit_find` unchecked NULL (CID 1696443, CID 1696442)

### The bug

`src/db/query.c`'s `keyset_emit_find` calls `slotcask_registry_get` and immediately passes the result to `slotcask_bulk_resolve_and_fetch` in a batching loop further down, without checking for NULL:

```c
    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

    /* Batch-fetch results: parallel arrays indexed by hash position. */
    uint8_t **fkeys = calloc(n_fetch, sizeof(*fkeys));
```

This function is the shared emission path for every indexed `find`/`fetch` query whose plan resolves to a `KeySet` (pure-OR unions, primary-intersect results, etc. — see the planner section of `CLAUDE.md`), so it's on a much hotter path than Task 1's count-only sibling. Same failure mode: a NULL `sdb` reaches `slotcask_bulk_resolve_and_fetch` unchecked and crashes.

### The fix

In `src/db/query.c`, find this exact block:

```c
    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);

    /* Batch-fetch results: parallel arrays indexed by hash position. */
    uint8_t **fkeys = calloc(n_fetch, sizeof(*fkeys));
```

Replace it with:

```c
    SlotcaskSchemaInfo sinfo = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &sinfo);
    if (!sdb) {
        /* Same unchecked-NULL bug as bulk_delete_phase1_indexed guards
           against on its analogous call (CID 1696443 / CID 1696442):
           a NULL sdb here would otherwise reach
           slotcask_bulk_resolve_and_fetch() unchecked in the batching
           loop below. hashes / key_to_fetch are the only allocations so
           far in this function. */
        free(hashes);
        free(key_to_fetch);
        return 0;
    }

    /* Batch-fetch results: parallel arrays indexed by hash position. */
    uint8_t **fkeys = calloc(n_fetch, sizeof(*fkeys));
```

### Regression test

None — see Background for rationale. Covered by the full test suite continuing to pass unchanged; `keyset_emit_find` is exercised extensively by every indexed find/fetch test already in the suite (its non-NULL path is unchanged by this fix).

---

## Final verification

After both tasks are complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** showing `# total: N passed, 0 failed`, with N unchanged from the pre-fix baseline (no new test cases were added in this plan). Do not consider this plan done, and do not report success, without pasting this real output.
