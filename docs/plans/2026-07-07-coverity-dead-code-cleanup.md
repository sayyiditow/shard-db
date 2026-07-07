# Coverity: dead code cleanup (unreachable v1 bodies + dead stores + duplicated block)

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-dead-code-cleanup main`.
- Do the eight tasks below **in order**. They touch five different files and are independent of each other.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all eight tasks are done: `./build/bin/shard-db-test run-all`.
- Every deletion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

This plan removes eight Coverity-flagged pieces of genuinely dead code. None of them change any runtime behavior — every deletion below is code that can never execute. Full triage context: `docs/coverity-triage-2026-07.md`.

**Systemic root cause (Tasks 1–5)**: the v1 → v2 slotcask migration left the old per-command implementation bodies physically in the file, sitting after an unconditional `return` that now always fires (the function now always dispatches to the `_v2` sibling, or otherwise returns before reaching the old body). The compiler treats this as ordinary unreachable code (no warning, since it's not provably unreachable from a diagnostic's perspective without inlining/const-prop) but Coverity's flow analysis flags it correctly. Five CIDs share this one root cause: `rebuild_object` (CID 1696476), `cmd_recount` (CID 1696464), `cmd_vacuum` (CID 1696450), `cmd_bulk_delete_criteria` (CID 1696415), `cmd_fetch` (CID 1696405).

**Task 6** (`cursor_fetch_cb`, CID 1696412) is a different flavor of the same class of bug: a duplicated 5-line block sitting after an unconditional `return 0;` inside the function body (not at the very end of the function) — Coverity additionally flags that the duplicated block reads `row` after the mutex protecting `ctx->results` (the array `row` points into) has already been unlocked, which would be a race *if* it were reachable; since it isn't reachable, deleting it removes both the dead code and that theoretical race in one edit.

**Tasks 7–8** (`recover_scan_tombstones_od` CID 1696480, `seg_scan_o_direct_varlen` CID 1696457) are cosmetic dead-store findings: `carry_len` is assigned a value that is provably never read before being unconditionally overwritten (in both cases, to `0`, three lines later) — removing the dead assignment changes nothing about program behavior.

None of the eight deletions in this plan need new tests: every function's *reachable* behavior is unchanged (the code removed was, by definition, never executed), and the existing test suite already exercises every one of these functions through their real (reachable) code paths — `rebuild_object`/`cmd_recount`/`cmd_vacuum`/`cmd_bulk_delete_criteria`/`cmd_fetch` all have existing maintenance/query test coverage, `cursor_fetch_cb` is exercised by every cursor-based find test, and the two dead-store fixes touch a variable that's reset to the same value a few lines later regardless.

---

## Task 1 — `rebuild_object` unreachable v1 body (CID 1696476)

### The bug

`src/db/query_find.c`'s `rebuild_object` always returns via `rebuild_object_v2` (v1 no longer exists as a live code path), but ~215 lines of the old v1 rebuild implementation remain physically in the function after that `return`:

```c
    /* v2 path runs an entirely separate rebuild over slotcask files. */
    return rebuild_object_v2(db_root, object, &old_sch, old_ts,
                              &new_sch, &new_ts, new_to_old,
                              slot_changed, splits_changed,
                              drop_tombstoned, added_lines, n_added);

    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", obj_dir);
    char new_data_dir[PATH_MAX];
    snprintf(new_data_dir, sizeof(new_data_dir), "%s/data.new", obj_dir);

    /* Clean any stale artifacts from a previous crashed rebuild. */
    rmrf(new_data_dir);
    mkdirp(new_data_dir);

    /* Iterate current shards, copy live records into data.new/. Use the
       persistent ucache for the read side — vacuum holds objlock_wrlock on
       this object so no concurrent ops are racing, but the consistent
       cache path keeps memory footprint shared with whatever ucache
       entries already existed and avoids a second kernel mmap region per
       shard during the rebuild. */
    int live_count = 0;
    for (int olds = 0; olds < old_splits; olds++) {
        char old_path[PATH_MAX];
        build_shard_filename(old_path, sizeof(old_path), data_dir, olds);
        FcacheRead ofc = fcache_get_read(old_path);
        if (!ofc.map) continue;
        uint8_t *omap = ofc.map;
        ShardHeader *oshdr = (ShardHeader *)omap;
        if (oshdr->magic != SHARD_MAGIC || oshdr->slots_per_shard == 0) {
            fcache_release(ofc); continue;
        }
        uint32_t old_slots = oshdr->slots_per_shard;
        if (ofc.size < shard_zoneA_end(old_slots)) { fcache_release(ofc); continue; }
        for (uint32_t s = 0; s < old_slots; s++) {
            SlotHeader *h = (SlotHeader *)(omap + zoneA_off(s));
            if (h->flag != 1) continue;  /* skip empty and tombstoned */

            const uint8_t *okey = omap + zoneB_off(s, old_slots, old_sch.slot_size);
            const uint8_t *oval = okey + h->key_len;

            int new_shard = (((unsigned)h->hash[0] << 8) | h->hash[1]) % new_splits;
            uint32_t start_slot_raw = ((uint32_t)h->hash[2] << 24) | ((uint32_t)h->hash[3] << 16)
                                    | ((uint32_t)h->hash[4] << 8)  |  (uint32_t)h->hash[5];

            char npath[PATH_MAX];
            build_shard_filename(npath, sizeof(npath), new_data_dir, new_shard);

            FcacheRead wh = ucache_get_write(npath, new_sch.slot_size);
            if (!wh.map) continue;
            uint8_t *map = wh.map;
            uint32_t new_slots = wh.slots_per_shard;
            uint32_t new_mask = new_slots - 1;

            int slot = -1;
            for (uint32_t i = 0; i < new_slots; i++) {
                uint32_t probe = (start_slot_raw + i) & new_mask;
                SlotHeader *nh = (SlotHeader *)(map + zoneA_off(probe));
                if (nh->flag == 0 && nh->key_len == 0) { slot = (int)probe; break; }
            }
            if (slot < 0) {
                /* Need to grow this new shard — release, grow, retry. */
                ucache_write_release(wh);
                if (ucache_grow_shard(npath, new_sch.slot_size) > 0) {
                    wh = ucache_get_write(npath, new_sch.slot_size);
                    if (!wh.map) continue;
                    map = wh.map;
                    new_slots = wh.slots_per_shard;
                    new_mask = new_slots - 1;
                    for (uint32_t i = 0; i < new_slots; i++) {
                        uint32_t probe = (start_slot_raw + i) & new_mask;
                        SlotHeader *nh = (SlotHeader *)(map + zoneA_off(probe));
                        if (nh->flag == 0 && nh->key_len == 0) { slot = (int)probe; break; }
                    }
                    if (slot < 0) {
                        ucache_write_release(wh);
                        LOG_ERROR(LOG_SUB_CONFIG, "REBUILD %s/%s: no free slot in new shard %d after grow", db_root, object, new_shard);
                        continue;
                    }
                } else {
                    LOG_ERROR(LOG_SUB_CONFIG, "REBUILD %s/%s: grow failed for new shard %d", db_root, object, new_shard);
                    continue;
                }
            }

            SlotHeader *nh = (SlotHeader *)(map + zoneA_off(slot));
            memset(nh, 0, HEADER_SIZE);
            memcpy(nh->hash, h->hash, 16);
            nh->key_len = h->key_len;
            uint8_t *npay = map + zoneB_off(slot, new_slots, new_sch.slot_size);
            memcpy(npay, okey, h->key_len);

            uint8_t *nval = npay + h->key_len;
            if (!slot_changed) {
                memcpy(nval, oval, h->value_len);
                nh->value_len = h->value_len;
            } else {
                /* Slot layout changed (compact and/or added fields):
                   copy each kept active field at its new offset; added
                   fields (new_to_old==-1) stay zero from the memset above. */
                for (int k = 0; k < new_ts.nfields; k++) {
                    int oi = new_to_old[k];
                    if (oi < 0) continue; /* newly-added field, zero */
                    memcpy(nval + new_ts.fields[k].offset,
                           oval + old_ts->fields[oi].offset,
                           old_ts->fields[oi].size);
                }
                nh->value_len = new_ts.total_size;
            }
            nh->flag = 1;
            ucache_bump_record_count(wh.slot, 1);
            ucache_write_release(wh);
            live_count++;
        }
        fcache_release(ofc);
    }

    /* Stage fields.conf.new if compacting (drop tombstoned lines). */
    char fpath[PATH_MAX], fpath_new[PATH_MAX], fpath_old[PATH_MAX];
    snprintf(fpath,     sizeof(fpath),     "%s/fields.conf", obj_dir);
    snprintf(fpath_new, sizeof(fpath_new), "%s/fields.conf.new", obj_dir);
    snprintf(fpath_old, sizeof(fpath_old), "%s/fields.conf.old", obj_dir);

    int fields_changed = drop_tombstoned || n_added > 0;
    if (fields_changed) {
        FILE *fin = fopen(fpath, "r");
        FILE *fout = fopen(fpath_new, "w");
        if (!fin || !fout) {
            if (fin) fclose(fin);
            if (fout) fclose(fout);
            rmrf(new_data_dir);
            OUT("{\"error\":\"Failed to stage fields.conf.new\"}\n");
            return 1;
        }
        char line[512];
        while (fgets(line, sizeof(line), fin)) {
            char stripped[512];
            strncpy(stripped, line, sizeof(stripped) - 1);
            stripped[sizeof(stripped) - 1] = '\0';
            stripped[strcspn(stripped, "\n")] = '\0';
            if (stripped[0] == '\0' || stripped[0] == '#') { fputs(line, fout); continue; }
            if (drop_tombstoned && strstr(stripped, ":removed")) continue;
            fputs(line, fout);
        }
        /* Append new fields */
        for (int a = 0; a < n_added; a++) {
            fprintf(fout, "%s\n", added_lines[a]);
        }
        fclose(fin);
        fclose(fout);
    }

    /* ===== Atomic swap window ===== */
    fcache_invalidate(data_dir);

    char data_old[PATH_MAX];
    snprintf(data_old, sizeof(data_old), "%s/data.old", obj_dir);
    rmrf(data_old);

    if (rename(data_dir, data_old) != 0) {
        rmrf(new_data_dir);
        if (fields_changed) unlink(fpath_new);
        OUT("{\"error\":\"Failed to rename data → data.old\"}\n");
        return 1;
    }
    if (rename(new_data_dir, data_dir) != 0) {
        /* Best-effort rollback — if this also fails the operator must
           manually restore data_old → data_dir; either way we're already
           returning an error so just log the secondary failure. */
        if (rename(data_old, data_dir) != 0)
            LOG_ERROR(LOG_SUB_VACUUM, "vacuum: rollback rename(%s → %s) failed: %s",
                    data_old, data_dir, strerror(errno));
        if (fields_changed) unlink(fpath_new);
        OUT("{\"error\":\"Failed to rename data.new → data\"}\n");
        return 1;
    }

    if (fields_changed) {
        if (rename(fpath, fpath_old) != 0)
            LOG_ERROR(LOG_SUB_VACUUM, "vacuum: rename(%s → %s) failed: %s",
                    fpath, fpath_old, strerror(errno));
        if (rename(fpath_new, fpath) != 0) {
            LOG_ERROR(LOG_SUB_VACUUM, "vacuum: rename(%s → %s) failed: %s — restoring old fields.conf",
                    fpath_new, fpath, strerror(errno));
            /* Best-effort restore of the previous fields.conf from .old. */
            (void)rename(fpath_old, fpath);
        }
    }

    if (splits_changed) {
        update_schema_conf_splits(db_root, object, new_splits);
    }

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);
    reset_deleted_count(db_root, object);
    set_count(db_root, object, live_count);

    /* Async-ish cleanup of .old artifacts */
    rmrf(data_old);
    if (fields_changed) unlink(fpath_old);

    /* Per-shard idx layout uses index_splits_for(splits) (see types.h).
       Changing splits changes the idx-shard count, so the on-disk idx
       files no longer match the routing math: writes go to the new shard
       count, reads fan out across the new count, and any old high-numbered
       shard files are both unreachable AND poisonous (the old layout
       stored all entries
       across the wider hash range, so dropping them leaves stale rows
       indexed and missing rows unindexed). Rebuild every index from the
       data shards atomically with the splits change. Compact-only changes
       slot_size but not the hash routing, so idx layout stays valid. */
    int idx_rebuilt = 0;
    if (splits_changed) idx_rebuilt = reindex_object(db_root, object, 0);

    LOG_AUDIT(LOG_SUB_CONFIG, "REBUILD %s/%s: live=%d, splits=%d→%d, slot_size=%d→%d, compact=%d, idx_rebuilt=%d",
            db_root, object, live_count, old_splits, new_splits,
            old_sch.slot_size, new_sch.slot_size, drop_tombstoned, idx_rebuilt);
    OUT("{\"status\":\"rebuilt\",\"live\":%d,\"splits\":%d,\"slot_size\":%d,\"compact\":%s,\"indexes_rebuilt\":%d}\n",
        live_count, new_splits, new_sch.slot_size, drop_tombstoned ? "true" : "false",
        idx_rebuilt);
    return 0;
}

int is_number(const char *s) {
```

### The fix

In `src/db/query_find.c`, replace the entire block quoted above (from `/* v2 path runs an entirely separate rebuild over slotcask files. */` through the line immediately before `int is_number(const char *s) {`) with:

```c
    /* v2 path runs an entirely separate rebuild over slotcask files. */
    return rebuild_object_v2(db_root, object, &old_sch, old_ts,
                              &new_sch, &new_ts, new_to_old,
                              slot_changed, splits_changed,
                              drop_tombstoned, added_lines, n_added);
}

int is_number(const char *s) {
```

### Regression test

None — see Background. `rebuild_object`'s only reachable behavior (the `return rebuild_object_v2(...)` call) is unchanged and already covered by every existing add-field/vacuum-compact/vacuum-splits test.

---

## Task 2 — `cmd_recount` unreachable v1 body (CID 1696464)

### The bug

`src/db/query_maint.c`'s `cmd_recount` always returns after the v2 kf-header-sum path, but the old v1 shard-scan implementation remains after that `return`:

```c
    if (sdb) slotcask_sum_kf_totals(sdb, &total_hdr, &deleted_hdr);
    int live = (int)(total_hdr > deleted_hdr ? total_hdr - deleted_hdr : 0);
    OUT("{\"count\":%d}\n", live);
    return 0;

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Collect shard paths */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));
    if (!paths) { OUT("{\"error\":\"oom\"}\n"); return 1; }

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(paths); OUT("{\"count\":0}\n"); set_count(db_root, object, 0); return 0; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (path_count >= path_cap) {
            path_cap *= 2;
            /* Plain realloc + walk: paths[] holds strdup'd entries that
               xrealloc_or_free's atomic free would orphan. */
            char **t = realloc(paths, path_cap * sizeof(char *));
            if (!t) {
                for (int k = 0; k < path_count; k++) free(paths[k]);
                free(paths);
                paths = NULL;
                path_count = 0;
                break;
            }
            paths = t;
        }
        char bp[PATH_MAX];
        snprintf(bp, sizeof(bp), "%s/%s", data_dir, e1->d_name);
        paths[path_count++] = strdup(bp);
    }
    closedir(d1);

    int total = 0;
    if (path_count > 0) {
        RecountWorkerArg *args = malloc(path_count * sizeof(RecountWorkerArg));
        if (!args) {
            for (int i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            OUT("{\"error\":\"oom\"}\n");
            return 1;
        }
        for (int i = 0; i < path_count; i++)
            args[i] = (RecountWorkerArg){ paths[i], sch.slot_size, &total };
        parallel_for_io(recount_worker, args, path_count, sizeof(RecountWorkerArg));
        free(args);
    }

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);

    set_count(db_root, object, total);
    OUT("{\"count\":%d}\n", total);
    return 0;
}

/* ========== TRUNCATE ========== */
```

### The fix

In `src/db/query_maint.c`, replace the block quoted above with:

```c
    if (sdb) slotcask_sum_kf_totals(sdb, &total_hdr, &deleted_hdr);
    int live = (int)(total_hdr > deleted_hdr ? total_hdr - deleted_hdr : 0);
    OUT("{\"count\":%d}\n", live);
    return 0;
}

/* ========== TRUNCATE ========== */
```

### Regression test

None — see Background. `cmd_recount`'s only reachable behavior is unchanged and already covered by the existing recount test cases.

---

## Task 3 — `cmd_vacuum` unreachable v1 body (CID 1696450)

### The bug

`src/db/query_maint.c`'s `cmd_vacuum` light path always returns after the v2 compact-segs/compact-kf calls, but the old v1 per-shard vacuum-worker implementation remains after that `return`:

```c
    reset_deleted_count(db_root, object);  /* v1 only; no-op for v2 */
    OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", dropped);
    return 0;
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Collect all shard paths */
    VacuumWork *shards = NULL;
    int shard_count = 0, shard_cap = 256;
    shards = malloc(shard_cap * sizeof(VacuumWork));
    if (!shards) { fprintf(stderr, "Error: oom\n"); return 1; }

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(shards); fprintf(stderr, "Error: No data directory for [%s]\n", object); return 1; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (shard_count >= shard_cap) {
            shard_cap *= 2;
            VacuumWork *t = xrealloc_or_free(shards, shard_cap * sizeof(*t));
            if (!t) {
                /* xrealloc_or_free already freed shards; reset count so the
                   parallel_for + cleaned-sum below don't dereference NULL. */
                shards = NULL;
                shard_count = 0;
                break;
            }
            shards = t;
        }
        snprintf(shards[shard_count].path, PATH_MAX, "%s/%s", data_dir, e1->d_name);
        shards[shard_count].slot_size = sch.slot_size;
        shards[shard_count].cleaned = 0;
        shard_count++;
    }
    closedir(d1);

    /* Parallel vacuum across all shards */
    parallel_for(vacuum_worker, shards, shard_count, sizeof(VacuumWork));

    int cleaned = 0;
    for (int i = 0; i < shard_count; i++)
        cleaned += shards[i].cleaned;
    free(shards);

    reset_deleted_count(db_root, object);
    OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", cleaned);
    return 0;
}
/* ========== SEQUENCES ========== */
```

### The fix

In `src/db/query_maint.c`, replace the block quoted above with:

```c
    reset_deleted_count(db_root, object);  /* v1 only; no-op for v2 */
    OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", dropped);
    return 0;
}
/* ========== SEQUENCES ========== */
```

### Regression test

None — see Background. `cmd_vacuum`'s only reachable behavior (both the heavy `rebuild_object` path and the light compact-segs/compact-kf path above this dead block) is unchanged and already covered by the existing vacuum test cases.

---

## Task 4 — `cmd_bulk_delete_criteria` unreachable legacy per-key loop (CID 1696415)

### The bug

`src/db/query_bulk.c`'s `cmd_bulk_delete_criteria` always returns after the v2 bucketed-worker path, but the old per-key delete loop remains after that `return`:

```c
    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;

    for (int i = 0; i < matched; i++) {
        const char *key = ctx.keys[i];
        size_t klen = strlen(key);
        uint8_t hash[16]; int shard_id, start_slot;
        compute_hash_raw(key, klen, hash);
        shard_id = compute_record_shard(hash, sch.splits);
        start_slot = 0;

        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

        FcacheRead wh = ucache_get_write(shard, 0);
        if (!wh.map) { skipped++; continue; }
        uint8_t *map = wh.map;
        uint32_t slots = wh.slots_per_shard;
        uint32_t mask = slots - 1;

        /* Find the record */
        int slot = -1;
        for (uint32_t si = 0; si < slots; si++) {
            uint32_t s = ((uint32_t)start_slot + si) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
                h->key_len == klen &&
                memcmp(map + zoneB_off(s, slots, sch.slot_size), key, klen) == 0) {
                slot = (int)s; break;
            }
        }

        if (slot < 0) { ucache_write_release(wh); skipped++; continue; }

        SlotHeader *h = (SlotHeader *)(map + zoneA_off(slot));
        uint8_t *value_ptr = map + zoneB_off(slot, slots, sch.slot_size) + h->key_len;

        /* Re-check criteria tree under wrlock (AND/OR supported) */
        if (!criteria_match_tree(value_ptr, tree, &fs)) {
            ucache_write_release(wh); skipped++; continue;
        }

        /* Per-record CAS — same optimistic-concurrency check single-op
           cmd_delete uses. Failures count as skipped, not errors. */
        if (cas_crit && cas_ncrit > 0 &&
            !cas_check(ts, value_ptr, (int)h->value_len, cas_crit, cas_ncrit)) {
            ucache_write_release(wh); skipped++; continue;
        }

        /* Extract indexed field values (as index-key bytes) BEFORE tombstoning */
        char idx_fields[MAX_FIELDS][256];
        int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
        for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
        uint8_t *idx_bufs[MAX_FIELDS];
        size_t   idx_lens[MAX_FIELDS];
        int      idx_have[MAX_FIELDS];
        memset(idx_bufs, 0, sizeof(idx_bufs));
        memset(idx_lens, 0, sizeof(idx_lens));
        memset(idx_have, 0, sizeof(idx_have));
        if (nidx > 0) {
            for (int fi = 0; fi < nidx; fi++) {
                idx_have[fi] = build_index_key_from_record(ts, value_ptr, idx_fields[fi],
                                                          &idx_bufs[fi], &idx_lens[fi]);
            }
        }

        /* Tombstone */
        h->flag = 2;
        ucache_bump_record_count(wh.slot, -1);
        ucache_write_release(wh);

        /* Index cleanup */
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_have[fi])
                delete_index_entry(db_root, object, idx_fields[fi], sch.splits,
                                   idx_bufs[fi], idx_lens[fi], hash);
            free(idx_bufs[fi]);
        }

        deleted++;
    }

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }

    LOG_INFO(LOG_SUB_QUERY, "BULK-DELETE %s matched=%d deleted=%d skipped=%d", object, matched, deleted, skipped);
    OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n", matched, deleted, skipped);

    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}

/* ========== VACUUM ========== */
```

### The fix

In `src/db/query_bulk.c`, replace the block quoted above with:

```c
    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}

/* ========== VACUUM ========== */
```

**Note on anchor uniqueness**: `for (int i = 0; i < matched; i++) {` alone occurs multiple times in this file (bucket-sort loops earlier in the same function), so match on the full quoted block above — the preceding `if (cas_crit) free_criteria(cas_crit, cas_ncrit);` / `free(ctx.keys); free_criteria_tree(tree); return 0;` sequence immediately followed by the blank line and `for (int i = 0; i < matched; i++) {` occurs exactly once (immediately after the v2 success-path response emission).

### Regression test

None — see Background. `cmd_bulk_delete_criteria`'s only reachable behavior (the v2 bucketed-worker path above this dead block, and the three earlier early-return paths for timeout/budget/dry-run/no-match) is unchanged and already covered by the existing bulk-delete-criteria test cases.

---

## Task 5 — `cmd_fetch` unreachable v1 body (CID 1696405)

### The bug

`src/db/query_find.c`'s `cmd_fetch` always returns via `cmd_fetch_v2`, but the old v1 shard-scan/cursor implementation remains after that `return`:

```c
    Schema sch = load_schema(db_root, object);
    return cmd_fetch_v2(db_root, object, offset, limit, proj_str, cursor,
                         format, delimiter, &sch, want_total);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    FieldSchema fs_fetch;
    init_field_schema(&fs_fetch, db_root, object);

    /* Parse projection fields */
    const char *proj_fields[MAX_FIELDS];
    char proj_buf[MAX_LINE];
    int proj_count = 0;
    if (proj_str && proj_str[0]) {
        snprintf(proj_buf, sizeof(proj_buf), "%s", proj_str);
        char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
        while (tok && proj_count < MAX_FIELDS) {
            proj_fields[proj_count++] = tok;
            tok = strtok_r(NULL, ",", &_tok_save);
        }
    }

    /* Collect shard paths (sorted for deterministic order) */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));
    if (!paths) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    DIR *d1 = opendir(data_dir);
    if (d1) {
        struct dirent *e1;
        while ((e1 = readdir(d1))) {
            if (e1->d_name[0] == '.') continue;
            size_t nlen = strlen(e1->d_name);
            if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
            if (path_count >= path_cap) {
                path_cap *= 2;
                /* Plain realloc (not xrealloc_or_free) so we can walk paths[]
                   to free already-strdup'd entries before releasing the array. */
                char **t = realloc(paths, path_cap * sizeof(char *));
                if (!t) {
                    /* OOM: free strdup'd entries + the array, zero path_count
                       so downstream loops (the scan at line 4191 and the
                       cleanup at 4235) skip without dereferencing NULL paths. */
                    for (int k = 0; k < path_count; k++) free(paths[k]);
                    free(paths);
                    paths = NULL;
                    path_count = 0;
                    break;
                }
                paths = t;
            }
            char bp[PATH_MAX];
            snprintf(bp, sizeof(bp), "%s/%s", data_dir, e1->d_name);
            paths[path_count++] = strdup(bp);
        }
        closedir(d1);
    }

    /* Parse cursor: "path_idx:slot_idx" */
    int start_path = 0;
    size_t start_slot = 0;
    if (cursor && cursor[0]) {
        sscanf(cursor, "%d:%zu", &start_path, &start_slot);
        start_slot++; /* resume AFTER the last returned slot */
    }

    int printed = 0;
    int next_path = -1;
    size_t next_slot = 0;

    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;

    if (csv_delim)
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count, fs_ptr, csv_delim);
    else if (rows_fmt)
        emit_rows_columns(proj_fields, proj_count, fs_ptr);
    else if (dict_fmt)
        OUT("{\"results\":{");
    else
        OUT("{\"results\":[");

    /* paths is NULL only when path_count was reset to 0 alongside it
       (initial-malloc fail returns early; realloc fail also zeroes
       path_count). The loop condition `pi < path_count` short-circuits
       when path_count == 0, so paths[pi] is never reached with NULL paths.
       Coverity can't trace the paired invariant — suppress.
       UNINIT for the same reason: paths[pi] is only read when pi < path_count,
       and path_count is incremented only after a successful strdup writes
       paths[path_count]. So paths[0..path_count-1] are always initialized. */
    for (int pi = start_path; pi < path_count && printed < limit; pi++) {
        /* coverity[forward_null] coverity[uninit_use_in_call] paths non-NULL and paths[pi] initialized when pi < path_count */
        FcacheRead fc = fcache_get_read(paths[pi]);
        if (!fc.map) continue;
        uint32_t shard_slots = fc.slots_per_shard;
        if (fc.size < shard_zoneA_end(shard_slots)) { fcache_release(fc); continue; }
        size_t slot_start = (pi == start_path) ? start_slot : 0;

        for (size_t si = slot_start; si < shard_slots && printed < limit; si++) {
            const SlotHeader *hdr = (const SlotHeader *)(fc.map + zoneA_off(si));
            if (hdr->flag != 1) continue;

            if (!cursor || !cursor[0]) {
                offset--;
                if (offset >= 0) continue;
            }

            const uint8_t *block = fc.map + zoneB_off(si, shard_slots, sch.slot_size);
            if (csv_delim) {
                char kbuf[1024];
                size_t kl = hdr->key_len < sizeof(kbuf) - 1 ? hdr->key_len : sizeof(kbuf) - 1;
                memcpy(kbuf, block, kl); kbuf[kl] = '\0';
                csv_emit_row(kbuf, block + hdr->key_len, hdr->value_len,
                             proj_count > 0 ? proj_fields : NULL,
                             proj_count, fs_ptr, csv_delim);
                printed++;
            } else if (rows_fmt)
                print_record_row(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
            else if (dict_fmt)
                print_record_dict(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
            else
                print_record_json(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
            next_path = pi;
            next_slot = si;
        }
        fcache_release(fc);
    }

    /* Build next cursor — CSV mode omits cursor (streaming export). */
    if (csv_delim) {
        /* Nothing more to append; body already ends with \n per row. */
    } else {
        const char *close = dict_fmt ? "}" : "]";
        if (printed >= limit && next_path >= 0)
            OUT("%s,\"cursor\":\"%d:%zu\"}\n", close, next_path, next_slot);
        else
            OUT("%s,\"cursor\":null}\n", close);
    }

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
    return 0;
}

/* ========== EXCLUDED KEYS HELPER ========== */
```

### The fix

In `src/db/query_find.c`, replace the block quoted above with:

```c
    Schema sch = load_schema(db_root, object);
    return cmd_fetch_v2(db_root, object, offset, limit, proj_str, cursor,
                         format, delimiter, &sch, want_total);
}

/* ========== EXCLUDED KEYS HELPER ========== */
```

### Regression test

None — see Background. `cmd_fetch`'s only reachable behavior (the `return cmd_fetch_v2(...)` call, and everything above it in the function computing `rows_fmt`/`dict_fmt`/`csv_delim`/`limit`) is unchanged and already covered by the existing fetch/cursor test cases.

---

## Task 6 — `cursor_fetch_cb` duplicated block after unconditional return (CID 1696412)

### The bug

`src/db/query.c`'s `cursor_fetch_cb` has a 5-line block duplicated verbatim right after the function's final `return 0;` (which always fires — every code path above it either returns early or falls through to this `return 0;`):

```c
    SmallPrefilterRow *row = &ctx->results[ctx->result_count++];
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    pthread_mutex_unlock(&ctx->lock);
    return 0;
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    return 0;
}
```

If this duplicated block were ever reachable, it would also be a data race: it reads/writes `row` (a pointer into `ctx->results`, the array the mutex protects) *after* `pthread_mutex_unlock(&ctx->lock)` has already released that protection.

### The fix

In `src/db/query.c`, find this exact block:

```c
    SmallPrefilterRow *row = &ctx->results[ctx->result_count++];
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    pthread_mutex_unlock(&ctx->lock);
    return 0;
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    return 0;
}
```

Replace it with:

```c
    SmallPrefilterRow *row = &ctx->results[ctx->result_count++];
    memcpy(row->hash, hash, 16);
    typed_field_to_index_key(ctx->ts, (const uint8_t *)value,
                             ctx->order_field_idx,
                             row->sort_key, &row->sort_key_len);
    pthread_mutex_unlock(&ctx->lock);
    return 0;
}
```

### Regression test

None — see Background. `cursor_fetch_cb`'s reachable behavior (everything before the first `return 0;`) is unchanged and already exercised by every cursor-based find test.

---

## Task 7 — `recover_scan_tombstones_od` dead store on `carry_len` (CID 1696480)

### The bug

`src/db/slotcask.c`'s `recover_scan_tombstones_od`, in its varlen carry-buffer reassembly path, assigns `carry_len = (int)rec_size;` and then never reads that value before unconditionally overwriting it to `0` three lines later:

```c
                    memcpy(carry + carry_len, buf, (size_t)need2);
                    pos += (size_t)need2; carry_len = (int)rec_size;
                }
                if (carry[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       carry_off, (uint32_t)rec_size, db->slot_size);
                carry_len = 0;
```

Between the assignment and the overwrite, only `carry[18]` (the flag byte) and `rec_size` (a separate local) are read — `carry_len` itself is never consulted, so the assignment is provably dead.

### The fix

In `src/db/slotcask.c`, find this exact line:

```c
                    pos += (size_t)need2; carry_len = (int)rec_size;
```

Replace it with:

```c
                    pos += (size_t)need2;
```

### Regression test

None — see Background. This is a pure dead-store removal; `carry_len`'s value is unconditionally set to `0` a few lines below in every code path that reaches this point, so behavior is identical. Covered by the full test suite continuing to pass unchanged (crash-recovery / SIGKILL-recovery tests already exercise this varlen tombstone-scan path).

---

## Task 8 — `seg_scan_o_direct_varlen` dead store on `carry_len` (CID 1696457)

### The bug

`src/db/io_direct.c`'s `seg_scan_o_direct_varlen` has the identical pattern to Task 7, in its own carry-buffer reassembly path: `carry_len = (int)rec_size;` is assigned and never read before being unconditionally overwritten to `0` a few lines later:

```c
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
                carry_len = (int)rec_size;
            }

            if (flag == 1) {
                if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            carry_len = 0;
```

Between the assignment and the overwrite, only `flag`/`vlen`/`carry` (the buffer contents, not `carry_len`) are read.

### The fix

In `src/db/io_direct.c`, find this exact block:

```c
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
                carry_len = (int)rec_size;
            }
```

Replace it with:

```c
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
            }
```

### Regression test

None — see Background. Same rationale as Task 7: pure dead-store removal, `carry_len` is unconditionally reset to `0` a few lines below regardless. Covered by the full test suite continuing to pass unchanged (the O_DIRECT varlen segment-scan path is already exercised by existing reindex/crash-recovery tests).

---

## Final verification

After all eight tasks are complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** showing `# total: N passed, 0 failed`, with N unchanged from the pre-fix baseline (no new test cases were added in this plan). Do not consider this plan done, and do not report success, without pasting this real output.
