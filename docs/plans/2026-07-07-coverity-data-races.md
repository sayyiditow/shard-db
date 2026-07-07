# Coverity: unlocked write to shared stream state in recover_one_stream

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-data-races main`.
- There is one task below — a single two-line change guarded by a lock/unlock pair.
- Build with `SKIP_TESTS=1 ./build.sh` to catch compile errors early, then run the full suite: `./build/bin/shard-db-test run-all`.
- The insertion below is anchored on **quoted exact text** from the current source. If the quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

`src/db/slotcask.c`'s `recover_one_stream` finishes walking a stream's segments by writing `db->streams[sid].active_file_id` and `db->streams[sid].reserve_off` directly, without holding that stream's `rotation_lock`:

```c
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
```

Every other writer of these two fields in this file *does* hold `rotation_lock` while writing them — e.g. the equivalent update in `slotcask_migrate_to_varlen`:

```c
        pthread_mutex_lock(&db->streams[s].rotation_lock);
        db->streams[s].active_file_id = dest_fid[s];
        db->streams[s].reserve_off    = dest_off[s];
        pthread_mutex_unlock(&db->streams[s].rotation_lock);
```

Coverity flagged this as two findings against the same unlocked write — CID 1696416 (`active_file_id`) and CID 1696410 (`reserve_off`) — since both fields are written together without the lock the rest of the codebase treats as required for this pair. `rotation_lock` for every stream is already initialized (via `pthread_mutex_init`) earlier in `slotcask_open`, well before `recover_streams`/`recover_one_stream` run, so taking it here is always safe — no ordering hazard.

`recover_one_stream` runs once per stream during `slotcask_open`'s startup recovery, dispatched in parallel across streams via `parallel_for_io` (see `recover_stream_worker` / `recover_streams`, same file). Each worker thread only ever touches its own `sid`'s stream struct, so in practice there's no *concurrent* writer racing this specific write today — but the missing lock is still a correctness bug relative to this codebase's own invariant (every writer of `active_file_id`/`reserve_off` holds `rotation_lock`), and matters the moment any future code path reads or writes these fields concurrently with recovery (e.g. a read-only status/stats query touching stream state before recovery for a given stream completes). Taking the lock here costs nothing (no contention exists at startup) and brings the function in line with the invariant the rest of the file already enforces.

---

## Task 1 — `recover_one_stream` writes `active_file_id`/`reserve_off` without `rotation_lock` (CID 1696416, CID 1696410)

### The bug

In `src/db/slotcask.c`, `recover_one_stream`'s final two statements before `free(ids); return 0;`:

```c
        last_offset = pos;
        segcache_release(&h);
    }
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
    free(ids);
    return 0;
}
```

### The fix

In `src/db/slotcask.c`, find this exact block:

```c
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
    free(ids);
    return 0;
}
```

Replace it with:

```c
    /* Same invariant as the equivalent update in slotcask_migrate_to_varlen:
       every writer of active_file_id/reserve_off holds rotation_lock
       (CID 1696416, CID 1696410). rotation_lock for this stream was already
       pthread_mutex_init'd earlier in slotcask_open, so this is always safe
       to take here. */
    pthread_mutex_lock(&db->streams[sid].rotation_lock);
    db->streams[sid].active_file_id = (uint32_t)last_id;
    db->streams[sid].reserve_off = (uint64_t)last_offset;
    pthread_mutex_unlock(&db->streams[sid].rotation_lock);
    free(ids);
    return 0;
}
```

**Note on anchor uniqueness**: `db->streams[sid].active_file_id = (uint32_t)last_id;` followed immediately by `db->streams[sid].reserve_off = (uint64_t)last_offset;` occurs exactly once in this file (the `slotcask_migrate_to_varlen` sibling uses `db->streams[s]` with a different variable name and different right-hand sides — `dest_fid[s]`/`dest_off[s]` — so it will not match this anchor).

### Regression test

None. This is a lock/unlock addition around an already-correct value computation, in a function whose output (`active_file_id`, `reserve_off`) is already exercised by every crash-recovery test in the suite (e.g. the SIGKILL recovery tests under `src/test/cases/`) — those tests assert the recovered stream state is correct, and this fix doesn't change what value is written, only serializes the write against `rotation_lock`. There is no practical way to construct a deterministic test that proves a *data race* was fixed (that would require a concurrent second writer to `active_file_id`/`reserve_off` during recovery, which no code path currently does — see Background). Covered by the full test suite continuing to pass unchanged.

---

## Final verification

After the task is complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** showing `# total: N passed, 0 failed`, with N unchanged from the pre-fix baseline (no new test cases were added in this plan). Do not consider this plan done, and do not report success, without pasting this real output.
