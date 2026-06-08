# Plan: MADV_SEQUENTIAL + MADV_NORMAL for kf shards in `slotcask_walk_live_skip`

**Goal**: Add `MADV_SEQUENTIAL` before and `MADV_NORMAL` after the kf-shard slot scan
in `slotcask_walk_live_skip`, mirroring the same hints already present in
`slotcask_walk_one_shard_streaming`.

**Why**: `slotcask_walk_live_skip` is the code path used by the `fetch` command for all
v2 objects. It walks every kf shard sequentially (all `cap` entries in slot order) but
carries no readahead hints. On a cold cache with 256 shards × 6 MB kf each (1.5 GB
total), the kernel issues 4–8 KB I/Os per page fault instead of 128 KB+ streaming
I/Os. This directly lengthens cold-cache `fetch` and `keys` calls. The fix is a 2-line
change per shard that mirrors the pattern already used in the streaming walker
(`slotcask_walk_one_shard_streaming`, added in the MADV_SEQUENTIAL fix that won 10×
cold on sum/avg in PR #33).

**Scope**: kf shard maps only. The segment files accessed during the emit phase are
touched one record at a time in kf-slot order (not sequential); adding MADV_SEQUENTIAL
to them would be counterproductive and is not part of this plan.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-fetch-kf-hints`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add MADV_SEQUENTIAL after kfcache_acquire in `slotcask_walk_live_skip`

**File**: `src/db/slotcask.c`

Locate the anchor inside `slotcask_walk_live_skip` (the per-shard acquire and cap read):
```
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) continue;

        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;
```

Replace with:
```c
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) continue;

        /* Sequential-walk readahead hint: we are about to scan every slot in
           this kf shard from index 0 to cap. MADV_SEQUENTIAL raises kernel
           readahead to 128 KB+ I/Os on a cold cache. Restored to MADV_NORMAL
           after the shard loop so concurrent point lookups on the same cached
           kf file are not burdened with unwanted readahead. Mirrors the same
           hint in slotcask_walk_one_shard_streaming. */
        int kf_seq = (kh.hdr && kh.map_size > 0 &&
                      madvise(kh.hdr, kh.map_size, MADV_SEQUENTIAL) == 0);
        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;
```

---

## Task 2 — Restore MADV_NORMAL and release kf handle at the end of each shard

**File**: `src/db/slotcask.c`

Inside `slotcask_walk_live_skip`, the per-shard kfcache_release is currently bare.
Locate the anchor:
```
        kfcache_release(&kh);
    }
    return 0;
}
```

This is the `kfcache_release` at the end of the outer `for (int s = ...)` loop body,
just before `return 0`. Replace with:
```c
        if (kf_seq) madvise(kh.hdr, kh.map_size, MADV_NORMAL);
        kfcache_release(&kh);
    }
    return 0;
}
```

**Important**: verify the anchor matches exactly one location — the `kfcache_release`
inside `slotcask_walk_live_skip`. This function is roughly 40 lines long; do not
accidentally edit the `kfcache_release` in any other function. If the anchor text
matches more than one site, stop and write `PLAN_NOTES.md`.

---

## Task 3 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.

Pay attention to tests involving `fetch` or `keys` commands.

---

## Invariants and edge cases

- `MADV_SEQUENTIAL` is advisory. `madvise` failure (e.g., very old kernel without
  support) is handled by the `kf_seq` guard — MADV_NORMAL is only called if
  SEQUENTIAL succeeded, preventing a spurious advisory call on an unset hint.
- `kh.hdr` is the base of the full kf mmap (header + entries). `kh.map_size` covers
  the header (24 B) plus all slot entries. Using `kh.hdr` (not `kh.map`) ensures the
  hint covers the header page too — consistent with how the streaming walker applies it.
- The `stop` variable controls the outer loop. If `stop = 1` is set inside the inner
  loop (emit phase), the outer `for (!stop)` condition terminates normally and
  `kfcache_release` is still reached, so MADV_NORMAL is always applied.
- Concurrent point-lookup threads may hold `kfcache_acquire` on the same kf shard
  while the fetch walk is running. `MADV_SEQUENTIAL` is per-process-mapping advice; it
  only affects the readahead for this process's mmap region, not other processes. It
  cannot cause correctness issues for concurrent readers — it is purely a prefetch hint.
