# Plan: MADV_SEQUENTIAL hints for light-vacuum compaction

**Goal**: Add `MADV_SEQUENTIAL` readahead hints to the two full-sequential-scan paths
inside the light-vacuum code path: (1) the kf-shard Phase B scan inside
`kfcache_resplit_locked`, and (2) the donor/recipient segment scans inside
`compact_migrate_records`.

**Why**: The light vacuum (`cmd_vacuum` without `--compact`/`--splits`) runs two
operations that read large files end-to-end but carry no readahead hints today:

- `kfcache_resplit_locked` Phase B: reads the entire old kf shard sequentially
  (`for i in 0..old_cap`) to copy live entries into the new file. No MADV_SEQUENTIAL.
  On a cold cache with a 300 MB kf shard, the kernel issues 4–8 KB I/Os instead of
  128 KB+ I/Os, adding seconds to kf-compact time.

- `compact_migrate_records`: reads all slots in the donor segment sequentially (to
  build a free-offset list for the recipient, then to migrate live records). Both donor
  and recipient are accessed sequentially via mmap, also with no hints.

**Scope**: Only `MADV_SEQUENTIAL` (and `MADV_NORMAL` to restore on exit) — no
`MADV_DONTNEED`. The light-vacuum path does NOT hold `objlock_wrlock` (only the
per-kf-shard wrlock for kf operations), so DONTNEED on segment pages is not safe
(concurrent readers may hold segcache handles on the same segments).

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-compact-hints`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — MADV_SEQUENTIAL around Phase B in `kfcache_resplit_locked`

**File**: `src/db/slotcask.c`

`kfcache_resplit_locked` Phase B reads `kh->map[i]` for every slot in the old kf shard.
Add a sequential hint before Phase B and restore after.

Locate the anchor (the Phase B comment and loop header):
```
    /* === Phase B: probe-rebuild loop === */
    uint64_t live_copied = 0;
    for (size_t i = 0; i < old_cap; i++) {
```

Replace with:
```c
    /* === Phase B: probe-rebuild loop === */
    /* Sequential hint: Phase B reads the entire old kf map from slot 0 to
       old_cap. MADV_SEQUENTIAL switches kernel readahead to 128 KB+ I/Os
       on cold cache, cutting this scan from seconds to sub-second on large
       shards. Restored to MADV_NORMAL after Phase B so subsequent point-
       lookup patterns on the (now-remapped) new kf are not over-read. */
    int kf_seq_set = (kh->hdr && kh->map_size > 0 &&
                      madvise(kh->hdr, kh->map_size, MADV_SEQUENTIAL) == 0);
    uint64_t live_copied = 0;
    for (size_t i = 0; i < old_cap; i++) {
```

Then locate the anchor immediately after the Phase B loop closes (the line that captures
the timestamp after the rebuild):
```
    uint64_t t_after_rebuild = kf_now_us();
```

Insert immediately before that line:
```c
    if (kf_seq_set) madvise(kh->hdr, kh->map_size, MADV_NORMAL);
```

Result around the timestamp:
```c
    if (kf_seq_set) madvise(kh->hdr, kh->map_size, MADV_NORMAL);
    uint64_t t_after_rebuild = kf_now_us();
```

**Edge case**: After Phase E, `kh->hdr` is remapped to the new file. The `madvise` at
the start of Phase B acts on the old map, which is correct — Phase B reads the old map.
After Phase B, the MADV_NORMAL restores the old map's advice before Phase C (msync). In
Phase E the old map is munmap'd and replaced; the new map starts with no hints, which is
correct for the ongoing kfcache use.

---

## Task 2 — MADV_SEQUENTIAL on donor and recipient in `compact_migrate_records`

**File**: `src/db/slotcask.c`

`compact_migrate_records` acquires both segment handles then performs two sequential
scans: one over the recipient (build free-offset list) and one over the donor (migrate
live records). Add MADV_SEQUENTIAL after acquiring each handle, and MADV_NORMAL before
releasing each.

**Step 2a — add hints after successful acquire of both handles**

Locate the anchor (immediately after both acquire calls succeed):
```
    int slot_size = db->slot_size;
    size_t total = dh.map_size / (size_t)slot_size;
```

Insert immediately before that anchor:
```c
    madvise(dh.map, dh.map_size, MADV_SEQUENTIAL);
    madvise(rh.map, rh.map_size, MADV_SEQUENTIAL);
```

Result:
```c
    madvise(dh.map, dh.map_size, MADV_SEQUENTIAL);
    madvise(rh.map, rh.map_size, MADV_SEQUENTIAL);
    int slot_size = db->slot_size;
    size_t total = dh.map_size / (size_t)slot_size;
```

**Step 2b — restore MADV_NORMAL before releasing**

Locate the anchor (function return block at the bottom of `compact_migrate_records`):
```
    free(free_offs);
    segcache_release(&rh);
    segcache_release(&dh);
    return rc;
}
```

Replace with:
```c
    free(free_offs);
    madvise(rh.map, rh.map_size, MADV_NORMAL);
    segcache_release(&rh);
    madvise(dh.map, dh.map_size, MADV_NORMAL);
    segcache_release(&dh);
    return rc;
}
```

**Note on early-return paths**: `compact_migrate_records` has two early-return paths
before the madvise SEQUENTIAL calls (lines that return -1 when `segcache_acquire`
fails). Those paths return before the handles are acquired, so no MADV_NORMAL is needed
there. The `free_offs` allocation failure also returns -1 via `segcache_release(&rh)` /
`segcache_release(&dh)` without the NORMAL restore — this is acceptable since those
pages haven't been accessed yet (the scan loops haven't started).

Locate those early-return paths and verify they still look like:
```
    if (segcache_acquire(&dh, donor_path, 0, 0) != 0) return -1;
    if (segcache_acquire(&rh, recipient_path, 0, 0) != 0) {
        segcache_release(&dh);
        return -1;
    }
```
If they do, no change is needed there.

The `free_offs` malloc failure early return path:
```
            free(free_offs);
            segcache_release(&rh);
            segcache_release(&dh);
            return -1;
```
Add MADV_NORMAL restores before those releases too:
```c
            free(free_offs);
            madvise(rh.map, rh.map_size, MADV_NORMAL);
            segcache_release(&rh);
            madvise(dh.map, dh.map_size, MADV_NORMAL);
            segcache_release(&dh);
            return -1;
```

---

## Task 3 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.

Pay attention to tests involving `vacuum` and index compaction.

---

## Invariants and edge cases

- `MADV_SEQUENTIAL` is purely advisory. It cannot cause data corruption. If `madvise`
  returns an error (e.g., unsupported on some kernel), the code continues correctly —
  the `kf_seq_set` guard ensures MADV_NORMAL is only called if SEQUENTIAL succeeded.
- In `compact_migrate_records`, MADV hints are applied unconditionally (no success
  check). `madvise` failure is benign here — worst case is no readahead improvement.
- The `rh` (recipient) segment is written to (not just read), but mmap MAP_SHARED
  writes are always coherent with the file. MADV_SEQUENTIAL affects only read prefetch
  and does not interfere with writes.
- MADV_NORMAL is restored before `segcache_release` so that if another thread acquires
  the same segcache entry (unlikely during compaction but possible for the recipient
  which stays live), it gets normal point-lookup access patterns.
