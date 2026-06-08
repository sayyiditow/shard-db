# Plan: O_DIRECT segment scan for recover_streams (startup)

**Goal**: Convert `recover_one_stream`'s segment scan for non-active files from
`segcache_acquire` (MAP_SHARED mmap) to O_DIRECT reads, so that old segment files do not
enter the page cache during startup recovery.

**Why**: `recover_streams` is called inside `slotcask_open` for every object at startup
(triggered by `warmup_object_open` → `slotcask_registry_get`). It scans **every `.dat`
file** in every stream to rebuild the tombstone free pool and locate the reserve frontier.
For an object with 10+ segment files of 128 MB each, this faults gigabytes of old segment
data into page cache — the same data the warmup comment explicitly says to avoid touching
because it "evicts hotter index pages."

**Why only non-active files**: The active (last) segment file must keep `segcache_acquire`
because subsequent inserts call `segcache_acquire` on it immediately. Taking the active
file through O_DIRECT would cause a cold segcache miss on the first write after startup.
Non-active files are read-once at startup and never written; they are perfect O_DIRECT
candidates.

**What recovery needs from each file**:
- Non-last files: find every slot with `flag==2` (tombstone) → `pool_push_free`. No
  early stop; preallocated empty tail at the end is harmless (inner loop stops naturally
  at last complete slot).
- Last (active) file: same tombstone scan PLUS find first `flag==0` slot to set
  `reserve_off`. Must stay in segcache for write access.

**Buffer size**: `lcm(ODIRECT_ALIGN=4096, slot_size)` scaled up to ~4 MB. This guarantees
every chunk holds an exact integer number of slots (no cross-chunk partial slot), so the
inner `for` loop needs no carry-over logic. `slot_size` is always a multiple of 8 (floor
32), so `lcm(4096, slot_size)` is always a multiple of 4096 (O_DIRECT safe) and of
`slot_size` (slot-aligned). Example: slot_size=40 → lcm=20480 → buf ≈ 4.2 MB.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-recover-streams`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `#include "io_direct.h"` to `slotcask.c`

**File**: `src/db/slotcask.c`

Locate the anchor:
```
#include <time.h>
#include <pthread.h>
```

Replace with:
```c
#include <time.h>
#include <pthread.h>
#include "io_direct.h"
```

If `#include "io_direct.h"` is already present (another O_DIRECT plan ran first), skip
this task and write `PLAN_NOTES.md: io_direct.h already included`.

---

## Task 2 — Add `recover_od_buf_size` and `recover_scan_tombstones_od` before `recover_one_stream`

**File**: `src/db/slotcask.c`

Locate the anchor (the comment immediately before `recover_one_stream`):
```
/* Walk every segment for a single stream, populate the in-memory free-slot
   pool from flag=2 slots, and position reserve_off past the last live slot
   in the highest-numbered segment. Returns 0 on success, -1 on error. */
static int recover_one_stream(SlotcaskDb *db, int sid) {
```

Insert the following IMMEDIATELY BEFORE that anchor:
```c
/* Compute a buffer size that is a multiple of both ODIRECT_ALIGN and
   slot_size. This guarantees every chunk holds an integer number of slots
   so the scan loop needs no carry-over state.
   slot_size is always a multiple of 8 (floor 32) so gcd(4096, slot_size)
   >= 8 and the computed lcm is always manageable (< 8 MB for any valid
   slot_size). */
static size_t recover_od_buf_size(int slot_size) {
    size_t a = (size_t)ODIRECT_ALIGN;
    size_t b = (size_t)slot_size;
    size_t x = a, y = b;
    while (y) { size_t t = x % y; x = y; y = t; }  /* x = gcd(a,b) */
    size_t lcm = a / x * b;
    /* Scale up to ~4 MB so we amortise syscall overhead across many slots. */
    size_t n = (ODIRECT_BUF_SIZE_DEFAULT + lcm - 1) / lcm;
    if (n < 1) n = 1;
    return lcm * n;
}

/* O_DIRECT scan of a non-active segment file for tombstoned (flag==2) slots.
   Pages never enter the page cache. Called for every file_id != last_id
   in recover_one_stream. Returns 0 on success; errors are non-fatal
   (missed tombstones just reduce free-pool size until next vacuum). */
static int recover_scan_tombstones_od(SlotcaskDb *db, int sid,
                                       int file_id, const char *path) {
    int fd = od_open(path);
    if (fd < 0) return -1;

    size_t buf_size = recover_od_buf_size(db->slot_size);
    uint8_t *buf = aligned_alloc(ODIRECT_ALIGN, buf_size);
    if (!buf) { close(fd); return -1; }

    int slot_size = db->slot_size;
    off_t file_off = 0;
    for (;;) {
        ssize_t nr = od_pread(fd, buf, buf_size, file_off);
        if (nr <= 0) break;
        /* buf_size is a multiple of slot_size, so nr is also a multiple
           (or a short final read whose incomplete trailing bytes are safely
           skipped by the <= condition). */
        for (ssize_t off = 0; off + slot_size <= nr; off += slot_size) {
            if (buf[off + 18] == 2) {
                pool_push_free(&db->streams[sid], (uint16_t)file_id,
                               (uint32_t)(file_off + off));
            }
        }
        file_off += (off_t)nr;
        if (nr < (ssize_t)buf_size) break; /* EOF */
    }

    free(buf);
    close(fd);
    return 0;
}

```

---

## Task 3 — Use O_DIRECT for non-last files in `recover_one_stream`

**File**: `src/db/slotcask.c`

Locate the anchor (the entire per-file loop body inside `recover_one_stream`):
```
    for (size_t fi = 0; fi < n_ids; fi++) {
        int file_id = ids[fi];
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, (uint32_t)file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) { free(ids); return -1; }
        off_t pos = 0;
        off_t lim = (off_t)h.map_size;
        while (pos + db->slot_size <= lim) {
            uint8_t flag = __atomic_load_n(&h.map[pos + 18], __ATOMIC_ACQUIRE);
            if (flag == 2) {
                pool_push_free(&db->streams[sid], (uint16_t)file_id,
                               (uint32_t)pos);
            } else if (flag == 0) {
                /* First empty slot in highest-numbered segment marks the
                   reserve frontier. Earlier segments may have empty tails
                   too (preallocated 128 MB), but only the latest one
                   matters for reserve_off. */
                if (file_id == last_id) break;
            }
            pos += db->slot_size;
        }
        if (file_id == last_id) last_offset = pos;
        segcache_release(&h);
    }
```

Replace with:
```c
    for (size_t fi = 0; fi < n_ids; fi++) {
        int file_id = ids[fi];
        char path[PATH_MAX];
        seg_path_for(path, db->data_dir, sid, (uint32_t)file_id);

        if (file_id != last_id) {
            /* Non-active segment: O_DIRECT scan for tombstones only.
               Read-once at startup, never written — pages must not enter
               the page cache and displace KF/index pages loaded by warmup. */
            recover_scan_tombstones_od(db, sid, file_id, path);
            continue;
        }

        /* Active (last) segment: mmap via segcache so the first post-startup
           insert doesn't take a cold segcache miss. Also scans tombstones
           and locates the reserve frontier. */
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0) != 0) { free(ids); return -1; }
        off_t pos = 0;
        off_t lim = (off_t)h.map_size;
        while (pos + db->slot_size <= lim) {
            uint8_t flag = __atomic_load_n(&h.map[pos + 18], __ATOMIC_ACQUIRE);
            if (flag == 2) {
                pool_push_free(&db->streams[sid], (uint16_t)file_id,
                               (uint32_t)pos);
            } else if (flag == 0) {
                break;
            }
            pos += db->slot_size;
        }
        last_offset = pos;
        segcache_release(&h);
    }
```

---

## Task 4 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
Pay attention to tests involving `insert`, `delete`, and `vacuum` (all exercise the free
pool path populated by recovery).

---

## Invariants and edge cases

- `recover_scan_tombstones_od` errors are non-fatal: if `od_open` fails (e.g., ENOENT
  on a file removed mid-startup by a concurrent vacuum), the function returns -1 and
  `recover_one_stream` continues. Missed tombstones just mean those slots are not in the
  free pool until next vacuum — correctness is unaffected.
- The `buf[off + 18]` flag read is a plain byte read from the O_DIRECT buffer (no atomic
  needed). O_DIRECT gives a point-in-time file snapshot; no concurrent writer can modify
  a non-active segment (they're only written by vacuum under `objlock_wrlock`).
- `SLOTCASK_SEG_MAX_BYTES = 128 MB` is always a multiple of `ODIRECT_ALIGN = 4096`
  (128 × 1024 × 1024 / 4096 = 32768), so `od_pread` never produces a partial-block read
  except at EOF. The inner loop's `off + slot_size <= nr` condition handles any trailing
  incomplete slot cleanly.
- Single-file streams (`n_ids == 1`): `last_id == ids[0]`, so `file_id != last_id` is
  never true — the entire recovery uses the existing segcache path. No behaviour change.
- If `od_open` falls back to buffered I/O + `POSIX_FADV_DONTNEED` internally (unsupported
  O_DIRECT on the filesystem), the `DONTNEED` advice is applied after each read, so pages
  are evicted immediately. Cache impact is minimal in this fallback case.
- The `recover_od_buf_size` GCD loop cannot infinite-loop: it terminates in
  O(log(min(a,b))) iterations (Euclidean algorithm). For any valid slot_size ≥ 32,
  gcd(4096, slot_size) ≥ 8 so lcm ≤ 4096 × slot_size / 8 = 512 × slot_size. Even at
  max slot_size ~11000 bytes, buf ≈ 5.6 MB — well within reason.
