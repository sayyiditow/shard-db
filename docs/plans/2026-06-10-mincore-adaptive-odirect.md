# Plan: mincore-adaptive O_DIRECT

**Date:** 2026-06-10  
**Branch:** `feat/mincore-adaptive-odirect`  
**Files:** `src/db/io_direct.c`, `src/db/io_direct.h`

## Problem

`od_open` always opens files with `O_DIRECT` (cache-bypass). When a file is already
resident in the OS page cache — after warmup, or because queries have been touching
index/kf files — we throw away that in-RAM data and re-read from disk at NVMe speed
(~3–4 GB/s) instead of RAM speed (~30–50 GB/s). This hurts aggregate full scans,
reindex btree scans, and any other O_DIRECT path that happens to be running on warm
data.

**Fix:** before opening with `O_DIRECT`, probe a small sample of pages with
`mincore(2)`. If ≥80% of sampled pages are resident, open buffered instead — the
caller's `od_pread` calls work identically on buffered fds. If cold, fall through to
existing `O_DIRECT` logic unchanged.

**Expected gain:** aggregate scan on warm data drops from ~12 s (NVMe) to ~1–2 s (RAM).
Reindex on a recently-warmed object also benefits. No change for cold data.

## Execution rules

- Branch off `main`.
- Tasks in order; build after every task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Never claim a step passed without the real build/test output.
- Locate every edit by the **quoted anchor text** below; if an anchor is not found
  exactly, stop and write `PLAN_NOTES.md` — do not guess.

---

## Task 1 — Add `file_is_resident` helper in `io_direct.c`

**File:** `src/db/io_direct.c`

**Anchor** (insert immediately before this line):
```
/* ============================================================
 * Low-level helpers
 * ========================================================= */
```

Insert the following block immediately before that anchor:

```c
/* ============================================================
 * Page-cache residency probe (Linux only)
 * ========================================================= */

#ifdef __linux__
#include <sys/mman.h>

/* Returns 1 if at least `threshold_pct` percent of the file's pages
   are resident in the OS page cache, 0 otherwise (including any error).
   Uses N_SAMPLES evenly-spaced page probes so the cost is O(1) regardless
   of file size — no full mincore vec allocation.

   Mechanism:
     mmap(PROT_NONE | MAP_PRIVATE | MAP_NORESERVE) reserves virtual address
     space without touching any physical pages.  mincore() then queries which
     4 KB pages in that mapping are backed by the page cache.  munmap releases
     the virtual reservation.  Total syscall cost: ~5–15 µs per file.

   Conservative on error: returns 0 so the caller falls through to O_DIRECT. */
#define OD_MINCORE_SAMPLES 8
static int file_is_resident(const char *path, int threshold_pct)
{
    struct stat st;
    if (stat(path, &st) != 0 || st.st_size <= 0)
        return 0;

    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;

    size_t sz = (size_t)st.st_size;
    void *addr = mmap(NULL, sz, PROT_NONE,
                      MAP_PRIVATE | MAP_NORESERVE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) return 0;

    long pgsz = sysconf(_SC_PAGESIZE);
    if (pgsz <= 0) pgsz = 4096;
    long total_pages = ((long)sz + pgsz - 1) / pgsz;

    int resident = 0;
    for (int i = 0; i < OD_MINCORE_SAMPLES; i++) {
        long page_idx = (long)i * total_pages / OD_MINCORE_SAMPLES;
        unsigned char vec = 0;
        /* mincore on a single page at each sample offset. */
        if (mincore((char *)addr + page_idx * pgsz,
                    (size_t)pgsz, &vec) == 0 && (vec & 1))
            resident++;
    }
    munmap(addr, sz);

    return resident * 100 / OD_MINCORE_SAMPLES >= threshold_pct;
}
#undef OD_MINCORE_SAMPLES
#endif /* __linux__ */

```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 2 — Check residency at the top of `od_open`

**File:** `src/db/io_direct.c`

**Anchor** (exact text — the opening of `od_open`):
```
int od_open(const char *path)
{
    int fd = -1;

#if defined(__APPLE__)
```

Replace with:

```c
int od_open(const char *path)
{
    int fd = -1;

#ifdef __linux__
    /* If the file is already resident in the page cache, open it buffered.
       od_pread is plain pread() so it works on any fd.  RAM bandwidth
       (~30–50 GB/s) far exceeds NVMe O_DIRECT bandwidth (~3–4 GB/s), so
       skipping O_DIRECT on warm files is a strict win.
       Threshold 80%: tolerates a few evicted pages at the edges of a large
       file without wrongly forcing O_DIRECT. */
    if (file_is_resident(path, 80))
        return open(path, O_RDONLY);
#endif

#if defined(__APPLE__)
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 3 — Update `od_open` header comment in `io_direct.h`

**File:** `src/db/io_direct.h`

**Anchor** (exact text):
```
/* Open `path` for unbuffered scanning.  Returns fd >= 0 on success.
 * Linux: tries O_DIRECT; macOS: applies F_NOCACHE.
 * On failure to enable cache-bypass (EINVAL / ENOTSUP / unsupported):
 *   silently opens buffered and applies posix_fadvise(SEQUENTIAL|DONTNEED).
 * Caller cannot distinguish buffered from unbuffered — that is intentional. */
int od_open(const char *path);
```

Replace with:

```c
/* Open `path` for unbuffered scanning.  Returns fd >= 0 on success.
 * Linux: probes page-cache residency via mincore first; if ≥80% of sampled
 *   pages are resident, opens buffered (RAM speed beats O_DIRECT on warm data).
 *   Otherwise tries O_DIRECT; falls back to buffered+FADV_DONTNEED on EINVAL.
 * macOS: applies F_NOCACHE via fcntl after open.
 * Caller cannot distinguish buffered from unbuffered — that is intentional. */
int od_open(const char *path);
```

Build: `SKIP_TESTS=1 ./build.sh` — must succeed.

---

## Task 4 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

Paste the actual output. Do not claim pass without real output.

---

## Invariants and edge cases

| Case | Expected behaviour |
|---|---|
| File ≥80% resident | `open(O_RDONLY)` returned; `od_pread` calls `pread()` — works identically |
| File cold (0% resident) | `file_is_resident` returns 0; existing O_DIRECT path unchanged |
| Mixed (some pages evicted) | 80% threshold tolerates partial eviction; still uses buffered if mostly hot |
| `stat` fails (file missing) | `file_is_resident` returns 0 → O_DIRECT path → `open(..., O_DIRECT)` fails with `ENOENT`; same error as before |
| `mmap(PROT_NONE)` fails | returns 0 → O_DIRECT path; conservative, no regression |
| `mincore` not available (kernel <2.3.99) | not a concern — supported since Linux 2.3.99 (1999) |
| macOS | `#ifdef __linux__` guard; `file_is_resident` not compiled; `od_open` unchanged on macOS |
| File size 0 | `file_is_resident` returns 0 → O_DIRECT path |
| `n_samples > total_pages` | `page_idx * pgsz` is within file; for tiny files all samples hit page 0; if resident returns 100% → buffered (correct) |

## Why 80% threshold

A 32 MB O_DIRECT chunk covers 8192 pages. If 6554+ pages (80%) are resident,
the file is effectively hot — the remaining cold pages are far cheaper to serve
from page-fault than to re-read the whole file via O_DIRECT. Lower threshold
(e.g. 50%) risks buffered reads on files that are only partially warm, causing
unnecessary cache pollution. Higher (e.g. 95%) risks O_DIRECT on nearly-hot
files. 80% is the midpoint and matches common "hot enough" heuristics in
Aerospike's implementation.
