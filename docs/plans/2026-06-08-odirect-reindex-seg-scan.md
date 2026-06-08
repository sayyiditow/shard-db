# Plan: O_DIRECT for reindex/add-index segment scans

**Goal**: Replace the buffered `open+fadvise+pread` loop in `seg_scan_worker` (index.c)
with a call to `seg_scan_o_direct`, giving reindex/add-index the same cache-bypassing
O_DIRECT path already used by FP_FULL_SCAN queries.

**Why**: `seg_scan_worker` reads every `.dat` segment file in full during `reindex` /
`add-index`. Currently it uses buffered `pread` with `POSIX_FADV_SEQUENTIAL` before
and `POSIX_FADV_DONTNEED` after each file. The fadvise-after-DONTNEED does evict pages
eventually, but the kernel still fills the page cache during the read, displacing hot
working-set pages. On a large object (50 GB segment store) this can flush the entire
L1/L2/TLB working set. `seg_scan_o_direct` already handles all alignment, double-
buffering, and EINVAL fallback — the only work here is extracting the inner-loop logic
into a callback and wiring it up.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-reindex-seg-scan`
- Build: `SKIP_TESTS=1 ./build.sh`
- Test: `./build/bin/shard-db-test run-all`
- Do tasks in order; do not skip.
- Locate every insertion site by the **quoted anchor text** given. If the anchor is not
  found exactly, stop and write `PLAN_NOTES.md` — do not guess or reinterpret.
- Never claim a step passed without showing the real build/test output.

---

## Task 1 — Add `#include "io_direct.h"` to index.c

**File**: `src/db/index.c`

Locate the anchor:
```
#include "slotcask.h"
```

Insert immediately after it:
```c
#include "io_direct.h"
```

Result after edit:
```c
#include "slotcask.h"
#include "io_direct.h"
```

---

## Task 2 — Add `reindex_seg_cb` callback before `seg_scan_worker`

**File**: `src/db/index.c`

This callback embodies the per-record field-extraction logic that is currently inside the
`pread` loop. `seg_scan_o_direct` only calls it for live slots (flag == 1), so the flag
check is omitted.

Locate the anchor (function definition line):
```
static void *seg_scan_worker(void *arg) {
```

Insert the following block **immediately before** that line:
```c
static int reindex_seg_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *ctx) {
    SegScanWorker *w = (SegScanWorker *)ctx;
    uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
    const uint8_t *value = rec + 24 + klen;
    (void)vlen;
    for (int fi = 0; fi < w->n_fields; fi++) {
        const MFFieldDesc *d = &w->descs[fi];
        if (d->type == MF_BITMAP) {
            int tidx = d->field_indices[0];
            if (tidx < 0) continue;
            const TypedField *tf = &w->ts->fields[tidx];
            const uint8_t *vb = value + tf->offset;
            const uint8_t *bval; size_t blen;
            if (tf->type == FT_VARCHAR) {
                uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
                if (al == 0) continue;
                bval = vb + 2; blen = al;
            } else {
                if (tf->size == 0) continue;
                bval = vb; blen = (size_t)tf->size;
            }
            int kf_shard = compute_record_shard(hash16, w->splits);
            bm_spill_append(&w->bm_writers[fi], kf_shard, bval, blen,
                            hash16, &w->had_error);
            continue;
        }
        mf_append_field(&w->fields[fi], d, hash16, value,
                        w->ts, w->splits, w->idx_n);
    }
    return 0;
}

```

**Invariant**: `reindex_seg_cb` must be byte-for-byte equivalent to the existing inner
slot loop in `seg_scan_worker`. The only differences are: (a) the flag==1 check is gone
(handled by `seg_scan_o_direct`), (b) `hash` is renamed `hash16` (the callback
parameter), (c) `value` is derived from `klen` extracted from `rec[16:17]`.

---

## Task 3 — Replace the buffer+pread loop in `seg_scan_worker`

**File**: `src/db/index.c`

Locate and **remove** the buffer allocation block. The anchor is:
```
    size_t slot_size = (size_t)w->slot_size;
    if (slot_size < 32) slot_size = 32;
    size_t chunk_slots = (4u << 20) / slot_size;
    if (chunk_slots < 1) chunk_slots = 1;
    size_t bufsz = chunk_slots * slot_size;
    uint8_t *buf = malloc(bufsz);
    if (!buf) { w->had_error = 1; return NULL; }
```

Replace with nothing (delete entirely — no replacement text).

---

Locate and **replace** the per-segment file-I/O block. The anchor (the entire block from
`open` through `close` and the segs_done increment):
```
        int fd = open(path, O_RDONLY);
        if (fd < 0) continue;
#ifdef __linux__
        posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
#endif
        off_t off = 0;
        for (;;) {
            ssize_t got = pread(fd, buf, bufsz, off);
            if (got <= 0) break;
            size_t nslots = (size_t)got / slot_size;
            if (nslots == 0) break;
            for (size_t s = 0; s < nslots; s++) {
                const uint8_t *rec = buf + s * slot_size;
                if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) != 1) continue;
                uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
                const uint8_t *hash  = rec;
                const uint8_t *value = rec + 24 + klen;
                for (int fi = 0; fi < w->n_fields; fi++) {
                    const MFFieldDesc *d = &w->descs[fi];
                    if (d->type == MF_BITMAP) {
                        /* Extract the bitmap dict value (strip varchar len prefix). */
                        int tidx = d->field_indices[0];
                        if (tidx < 0) continue;
                        const TypedField *tf = &w->ts->fields[tidx];
                        const uint8_t *vb = value + tf->offset;
                        const uint8_t *bval; size_t blen;
                        if (tf->type == FT_VARCHAR) {
                            uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
                            if (al == 0) continue;
                            bval = vb + 2; blen = al;
                        } else {
                            if (tf->size == 0) continue;
                            bval = vb; blen = (size_t)tf->size;
                        }
                        int kf_shard = compute_record_shard(hash, w->splits);
                        bm_spill_append(&w->bm_writers[fi], kf_shard, bval, blen,
                                        hash, &w->had_error);
                        continue;
                    }
                    mf_append_field(&w->fields[fi], d, hash, value,
                                    w->ts, w->splits, w->idx_n);
                }
            }
            off += (off_t)(nslots * slot_size);
            if ((size_t)got < bufsz) break;  /* EOF */
        }
#ifdef __linux__
        /* Drop this segment's pages — they won't be touched again, and
           caching the whole value store would thrash a memory-bound box. */
        posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#endif
        close(fd);
        if (w->segs_done) atomic_fetch_add(w->segs_done, 1);
```

Replace with:
```c
        int rc = seg_scan_o_direct(path, (int)w->slot_size, reindex_seg_cb, w);
        if (rc < 0) w->had_error = 1;
        if (w->segs_done) atomic_fetch_add(w->segs_done, 1);
```

---

Locate and **remove** the `free(buf)` line. The anchor is:
```
    free(buf);
```
(It appears once, immediately after the per-segment loop, before the `/* Final flush */`
comment block.) Delete this line entirely.

---

## Task 4 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.

Pay attention to any test involving `add-index`, `reindex`, or index rebuild.

---

## Invariants and edge cases

- `seg_scan_o_direct` already handles: O_DIRECT open failure (silent fallback to buffered
  + POSIX_FADV_DONTNEED), EOF on last chunk, aligned buffer allocation.
- `seg_scan_o_direct` only fires the callback for live slots (flag == 1). The removed
  `__atomic_load_n(&rec[18], ...)` check was equivalent.
- The `segs_done` atomic increment is preserved in the same position.
- `w->had_error` is set on I/O error (`rc < 0`). The old code used `continue` on open
  failure (fd < 0), effectively the same — a missing segment does not set `had_error`.
  `seg_scan_o_direct` returns -errno on hard I/O errors only; a missing file returns 0
  (skipped silently). This matches the old `if (fd < 0) continue` behavior.
- The `w->slot_size` guard (`slot_size < 32 → slot_size = 32`) from the old code is
  dropped. `seg_scan_o_direct` internally uses `slot_size` for chunk subdivision;
  schemas with `slot_size < 32` cannot exist (minimum header is 24 B + at least 1-byte
  key + empty value = 25 B, rounded up). The guard was dead code.
