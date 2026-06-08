# Plan: O_DIRECT KF shard scan for slotcask_walk_live_skip (fetch/keys)

**Goal**: Replace the `kfcache_acquire` mmap-based KF shard scan in
`slotcask_walk_live_skip` with a true O_DIRECT read via a new `kf_scan_o_direct` helper,
so that `fetch` and `keys` commands do not pull the KF shards into page cache.

**Why**: `slotcask_walk_live_skip` is the backend for the `fetch` and `keys` commands. It
reads every KF shard sequentially (all `cap` entries in slot order). At `splits=256` the
KF is 384 MB total; at `splits=4096` it can be 6 GB. The current `kfcache_acquire` path
maps each shard MAP_SHARED and faults every entry page into cache, polluting the cache for
an O(N) walk that is never repeated. O_DIRECT KF reads leave the cache untouched.

**Scope**: KF scan only. The subsequent per-record segment lookups (after the skip window)
use `segcache_acquire` for random single-record access — not a sequential scan, not a
candidate for O_DIRECT, and not changed by this plan.

**`SlotcaskKfEntry` layout** (24 bytes, packed):
`hash[16] | flag(1B) | stream_id(1B) | file_id(2B) | offset(4B)`
`SLOTCASK_KF_HDR_SIZE = 24` bytes (header before entries).

**Buffer size**: `12 * 1024 * 1024 = 12582912` bytes. This is the smallest practical
O_DIRECT-aligned (`4096` divides it) buffer that also holds an integer number of entries
(`12582912 / 24 = 524288` exactly). No cross-chunk entry splits.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-fetch-kf`
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

If `#include "io_direct.h"` is already present, skip and write
`PLAN_NOTES.md: io_direct.h already included`.

---

## Task 2 — Add `kf_scan_o_direct` and `KfOdSkipCtx` before `slotcask_walk_live_skip`

**File**: `src/db/slotcask.c`

Locate the anchor (function signature of `slotcask_walk_live_skip`):
```
int slotcask_walk_live_skip(SlotcaskDb *db, int64_t skip_n,
                              SlotcaskScanCb cb, void *ctx) {
```

Insert the following IMMEDIATELY BEFORE that line:
```c
/* O_DIRECT sequential scan of a KF shard file. Reads entries in chunks of
   12 MB (aligned to both ODIRECT_ALIGN=4096 and sizeof(SlotcaskKfEntry)=24).
   Calls cb(entry, ctx) for every entry including empty and tombstoned ones —
   the caller decides which flags to act on. Stops early if cb returns != 0. */
#define KF_OD_BUF_SIZE (12 * 1024 * 1024)  /* 12 MB: lcm(4096,24)*1024 */
static int kf_scan_o_direct(const char *kf_path,
                              int (*cb)(const SlotcaskKfEntry *, void *),
                              void *ctx) {
    int fd = od_open(kf_path);
    if (fd < 0) return -1;
    uint8_t *buf = aligned_alloc(ODIRECT_ALIGN, KF_OD_BUF_SIZE);
    if (!buf) { close(fd); return -1; }

    off_t file_off = 0;
    int stopped = 0;
    while (!stopped) {
        ssize_t nr = od_pread(fd, buf, KF_OD_BUF_SIZE, file_off);
        if (nr <= 0) break;
        /* First chunk: skip the 24-byte KF file header. */
        size_t start = (file_off == 0) ? SLOTCASK_KF_HDR_SIZE : 0;
        for (size_t off = start;
             off + sizeof(SlotcaskKfEntry) <= (size_t)nr && !stopped;
             off += sizeof(SlotcaskKfEntry)) {
            if (cb((const SlotcaskKfEntry *)(buf + off), ctx) != 0)
                stopped = 1;
        }
        file_off += (off_t)nr;
    }

    free(buf);
    close(fd);
    return 0;
}

/* Callback context for slotcask_walk_live_skip's O_DIRECT KF scan. */
typedef struct {
    SlotcaskDb    *db;
    int64_t        remaining_skip;
    int            stop;
    SlotcaskScanCb cb;
    void          *ctx;
} KfOdSkipCtx;

static int kf_od_skip_emit_cb(const SlotcaskKfEntry *e, void *raw) {
    KfOdSkipCtx *c = (KfOdSkipCtx *)raw;
    if (c->stop) return 1;
    if (e->flag != 1) return 0;

    /* Cheap skip: count live entries without touching segments. */
    if (c->remaining_skip > 0) { c->remaining_skip--; return 0; }

    /* Past the skip window — load the segment record and emit. */
    char seg_path[PATH_MAX];
    seg_path_for(seg_path, c->db->data_dir, e->stream_id, e->file_id);
    SlotcaskSegHandle sh;
    if (segcache_acquire(&sh, seg_path, 0, 0) != 0) return 0;
    const uint8_t *rec = sh.map + e->offset;
    if (!seg_rec_live_with_hash(rec, e->hash)) {
        segcache_release(&sh);
        return 0;
    }
    uint16_t klen = seg_rec_klen(rec);
    uint32_t vlen = seg_rec_vlen(rec);
    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + klen;
    if (c->cb(e->hash, key, klen, value, vlen, c->ctx) != 0) {
        c->stop = 1;
        segcache_release(&sh);
        return 1;
    }
    segcache_release(&sh);
    return 0;
}

```

---

## Task 3 — Replace `slotcask_walk_live_skip` body with O_DIRECT KF scan

**File**: `src/db/slotcask.c`

Replace the entire body of `slotcask_walk_live_skip` (from the opening `{` to the closing
`}`) using this exact anchor for the old body:
```
    if (!db || !cb) return -1;
    int64_t remaining_skip = skip_n;
    int stop = 0;
    for (int s = 0; s < db->num_shards && !stop; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) continue;

        size_t cap = kh.capacity;
        SlotcaskKfEntry *kf = kh.map;
        for (size_t i = 0; i < cap && !stop; i++) {
            SlotcaskKfEntry *e = &kf[i];
            uint8_t flag = __atomic_load_n(&e->flag, __ATOMIC_ACQUIRE);
            if (flag != 1) continue;

            /* Cheap skip: count this live entry, no segcache touch. */
            if (remaining_skip > 0) { remaining_skip--; continue; }

            /* Past the skip window — load the seg and emit. */
            char seg_path[PATH_MAX];
            seg_path_for(seg_path, db->data_dir, e->stream_id, e->file_id);
            SlotcaskSegHandle sh;
            if (segcache_acquire(&sh, seg_path, 0, 0) != 0) continue;
            const uint8_t *rec = sh.map + e->offset;
            if (!seg_rec_live_with_hash(rec, e->hash)) {
                segcache_release(&sh);
                continue;
            }
            uint16_t klen = seg_rec_klen(rec);
            uint32_t vlen = seg_rec_vlen(rec);
            const uint8_t *key   = rec + 24;
            const uint8_t *value = rec + 24 + klen;
            if (cb(e->hash, key, klen, value, vlen, ctx) != 0) stop = 1;
            segcache_release(&sh);
        }
        kfcache_release(&kh);
    }
    return 0;
}
```

Replace with:
```c
    if (!db || !cb) return -1;
    KfOdSkipCtx c = {
        .db = db, .remaining_skip = skip_n, .stop = 0, .cb = cb, .ctx = ctx
    };
    for (int s = 0; s < db->num_shards && !c.stop; s++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, s);
        kf_scan_o_direct(kf_path, kf_od_skip_emit_cb, &c);
    }
    return 0;
}
```

---

## Task 4 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
Pay attention to tests involving `fetch` and `keys`.

---

## Invariants and edge cases

- `kf_scan_o_direct` calls `cb` for ALL entries (empty, live, tombstoned). The
  `kf_od_skip_emit_cb` filters to `flag == 1` (live only) — matches original behaviour.
- `remaining_skip` is in the shared `KfOdSkipCtx` and decrements across shards, so the
  skip window is correctly applied across the full shard sequence.
- The O_DIRECT read is a file snapshot. A concurrent writer may update a KF entry after
  the O_DIRECT read but before we call `segcache_acquire`. The `seg_rec_live_with_hash`
  check in `kf_od_skip_emit_cb` detects this stale-pointer race — same guard as the
  original mmap path.
- If `od_open` fails (file not found, or newly created shard not yet flushed), the shard
  is silently skipped — matching the original `if (kfcache_acquire(...) != 0) continue`.
- The `kf_scan_o_direct` 12 MB buffer holds 524288 entries per chunk. At 1M slots/shard
  (small objects), this reads the whole shard in two chunks. At 256K slots/shard, one
  chunk. At 16M slots/shard (max after resplit), 32 chunks.
- `kf_scan_o_direct` is a static helper — not exported, not in `slotcask.h`.
