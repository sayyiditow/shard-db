# Plan: O_DIRECT donor scan in compact_migrate_records

**Goal**: Replace the donor segment's `segcache_acquire` (MAP_SHARED mmap) in
`compact_migrate_records` with a true O_DIRECT scan via `seg_scan_o_direct`, so that
vacuum compaction does not pull donor segment data into the page cache.

**Why**: During vacuum, `compact_migrate_records` reads the donor segment once (to migrate
live records into the recipient) and then deletes it. It is a perfect read-once workload.
The current mmap path faults the entire donor into page cache and keeps it resident after
the call, displacing hot working-set data. Switching the donor read to O_DIRECT means its
pages never enter cache.

**Scope**: Donor segment only (read-once, then unlinked). The recipient segment stays as
`segcache_acquire` because (a) it has already been partially written and its pages are
likely already resident, and (b) we need to write to it via mmap in the same call.

---

## Execution rules

- Branch off `main`: `git checkout -b feat/odirect-compact`
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

If `#include "io_direct.h"` is already present (because `feat/odirect-rebuild` ran first),
skip this task and write `PLAN_NOTES.md: io_direct.h already included`.

---

## Task 2 — Add `CompactOdCtx` struct and `compact_od_cb` before `compact_migrate_records`

**File**: `src/db/slotcask.c`

Locate the anchor (the function signature of `compact_migrate_records`):
```
static int compact_migrate_records(SlotcaskDb *db, int stream_id,
                                    uint32_t donor_fid, uint32_t recipient_fid) {
```

Insert the following IMMEDIATELY BEFORE that line:
```c
/* Context for compact_od_cb — carries recipient mmap and free-slot list. */
typedef struct {
    SlotcaskDb *db;
    int         stream_id;
    uint32_t    donor_fid;
    uint32_t    recipient_fid;
    uint8_t    *rmap;        /* recipient mmap base (MAP_SHARED, writeable) */
    uint32_t   *free_offs;   /* free slot byte-offsets in recipient */
    size_t      free_count;
    size_t      free_idx;
    int         rc;
} CompactOdCtx;

/* od_record_cb adapter: called by seg_scan_o_direct for each live donor slot.
   Writes the record to the next free recipient slot, then repoints the kf entry
   under the kf shard's wrlock. */
static int compact_od_cb(const uint8_t *rec, size_t vlen,
                          const uint8_t hash16[16], void *raw) {
    CompactOdCtx *c = (CompactOdCtx *)raw;
    if (c->rc != 0) return 1;  /* already failed — abort scan */

    if (c->free_idx >= c->free_count) { c->rc = -1; return 1; }

    uint32_t target_off = c->free_offs[c->free_idx];

    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    const uint8_t *key   = rec + 24;
    const uint8_t *value = rec + 24 + (size_t)klen;

    /* Step 1: write record into recipient free slot (vacuum holds objlock_wrlock
       so no concurrent writer can race on this offset). */
    seg_record_emit(c->rmap + target_off, c->db->slot_size,
                    hash16, key, (size_t)klen, value, (size_t)vlen);

    /* Step 2: repoint kf entry under the kf shard wrlock. */
    int kfshard = shard_for_hash(hash16, c->db->num_shards);
    char kfp[PATH_MAX];
    kf_path_for(kfp, c->db->data_dir, kfshard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, c->db->slots_per_shard, 1) != 0) {
        c->rc = -1; return 1;
    }

    uint8_t cur_flag, cur_sid;
    uint16_t cur_fid;
    uint32_t cur_off;
    size_t kf_slot_idx;
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0 || (int)cur_sid != c->stream_id ||
        (uint32_t)cur_fid != c->donor_fid) {
        /* Orphan (lr!=0) or already repointed (cur_fid != donor_fid):
           recipient slot was written but kf won't point to it — the slot
           becomes a recoverable orphan, same as the original mmap path. */
        kfcache_release(&kh);
        c->free_idx++;
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    c->free_idx++;
    return 0;
}

```

---

## Task 3 — Rewrite `compact_migrate_records` to use O_DIRECT for the donor

**File**: `src/db/slotcask.c`

Replace the entire body of `compact_migrate_records` (from the opening `{` through the
closing `}`) using the anchor below. The anchor is the full current function body. Match
from the first line after the signature through the final `}`:

```
    char donor_path[PATH_MAX], recipient_path[PATH_MAX];
    seg_path_for(donor_path, db->data_dir, stream_id, donor_fid);
    seg_path_for(recipient_path, db->data_dir, stream_id, recipient_fid);

    SlotcaskSegHandle dh, rh;
    if (segcache_acquire(&dh, donor_path, 0, 0) != 0) return -1;
    if (segcache_acquire(&rh, recipient_path, 0, 0) != 0) {
        segcache_release(&dh);
        return -1;
    }

    int slot_size = db->slot_size;
    size_t total = dh.map_size / (size_t)slot_size;

    /* Build recipient free-offset list (every slot whose flag != 1). Done
       once up front so the migration loop is O(donor_live) instead of
       O(donor_live × recipient_total). */
    uint32_t *free_offs = NULL;
    size_t free_count = 0, free_cap = 0;
    for (size_t s = 0; s < total; s++) {
        const uint8_t *rec = rh.map + s * (size_t)slot_size;
        if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) == 1) continue;
        if (free_count == free_cap) {
            size_t nc = free_cap ? free_cap * 2 : 256;
            uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
            if (!t) {
                free(free_offs);
                segcache_release(&rh);
                segcache_release(&dh);
                return -1;
            }
            free_offs = t;
            free_cap = nc;
        }
        free_offs[free_count++] = (uint32_t)(s * (size_t)slot_size);
    }

    int rc = 0;
    size_t free_idx = 0;
    for (size_t s = 0; s < total && rc == 0; s++) {
        const uint8_t *drec = dh.map + s * (size_t)slot_size;
        if (__atomic_load_n(&drec[18], __ATOMIC_ACQUIRE) != 1) continue;

        if (free_idx >= free_count) { rc = -1; break; }

        uint8_t hash[16];
        memcpy(hash, drec, 16);
        uint16_t klen;
        uint32_t vlen;
        memcpy(&klen, drec + 16, 2);
        memcpy(&vlen, drec + 20, 4);
        const uint8_t *key = drec + 24;
        const uint8_t *value = drec + 24 + (size_t)klen;

        uint32_t donor_off = (uint32_t)(s * (size_t)slot_size);
        uint32_t target_off = free_offs[free_idx];

        /* Step 1: write recipient slot. Vacuum holds objlock_wrlock so no
           concurrent writer can race on this offset. */
        seg_record_emit(rh.map + target_off, slot_size, hash,
                         key, (size_t)klen, value, (size_t)vlen);

        /* Step 2-4: repoint kf entry under the kf shard's wrlock. */
        int kfshard = shard_for_hash(hash, db->num_shards);
        char kfp[PATH_MAX];
        kf_path_for(kfp, db->data_dir, kfshard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kfp, db->slots_per_shard, 1) != 0) {
            rc = -1; break;
        }
        uint8_t cur_flag, cur_sid;
        uint16_t cur_fid;
        uint32_t cur_off;
        size_t kf_slot_idx;
        int lr = kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                       &cur_flag, &cur_sid, &cur_fid,
                                       &cur_off, &kf_slot_idx);
        if (lr != 0) {
            /* No kf entry — donor record is an orphan from a prior crash.
               Its recipient mirror also becomes an orphan; no harm. */
            kfcache_release(&kh);
            free_idx++;
            continue;
        }
        if ((int)cur_sid != stream_id || (uint32_t)cur_fid != donor_fid ||
            cur_off != donor_off) {
            /* kf points elsewhere — donor slot was already superseded
               (e.g. by an earlier-in-this-vacuum migration). Skip. */
            kfcache_release(&kh);
            free_idx++;
            continue;
        }
        kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)stream_id,
                            (uint16_t)recipient_fid, target_off);
        kfcache_release(&kh);
        free_idx++;
    }

    free(free_offs);
    segcache_release(&rh);
    segcache_release(&dh);
    return rc;
}
```

Replace with the new body (same function signature, new body):
```c
    char donor_path[PATH_MAX], recipient_path[PATH_MAX];
    seg_path_for(donor_path, db->data_dir, stream_id, donor_fid);
    seg_path_for(recipient_path, db->data_dir, stream_id, recipient_fid);

    /* Recipient: mmap for writes and for building the free-slot list. */
    SlotcaskSegHandle rh;
    if (segcache_acquire(&rh, recipient_path, 0, 0) != 0) return -1;

    int slot_size = db->slot_size;
    size_t total = rh.map_size / (size_t)slot_size;

    /* Build recipient free-offset list (every slot whose flag != 1). */
    uint32_t *free_offs = NULL;
    size_t free_count = 0, free_cap = 0;
    for (size_t s = 0; s < total; s++) {
        const uint8_t *rec = rh.map + s * (size_t)slot_size;
        if (__atomic_load_n(&rec[18], __ATOMIC_ACQUIRE) == 1) continue;
        if (free_count == free_cap) {
            size_t nc = free_cap ? free_cap * 2 : 256;
            uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
            if (!t) {
                free(free_offs);
                segcache_release(&rh);
                return -1;
            }
            free_offs = t;
            free_cap = nc;
        }
        free_offs[free_count++] = (uint32_t)(s * (size_t)slot_size);
    }

    /* Donor: O_DIRECT scan — read-once, then unlinked; pages must not enter
       the page cache. compact_od_cb handles kf repoint per live record. */
    CompactOdCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map,
        .free_offs = free_offs, .free_count = free_count,
        .free_idx = 0, .rc = 0,
    };
    seg_scan_o_direct(donor_path, slot_size, compact_od_cb, &ctx);

    free(free_offs);
    segcache_release(&rh);
    return ctx.rc;
}
```

---

## Task 4 — Build and test

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed` with the same N as before.
Pay attention to tests involving `vacuum`.

---

## Invariants and edge cases

- The original code dropped the `cur_off != donor_off` check. This is safe: the kf lookup
  is by hash + key. If `cur_fid == donor_fid`, the only record at that hash in the donor
  is the one we're migrating (hashes are unique). There is no ambiguity.
- Orphaned recipient slots (written by `seg_record_emit` but not kf-repointed) are
  recoverable on next vacuum — the same behaviour as the original mmap path.
- Donor and recipient segment files are always the same size
  (`SLOTCASK_SEG_MAX_BYTES`) so `total = rh.map_size / slot_size` correctly counts both.
- If `seg_scan_o_direct` falls back to buffered I/O internally (unsupported FS), behaviour
  is identical — the callback contract is unchanged.
- `compact_od_cb` sets `ctx.rc = -1` and returns 1 (stop scan) when the free-slot list is
  exhausted, matching the original `rc = -1; break` path.
