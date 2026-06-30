# Fix: compact kf-corruption + rebuild-kf recovery command

## Background / root cause

After the 2026.06.4 release (variable-length records, PR #185), `cmd_vacuum` on the light
path calls `slotcask_compact_segs` → `compact_one_stream_varlen` per stream. That function:

1. Enumerates all non-active segment files in the stream directory.
2. Uses `seg_stat_one_varlen` to count flag=1 records per file (live count).
3. Two-pointer merge: migrates sparse donors into dense recipients via
   `compact_migrate_records_varlen`, then calls `compact_drop_seg_file` on the donor.

**The bug**: `compact_migrate_records_varlen` returns 0 (success) even when the kf update
for a record was silently skipped — including cases where `kf_lookup_with_slot` returned
−1 because `verify_stored_key` couldn't open the segment file the kf entry currently
references (the file is already missing or corrupted). In that case the kf entry still
points to the donor file, but the donor gets deleted anyway, leaving a dangling kf pointer.

Confirmed local symptom: 25% of kf entries in the users object point to segment files that
no longer exist (`streams/006/000464.dat`, `000465.dat`, `streams/007/001464.dat`,
`001465.dat`), causing every bulk-insert that needs `pre_commit_needs_old=1` (triggered by
karma/created btree indexes) to fail with `some_records_dropped`.

## Execution rules

- Branch: `git checkout -b feat/rebuild-kf` off `main`.
- Do tasks in order; do not skip.
- Build: `SKIP_TESTS=1 ./build.sh` after each C task; confirm zero errors.
- Test: `./build/bin/shard-db-test run-all` at the end; must print `# total: N passed, 0 failed`.
- Anchors below are exact quoted text; if a quoted anchor is not found verbatim, stop and
  write `docs/plans/PLAN_NOTES.md` — do not guess.
- Never claim a step passed without pasting the real output.

---

## Task 1 — Branch

```bash
git checkout -b feat/rebuild-kf
```

---

## Task 2 — Add `kf_lookup_failed` counter to `VarlenCompactCtx`

**File**: `src/db/slotcask.c`

**Anchor** (find this exact text):

```c
} VarlenCompactCtx;
```

**Replace** the entire struct (from `/* Context for varlen compact_cb. */` through
`} VarlenCompactCtx;`) with:

```c
/* Context for varlen compact_cb. */
typedef struct {
    SlotcaskDb *db;
    int         stream_id;
    uint32_t    donor_fid;
    uint32_t    recipient_fid;
    uint8_t    *rmap;        /* recipient mmap base */
    size_t      rmap_size;   /* total mapped bytes */
    uint32_t   *free_offs;   /* free slot byte-offsets in recipient */
    uint32_t   *free_caps;   /* capacity of each free slot (0 = unbounded) */
    size_t      free_count;
    size_t      free_next;   /* next index to try (cached linear scan position) */
    int         rc;
    uint32_t    kf_lookup_failed; /* live records where kf lookup returned -1 */
} VarlenCompactCtx;
```

---

## Task 3 — Track kf_lookup_failed in `varlen_compact_cb`

**File**: `src/db/slotcask.c`

**Anchor** (find this exact block inside `varlen_compact_cb`):

```c
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0 || (int)cur_sid != c->stream_id ||
        (uint32_t)cur_fid != c->donor_fid) {
        kfcache_release(&kh);
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    return 0;
```

**Replace** with:

```c
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0) {
        /* Lookup failed: check whether a live kf entry exists for this hash.
           If yes, the entry's stored segment location is inaccessible (file
           missing or corrupt) — count this as a failed update so the donor
           is not deleted.  If the hash has no live entry (deleted or unknown),
           it is a legitimate orphan and we skip silently. */
        size_t cap = kh.capacity;
        size_t kstart = kf_slot_for(hash16, cap);
        for (size_t ki = 0; ki < cap; ki++) {
            size_t kslot = (kstart + ki) % cap;
            SlotcaskKfEntry *ke = &kh.map[kslot];
            if (ke->flag == 0) break;
            if (memcmp(ke->hash, hash16, 16) == 0) {
                if (ke->flag == 1) c->kf_lookup_failed++;
                break;
            }
        }
        kfcache_release(&kh);
        return 0;
    }
    if ((int)cur_sid != c->stream_id || (uint32_t)cur_fid != c->donor_fid) {
        /* Already repointed elsewhere (legitimate orphan). */
        kfcache_release(&kh);
        return 0;
    }

    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                        (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    return 0;
```

---

## Task 4 — Add `kf_lookup_failed` to `CompactOdCtx` (fixed format)

**File**: `src/db/slotcask.c`

**Anchor**:

```c
} CompactOdCtx;
```

**Replace** the entire struct (from `/* Context for compact_od_cb */` through `} CompactOdCtx;`) with:

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
    uint32_t    kf_lookup_failed; /* live records where kf lookup returned -1 */
} CompactOdCtx;
```

---

## Task 5 — Track kf_lookup_failed in `compact_od_cb` (fixed format)

**File**: `src/db/slotcask.c`

**Anchor** (inside `compact_od_cb`):

```c
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
```

**Replace** with:

```c
    int lr = kf_lookup_with_slot(&kh, hash16, key, klen, c->db->data_dir,
                                  &cur_flag, &cur_sid, &cur_fid,
                                  &cur_off, &kf_slot_idx);
    if (lr != 0) {
        size_t cap = kh.capacity;
        size_t kstart = kf_slot_for(hash16, cap);
        for (size_t ki = 0; ki < cap; ki++) {
            size_t kslot = (kstart + ki) % cap;
            SlotcaskKfEntry *ke = &kh.map[kslot];
            if (ke->flag == 0) break;
            if (memcmp(ke->hash, hash16, 16) == 0) {
                if (ke->flag == 1) c->kf_lookup_failed++;
                break;
            }
        }
        kfcache_release(&kh);
        c->free_idx++;
        return 0;
    }
    if ((int)cur_sid != c->stream_id || (uint32_t)cur_fid != c->donor_fid) {
        /* Already repointed elsewhere (legitimate orphan). */
        kfcache_release(&kh);
        c->free_idx++;
        return 0;
    }
```

---

## Task 6 — Guard donor delete in `compact_one_stream_varlen`

Add an `out_kf_failed` out-pointer to `compact_migrate_records_varlen`, then use it to
gate the donor delete at the call site.

**File**: `src/db/slotcask.c`

**Anchor** (function signature):

```c
static int compact_migrate_records_varlen(SlotcaskDb *db, int stream_id,
                                           uint32_t donor_fid,
                                           uint32_t recipient_fid) {
```

**Replace** with:

```c
static int compact_migrate_records_varlen(SlotcaskDb *db, int stream_id,
                                           uint32_t donor_fid,
                                           uint32_t recipient_fid,
                                           uint32_t *out_kf_failed) {
```

**Anchor** (VarlenCompactCtx initialiser in that same function):

```c
    VarlenCompactCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map, .rmap_size = rmap_size,
        .free_offs = free_offs, .free_caps = free_caps,
        .free_count = free_count, .free_next = 0, .rc = 0,
    };
```

**Replace** with:

```c
    VarlenCompactCtx ctx = {
        .db = db, .stream_id = stream_id,
        .donor_fid = donor_fid, .recipient_fid = recipient_fid,
        .rmap = rh.map, .rmap_size = rmap_size,
        .free_offs = free_offs, .free_caps = free_caps,
        .free_count = free_count, .free_next = 0, .rc = 0,
        .kf_lookup_failed = 0,
    };
```

**Anchor** (return statement at bottom of `compact_migrate_records_varlen`):

```c
    free(free_offs);
    free(free_caps);
    segcache_release(&rh);
    return ctx.rc;
}
```

**Replace** with:

```c
    if (out_kf_failed) *out_kf_failed = ctx.kf_lookup_failed;
    free(free_offs);
    free(free_caps);
    segcache_release(&rh);
    return ctx.rc;
}
```

**Anchor** (call site inside `compact_one_stream_varlen`):

```c
        if (recip_free >= files[i].live_count) {
            if (compact_migrate_records_varlen(db, stream_id,
                                                files[i].file_id,
                                                files[j].file_id) == 0) {
                if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                    dropped++;
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
```

**Replace** with:

```c
        if (recip_free >= files[i].live_count) {
            uint32_t kf_failed = 0;
            if (compact_migrate_records_varlen(db, stream_id,
                                                files[i].file_id,
                                                files[j].file_id,
                                                &kf_failed) == 0) {
                /* Only delete donor if every live kf entry that referenced it
                   was successfully repointed.  kf_failed > 0 means at least
                   one live kf entry exists for a record in this donor but
                   verify_stored_key could not reach its backing file — the
                   donor must be preserved so rebuild-kf can recover it. */
                if (kf_failed == 0) {
                    if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                        dropped++;
                }
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
```

---

## Task 7 — ~~Superseded~~ — Skip

Merged into Task 6. Proceed to Task 8.

---

## Task 8 — Same guard for `compact_one_stream` (fixed format)

**File**: `src/db/slotcask.c`

Parallel change to the fixed-format path.

First, add `out_kf_failed` parameter to `compact_migrate_records`:

**Anchor**:

```c
static int compact_migrate_records(SlotcaskDb *db, int stream_id,
                                    uint32_t donor_fid, uint32_t recipient_fid) {
```

**Replace** with:

```c
static int compact_migrate_records(SlotcaskDb *db, int stream_id,
                                    uint32_t donor_fid, uint32_t recipient_fid,
                                    uint32_t *out_kf_failed) {
```

Find the `CompactOdCtx ctx = {` initialiser block inside `compact_migrate_records` and add
`.kf_lookup_failed = 0,` in the same position as VarlenCompactCtx above. Then find the
return statement at the bottom of `compact_migrate_records` and add the out-pointer write
before the return, mirroring the varlen version.

Then update the call site in `compact_one_stream`:

**Anchor**:

```c
        if (recip_free >= files[i].live_count) {
            if (compact_migrate_records(db, stream_id,
                                          files[i].file_id, files[j].file_id) == 0) {
                if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                    dropped++;
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
```

**Replace** with:

```c
        if (recip_free >= files[i].live_count) {
            uint32_t kf_failed = 0;
            if (compact_migrate_records(db, stream_id,
                                          files[i].file_id, files[j].file_id,
                                          &kf_failed) == 0) {
                if (kf_failed == 0) {
                    if (compact_drop_seg_file(db, stream_id, files[i].file_id) == 0)
                        dropped++;
                }
                files[j].live_count += files[i].live_count;
            }
            i++;
        } else {
```

---

## Task 9 — Implement `slotcask_rebuild_kf` in `slotcask.c`

Insert the new function immediately before the anchor:

**Anchor** (find this text to locate insertion point):

```c
/* Public entry point. Caller must hold objlock_wrlock for the object. */
int slotcask_compact_segs(SlotcaskDb *db, int *out_dropped) {
```

**Insert before** (paste the full function immediately above that line):

```c
/* File-id comparator for qsort (ascending). */
static int cmp_fid_asc(const void *a, const void *b) {
    uint32_t x = *(const uint32_t *)a, y = *(const uint32_t *)b;
    return (x > y) - (x < y);
}

/* Rebuild the kf for every live segment record.
 *
 * Scans ALL segment files for all streams in ascending file_id order.
 * For each flag=1 (live) record, probes the corresponding kf shard by
 * hash alone (no segment re-read) and repoints the entry to the current
 * (stream_id, file_id, offset).  Processing in ascending file_id order
 * ensures the final kf entry for each key points to the highest-file_id
 * occurrence, which is the most recent version.
 *
 * Invariants:
 *   - Caller must hold the per-object write lock (objlock_wrlock).
 *   - Skips kf entries with flag=2 (tombstone) — deleted keys stay deleted.
 *   - Skips keys not found in kf (no ghost insertions).
 *   - After scan, invalidates kfcache for the object so callers see new pointers.
 *
 * Returns the number of kf entries actually updated (changed to a different
 * stream_id/file_id/offset).  Returns -1 on a fatal allocation error.
 */
int slotcask_rebuild_kf(SlotcaskDb *db) {
    if (!db) return 0;
    int total_repaired = 0;

    for (int s = 0; s < db->num_streams; s++) {
        char stream_dir[PATH_MAX];
        stream_dir_for(stream_dir, db->data_dir, s);

        DIR *dh = opendir(stream_dir);
        if (!dh) continue;

        uint32_t *fids = NULL;
        size_t nfids = 0, fcap = 0;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            if (de->d_name[0] == '.') continue;
            size_t nlen = strlen(de->d_name);
            if (nlen != 10 || strcmp(de->d_name + 6, ".dat") != 0) continue;
            uint32_t fid = (uint32_t)strtoul(de->d_name, NULL, 10);
            if (nfids == fcap) {
                size_t nc = fcap ? fcap * 2 : 16;
                uint32_t *t = realloc(fids, nc * sizeof(uint32_t));
                if (!t) { free(fids); closedir(dh); return -1; }
                fids = t; fcap = nc;
            }
            fids[nfids++] = fid;
        }
        closedir(dh);
        if (nfids == 0) { free(fids); continue; }

        qsort(fids, nfids, sizeof(uint32_t), cmp_fid_asc);

        for (size_t fi = 0; fi < nfids; fi++) {
            char seg_path[PATH_MAX];
            seg_path_for(seg_path, db->data_dir, s, fids[fi]);
            SlotcaskSegHandle sh;
            if (segcache_acquire(&sh, seg_path, 0, 0) != 0) continue;

            size_t off = 0;
            size_t file_size = sh.map_size;

            while (off + 24 <= file_size) {
                const uint8_t *rec = sh.map + off;
                uint8_t  flag = rec[18];
                uint16_t klen;
                uint32_t vlen;
                memcpy(&klen, rec + 16, 2);
                memcpy(&vlen, rec + 20, 4);

                size_t rec_size;
                if (db->format == SLOTCASK_FORMAT_VARIABLE) {
                    rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                } else {
                    rec_size = (size_t)db->slot_size;
                }
                if (off + rec_size > file_size) break;

                if (flag == 1) {
                    const uint8_t *hash16 = rec;
                    int kfshard = shard_for_hash(hash16, db->num_shards);
                    char kfp[PATH_MAX];
                    kf_path_for(kfp, db->data_dir, kfshard);
                    SlotcaskKfHandle kh;
                    if (kfcache_acquire(&kh, kfp, db->slots_per_shard, 1) == 0) {
                        /* Match kf entries by hash only — no key re-fetch from
                           segment.  xxh128 collisions are negligible; a segment
                           record with a corrupted hash field could theoretically
                           repoint an unrelated live entry.  Acceptable: this is
                           a recovery-only operation invoked explicitly by an
                           operator. */
                        size_t cap = kh.capacity;
                        size_t kstart = kf_slot_for(hash16, cap);
                        for (size_t ki = 0; ki < cap; ki++) {
                            size_t kslot = (kstart + ki) % cap;
                            SlotcaskKfEntry *ke = &kh.map[kslot];
                            if (ke->flag == 0) break; /* probe chain end */
                            if (memcmp(ke->hash, hash16, 16) != 0) continue;
                            if (ke->flag == 1) {
                                /* Only update if location differs. */
                                if (ke->stream_id != (uint8_t)s ||
                                    ke->file_id   != (uint16_t)fids[fi] ||
                                    ke->offset    != (uint32_t)off) {
                                    kf_repoint_at_slot(&kh, kslot,
                                                        (uint8_t)s,
                                                        (uint16_t)fids[fi],
                                                        (uint32_t)off);
                                    total_repaired++;
                                }
                            }
                            /* flag=2: deleted key — leave tombstone intact. */
                            break;
                        }
                        kfcache_release(&kh);
                    }
                }

                off += rec_size;
            }

            segcache_release(&sh);
        }

        free(fids);
    }

    /* Flush kf cache so subsequent reads pick up the new pointers. */
    kfcache_invalidate_prefix(db->data_dir);

    return total_repaired;
}
```

---

## Task 10 — Declare `slotcask_rebuild_kf` in `slotcask.h`

**File**: `src/db/slotcask.h`

**Anchor** (find after the slotcask_compact_segs declaration):

```c
int slotcask_compact_segs(SlotcaskDb *db, int *out_dropped);
```

**Insert after**:

```c
/* Rebuild kf from segment scan.  Caller holds objlock_wrlock.
   Returns number of entries repaired, or -1 on fatal error. */
int slotcask_rebuild_kf(SlotcaskDb *db);
```

---

## Task 11 — Implement `cmd_rebuild_kf` in `query.c`

**File**: `src/db/query.c`

**Anchor** (find this function to locate insertion point):

```c
int cmd_recount(const char *db_root, const char *object) {
```

**Insert before** that line:

```c
int cmd_rebuild_kf(const char *db_root, const char *object) {
    Schema sch = load_schema(db_root, object);
    if (!sch.splits) { OUT("{\"error\":\"object not found\"}\n"); return 1; }
    SlotcaskSchemaInfo info = {
        .splits = sch.splits, .slot_size = sch.slot_size,
        .streams = sch.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"object not open\"}\n"); return 1; }
    int repaired = slotcask_rebuild_kf(sdb);
    if (repaired < 0) { OUT("{\"error\":\"rebuild-kf failed (oom)\"}\n"); return 1; }
    OUT("{\"status\":\"ok\",\"repaired\":%d}\n", repaired);
    return 0;
}

```

---

## Task 12 — Declare `cmd_rebuild_kf` in `types.h`

**File**: `src/db/types.h`

**Anchor** (find this line):

```c
int cmd_recount(const char *db_root, const char *object);
```

**Insert after**:

```c
int cmd_rebuild_kf(const char *db_root, const char *object);
```

---

## Task 13 — Wire `rebuild-kf` in `server.c` (JSON mode)

**File**: `src/db/server.c`

**Anchor** (find this block in the JSON dispatch section):

```c
    } else if (strcmp(mode, "recount") == 0) {
        cmd_recount(db_root, object);
```

**Insert before** that line:

```c
    } else if (strcmp(mode, "rebuild-kf") == 0) {
        cmd_rebuild_kf(db_root, object);
```

---

## Task 14 — Wire `rebuild-kf` in `server.c` (CLI mode)

**File**: `src/db/server.c`

**Anchor** (find this block in the CLI dispatch section):

```c
    } else if (strcasecmp(cmd, "recount") == 0) {
        cmd_recount(eff_root, object);
```

**Insert before** that line:

```c
    } else if (strcasecmp(cmd, "rebuild-kf") == 0) {
        cmd_rebuild_kf(eff_root, object);
```

Also add `"rebuild-kf"` to the write-mode list so the server grants proper access.

**Anchor** (the write-mode string array that includes "vacuum"):

```c
        "truncate", "vacuum", "backup", "recount",
```

**Replace** with:

```c
        "truncate", "vacuum", "backup", "recount", "rebuild-kf",
```

---

## Task 15 — Write test `test-rebuild-kf`

**File**: `src/test/cases/test_rebuild_kf.c`  (new file)

Follow the pattern from `test_vacuum_streams_mismatch.c` exactly: `static int fn(void)`
returning int, `TestEnv`/`TestClient` setup, `tc_request` for all queries, `ASSERT_EQ_INT`
/ `ASSERT_TRUE` / `ASSERT_CONTAINS` / `ASSERT_NOT_NULL` for assertions, `test_env_kill` +
`test_env_start_at` for the restart cycle.

Full implementation:

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <limits.h>

/* Corrupt kf shard 0 of testdir/rebuildtest by setting file_id=0xFFFF for
   the first `n` live entries found.  Returns the number corrupted. */
static int corrupt_kf_entries(const char *db_root, int n) {
    char kfp[PATH_MAX];
    snprintf(kfp, sizeof(kfp), "%s/testdir/rebuildtest/data/kf/000.kf", db_root);
    int fd = open(kfp, O_RDWR);
    if (fd < 0) return 0;
    struct stat st;
    fstat(fd, &st);
    uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (m == MAP_FAILED) return 0;

    /* Header is 24 bytes; each entry is 24 bytes.
       Entry layout: hash[16] flag[1] stream_id[1] file_id[2 LE] offset[4] */
    int corrupted = 0;
    size_t cap = ((size_t)st.st_size - 24) / 24;
    for (size_t i = 0; i < cap && corrupted < n; i++) {
        uint8_t *e = m + 24 + i * 24;
        if (e[16] == 1) { /* flag=1 (live) */
            e[18] = 0xFF; /* file_id low byte  */
            e[19] = 0xFF; /* file_id high byte */
            corrupted++;
        }
    }
    msync(m, (size_t)st.st_size, MS_SYNC);
    munmap(m, (size_t)st.st_size);
    return corrupted;
}

static int test_rebuild_kf_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int saved_port = env.port;
    char saved_db_root[256];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"testdir\"}", &resp);
    free(resp); resp = NULL;

    /* Create VARIABLE-format object. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"testdir\",\"object\":\"rebuildtest\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[{\"name\":\"karma\",\"type\":\"int\"},"
                    "{\"name\":\"username\",\"type\":\"varchar\",\"size\":32}]}",
        &resp);
    free(resp); resp = NULL;

    /* Insert 200 records. */
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"testdir\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\",\"value\":{\"karma\":%d,\"username\":\"u%d\"}}",
            i, i * 10, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Confirm 200 records via bare-integer count response. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"testdir\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_EQ_INT(resp ? atoi(resp) : -1, 200, "200 records after insert");
    free(resp); resp = NULL;

    tc_close(tc);

    /* Corrupt 50 kf entries directly on disk (daemon not running). */
    int c = corrupt_kf_entries(env.db_root, 50);
    ASSERT_TRUE(c > 0, "at least 1 kf entry corrupted");

    /* Kill daemon so kf is re-read from disk on restart. */
    test_env_kill(&env);

    if (test_env_start_at(&env, saved_db_root, saved_port) != 0) return 1;
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after restart");
    if (!tc) { test_env_stop(&env); return 1; }

    /* At least some gets should fail — corrupted file_id=0xFFFF causes
       verify_stored_key to fail to open the segment. */
    int errors = 0;
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"testdir\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && strstr(resp, "\"error\"")) errors++;
        free(resp); resp = NULL;
    }
    ASSERT_TRUE(errors > 0, "some gets fail after kf corruption");

    /* Run rebuild-kf. */
    tc_request(tc,
        "{\"mode\":\"rebuild-kf\",\"dir\":\"testdir\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"ok\"", "rebuild-kf returns ok");
    /* Parse repaired count: find "repaired": and atoi the number after it. */
    const char *rp = resp ? strstr(resp, "\"repaired\":") : NULL;
    ASSERT_TRUE(rp && atoi(rp + 11) > 0, "repaired > 0");
    free(resp); resp = NULL;

    /* All 200 records should now be accessible. */
    int ok = 0;
    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"get\",\"dir\":\"testdir\",\"object\":\"rebuildtest\","
            "\"key\":\"user%04d\"}", i);
        tc_request(tc, req, &resp);
        if (resp && strstr(resp, "\"karma\"")) ok++;
        free(resp); resp = NULL;
    }
    ASSERT_EQ_INT(ok, 200, "all 200 records readable after rebuild-kf");

    /* Idempotency: second rebuild-kf must report repaired=0. */
    tc_request(tc,
        "{\"mode\":\"rebuild-kf\",\"dir\":\"testdir\",\"object\":\"rebuildtest\"}",
        &resp);
    ASSERT_CONTAINS(resp, "\"repaired\":0", "second rebuild-kf is idempotent");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-rebuild-kf", test_rebuild_kf_run)
```

Register the test by adding `test_rebuild_kf.c` to the test build.  In `build.sh`, find
the existing test file list and add the new file alongside `test_vacuum_streams_mismatch.c`.

---

## Task 16 — Build and run all tests

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

If `test-rebuild-kf` fails due to missing harness helpers (`sdb_get_errors`,
`sdb_restart`, `json_int`), look at how existing tests do it and adapt — do not invent
helper names not present in the harness.

---

## Task 17 (separate repo) — Fix JS `some_records_dropped` tolerance in shard-db-hn-explorer

In the HN explorer source (not this repo), locate the refresh-cache server file.  The
production build was patched in-place; the source fix must mirror it:

**Pattern to find** (three instances, one each for stories, comments, users):

```javascript
if (isError(r)) throw new Error(`
```

**Replace each** with:

```javascript
if (isError(r) && r.error !== "some_records_dropped") throw new Error(`
```

The error message body should be preserved (the production patch accidentally dropped it
via bash template-literal interpolation).  The correct form is:

```javascript
if (isError(r) && r.error !== "some_records_dropped")
    throw new Error(`<object> bulk-insert: ${r.error}`);
```

This is a sveltekit/bun project — rebuild locally (`bun run build`) and ship artifacts to
production after the shard-db binary is updated and `rebuild-kf` has been run on all
affected objects.

---

## Post-fix recovery runbook (after deploying new binary)

```bash
# On the production server, stop the daemon
./shard-db stop

# Copy new binary
scp build/bin/shard-db root@152.53.131.43:/opt/shard-db/shard-db

# Start the daemon
./shard-db start

# Run rebuild-kf on affected objects (users, comments, stories — do all)
./shard-db query '{"mode":"rebuild-kf","dir":"hn","object":"users"}'
./shard-db query '{"mode":"rebuild-kf","dir":"hn","object":"comments"}'
./shard-db query '{"mode":"rebuild-kf","dir":"hn","object":"stories"}'

# Verify counts are sane
./shard-db count hn users
./shard-db count hn comments
./shard-db count hn stories
```

Run locally first on `./db/hn/` before touching production.
