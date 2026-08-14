# Fix: recover the active-segment append frontier across a mid-file free-pool gap

Status: **revised execution-ready plan; awaiting human approval.** This
revision supersedes the stopped execution recorded in `PLAN_NOTES.md`.

## Goal and scope

Fix only the startup-recovery path for an active variable-length segment. A
restart must reconstruct `reserve_off` after every validated record, even when
a zero-filled free-pool gap precedes a later record. This prevents subsequent
appends from overwriting that later record.

This plan does not persist a cursor, write a synthetic segment header/flag,
change the on-disk format, or alter normal write allocation. It replaces the
startup non-active-file tombstone sweep with a KF-driven, per-candidate record
validation pass; it does not change any query or maintenance scanner.

## Root cause and invariants

`pool_split_leftover` writes a zero-filled leftover span after a smaller record
reuses a larger tombstoned slot. The active-file loop in `recover_one_stream`
currently stops at the first `flag == 0` byte. If a live record follows that
gap, it records the gap's start as `reserve_off`; the next post-restart append
eventually overwrites the later record.

The fix must preserve these invariants:

- A valid segment record has `flag ∈ {1,2}`, a footprint no larger than
  `db->slot_size`, an in-bounds header/payload, and a hash matching its key.
- A flag-zero region is an interior gap only when a validated record appears
  within the next `db->slot_size` bytes. Otherwise it is the unwritten tail
  and recovery stops at the final validated record.
- The free pool is rebuilt only from a KF tombstone whose current segment
  record is also a validated tombstone with the same hash. A KF tombstone is a
  deleted-key hash-table entry, not proof that its historical segment address
  remains free after slot reuse.
- Free-pool capacity always comes from the current validated segment record,
  never `db->slot_size`.
- A malformed/nonzero or hash-invalid candidate with no later validated record
  is on-disk corruption, not a tail: fail `slotcask_open` with `EUCLEAN`.
- No new bytes are written during recovery and no segment format changes.

`seg_scan_varlen_struct_ok`, `seg_scan_varlen_hash_ok`, and
`seg_scan_varlen_resync` in `src/db/seg_scan_varlen.h` already implement the
required bounded validation/resync contract. The whole active file is mmap'd,
so the helper can be called directly; no copy of the scanner is needed.

## Call sites / consumers

Production changes are `recover_one_stream` (frontier only),
`slotcask_pool_rebuild_worker` (validated pool entries), and the startup call
site that currently runs `recover_scan_tombstones_od` for old files. Their
consumer is `slotcask_open`. The shared scanner helpers and all wire, CLI,
public C API, and on-disk formats remain unchanged.

## Task 1 — Add the regression test first

**File:** `src/test/cases/test_slotcask_active_recovery_resync.c`

The test opens one one-stream slotcask directly, which is deliberate:
`slotcask_close` followed by `slotcask_open` invokes the exact production
startup-recovery path without server timing, a fixed port, or process-global
test state. It builds this deterministic layout in the active file:

1. Insert A (5000 bytes), then C (1 byte), so C is physically after A.
2. Delete A, then insert smaller B (4000 bytes). Both sizes use the unbounded
   pool bucket, so B reuses A and `pool_split_leftover` leaves an interior gap
   before C.
3. Close and reopen. On the base revision, recovery stops at the gap and
   `reserve_off` is 1032 bytes early.
4. Insert D (5000 bytes), which crosses the old gap. On the base revision this
   clobbers C; after the fix C remains readable and byte-identical.

Create the complete file below at the quoted insertion anchor
`src/test/cases/test_slotcask_v2_crash.c \\` in `build.sh`, immediately after
that existing entry. The runner discovers `TEST_REGISTER` cases through static
initialization, but `build.sh` maintains the test source list explicitly.

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static size_t rec_size(size_t klen, size_t vlen) {
    return (24u + klen + vlen + 7u) & ~(size_t)7u;
}

static int read_is(SlotcaskDb *db, const char *key,
                   const void *want, size_t want_len) {
    void *got = NULL;
    size_t got_len = 0;
    int rc = slotcask_get(db, key, strlen(key), &got, &got_len);
    int ok = rc == 0 && got_len == want_len &&
             memcmp(got, want, want_len) == 0;
    free(got);
    return ok;
}

static int test_slotcask_active_recovery_resync_run(void) {
    char dir[256];
    snprintf(dir, sizeof(dir), "/tmp/shard-db-active-recovery-%d", (int)getpid());
    char rmcmd[320];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", dir);
    system(rmcmd);
    mkdir(dir, 0755);

    char a[5000], b[4000], d[5000];
    memset(a, 'A', sizeof(a));
    memset(b, 'B', sizeof(b));
    memset(d, 'D', sizeof(d));

    slotcask_init(64, 64);
    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, dir, 8, 1, 8192), 0, "open fixture db");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "a", 1, a, sizeof(a)), 0,
                  "insert large donor A");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "c", 1, "C", 1), 0,
                  "insert live C after donor A");
    ASSERT_EQ_INT(slotcask_delete(&db, "a", 1), 0, "delete donor A");
    ASSERT_EQ_INT(slotcask_insert(&db, 0, "b", 1, b, sizeof(b)), 0,
                  "reuse A with smaller B and create an interior gap");

    const size_t c_end = rec_size(1, sizeof(a)) + rec_size(1, 1);
    ASSERT_TRUE(db.streams[0].reserve_off == c_end,
                "pre-restart append frontier is after C");
    slotcask_close(&db);

    memset(&db, 0, sizeof(db));
    ASSERT_EQ_INT(slotcask_open(&db, dir, 8, 1, 8192), 0,
                  "reopen runs active-file recovery");
    ASSERT_TRUE(db.streams[0].reserve_off == c_end,
                "recovery restores append frontier after C, not at the gap");
    ASSERT_TRUE(read_is(&db, "c", "C", 1), "C survives the restart");

    ASSERT_EQ_INT(slotcask_insert(&db, 0, "d", 1, d, sizeof(d)), 0,
                  "post-restart append crosses the old gap");
    ASSERT_TRUE(read_is(&db, "c", "C", 1),
                "post-restart append does not overwrite C");
    ASSERT_TRUE(read_is(&db, "b", b, sizeof(b)), "B remains intact");
    ASSERT_TRUE(read_is(&db, "d", d, sizeof(d)), "D is readable");

    slotcask_close(&db);
    slotcask_shutdown();
    system(rmcmd);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-slotcask-active-recovery-resync",
              test_slotcask_active_recovery_resync_run)
```

Run this exact test on the base revision and paste its failure. It must fail
at the recovered-frontier assertion and the C-after-D assertion. After Task 2
alone, the frontier assertion must pass while C-after-D still fails: that is
the proof that the stale-KF-tombstone defect is independent. After Task 3,
all assertions must pass. Do not change an assertion to accommodate an
intermediate or base behavior.

```bash
./build/bin/shard-db-test run test-slotcask-active-recovery-resync
```

## Task 2 — Use the shared bounded resync helper for the active frontier only

**File:** `src/db/slotcask.c`

Replace the complete active-file walk beginning at the quoted anchor:

```c
        off_t pos = 0;
        off_t lim = (off_t)h.map_size;
        while (pos + 24 <= lim) {
```

and ending immediately before:

```c
        last_offset = pos;
```

with this complete code block:

```c
        size_t pos = 0;
        size_t lim = h.map_size;
        while (pos + 24 <= lim) {
            size_t rec_size;
            uint8_t flag;
            uint16_t klen;
            uint32_t vlen;
            int valid = seg_scan_varlen_struct_ok(h.map, lim, pos,
                                                   (size_t)db->slot_size,
                                                   &rec_size, &flag,
                                                   &klen, &vlen);
            if (valid && flag != 0)
                valid = seg_scan_varlen_hash_ok(h.map, pos, klen);

            if (!valid || flag == 0) {
                size_t next;
                if (seg_scan_varlen_resync(h.map, lim, pos,
                                            (size_t)db->slot_size,
                                            (size_t)db->slot_size, &next)) {
                    pos = next;
                    continue;
                }
                if (valid && flag == 0)
                    break; /* ordinary unwritten tail */
                segcache_release(&h);
                free(ids);
                errno = EUCLEAN;
                return -1;
            }

            pos += rec_size;
        }
```

Also replace this single assignment at its quoted anchor:

```c
        last_offset = pos;
```

with:

```c
        last_offset = (off_t)pos;
```

Why this is safe:

- The helper only resumes at an 8-byte-aligned, bounds-checked, hash-verified
  live/tombstone record; zero bytes cannot be mistaken for a record.
- The resync window is exactly `db->slot_size`, the maximum possible capacity
  of a split leftover; a later record beyond that distance is not a valid
  consequence of this allocator's gap format.
- A pure zero tail still stops recovery, retaining the existing append-tail
  behavior. Nonzero malformed data instead fails closed with `EUCLEAN`.
- Pool reconstruction is deliberately absent from this loop; Task 3 is the
  only startup source of free-pool entries, preventing duplicates and making
  the physical-validation rule uniform for active and rotated files.

## Task 3 — Rebuild the free pool from validated KF candidates

**File:** `src/db/slotcask.c`

The current `slotcask_pool_rebuild_worker` is unsafe because it pushes every
`flag == 2` KF entry with capacity `db->slot_size`. Replace its complete body,
from the quoted anchor `static void *slotcask_pool_rebuild_worker(void *raw) {`
through its closing `}`, with a two-phase worker:

```c
static void *slotcask_pool_rebuild_worker(void *raw) {
    SlotcaskOpenArg *a = (SlotcaskOpenArg *)raw;
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, a->db->data_dir, a->shard_id);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, a->db->slots_per_shard, 0) != 0)
        return NULL;

    size_t count = 0;
    for (size_t i = 0; i < kh.capacity; i++)
        if (kh.map[i].flag == 2 && kh.map[i].stream_id < a->db->num_streams)
            count++;
    SlotcaskKfEntry *candidates = calloc(count, sizeof(*candidates));
    if (!candidates && count != 0) { kfcache_release(&kh); return NULL; }
    size_t n = 0;
    for (size_t i = 0; i < kh.capacity; i++) {
        if (kh.map[i].flag == 2 && kh.map[i].stream_id < a->db->num_streams)
            candidates[n++] = kh.map[i];
    }
    kfcache_release(&kh);

    for (size_t i = 0; i < n; i++) {
        SlotcaskKfEntry *e = &candidates[i];
        char path[PATH_MAX];
        seg_path_for(path, a->db->data_dir, e->stream_id, e->file_id);
        SlotcaskSegHandle h;
        if (segcache_acquire(&h, path, 0, 0, 0) != 0) continue;
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int valid = seg_scan_varlen_struct_ok(h.map, h.map_size, e->offset,
                                               (size_t)a->db->slot_size,
                                               &rec_size, &flag, &klen, &vlen);
        if (valid && flag == 2)
            valid = memcmp(h.map + e->offset, e->hash, sizeof(e->hash)) == 0 &&
                    seg_scan_varlen_hash_ok(h.map, e->offset, klen);
        if (valid && flag == 2)
            pool_push_free_cap(&a->db->streams[e->stream_id], e->file_id,
                               e->offset, (uint32_t)rec_size,
                               a->db->slot_size);
        segcache_release(&h);
    }
    free(candidates);
    return NULL;
}
```

The candidate copy is required: release the KF rdlock before any segment-cache
acquire, so no worker holds a KF lock while taking a segment lock. Existing
`parallel_for_io` continues to run workers per KF shard concurrently.

In `recover_one_stream`, replace this old-file branch at the quoted anchor:

```c
        if (file_id != last_id) {
            (void)recover_scan_tombstones_od(db, sid, file_id, path);
            continue;
        }
```

with:

```c
        if (file_id != last_id)
            continue;
```

After this edit, search for `recover_scan_tombstones_od`. If no call sites
remain, delete its complete static definition beginning at the quoted anchor
`static int recover_scan_tombstones_od(SlotcaskDb *db, int sid,` and ending
immediately before the quoted anchor `/* Walk every segment for a single
stream, populate the in-memory free-slot`. If any call site remains, stop and
add it to `PLAN_NOTES.md` for a planning decision; do not alter that caller.

## Task 4 — Prove both regressions and run required validation

After Tasks 2–3, rebuild and run the regression test. Then temporarily restore
the exact old loop from the Task 2 anchor, rebuild, and rerun the same test;
paste the expected failure. Reapply the Task 2 block, rebuild again, and paste
the passing output. This is the required proof that the test detects this
specific fix rather than merely passing incidentally.

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-slotcask-active-recovery-resync
./build/bin/shard-db-test run test-variable-length
./build/bin/shard-db-test run test-varlen-scan-resync
./build/bin/shard-db-test run-all
```

The diff changes startup recovery, cached segment access, and stream state;
run both local dynamic-safety gates before handing it to review:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run test-slotcask-active-recovery-resync
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run test-slotcask-active-recovery-resync
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

If a quoted anchor is absent, write `PLAN_NOTES.md` describing the mismatch
and halt the complete execution run. If an uncovered design decision appears,
stop and ask the human; do not improvise. Leave all resulting work uncommitted
for raw-diff review.

## Expected changed files

- `src/db/slotcask.c` — active frontier and validated pool recovery.
- `src/test/cases/test_slotcask_active_recovery_resync.c` — regression test.
- `build.sh` — compile the new registered test case.
- This plan document.
