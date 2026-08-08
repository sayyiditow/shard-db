# VARLEN segment-scan resync: fix silent desync after slot reuse

## Status

Revision 6. Rewritten in full to address every blocker and required
change from the review of revision 1 (see "Review history" at the
bottom). The diagnostics/logging refactor already on `main`
(process-global log handler for embedded mode) is unrelated prior work
and out of scope here — this plan only adds new `LOG_ERROR`/`LOG_WARN`/
`LOG_INFO` call sites using that existing infrastructure.

## Execution rules

- Branch off `main` into a fresh feature branch `fix/varlen-scan-resync`
  before starting Task 0. Run `git status` first. The code worktree must
  not have unrelated staged, modified, or untracked source/build changes;
  stop and ask if it does. The intentionally uncommitted Markdown files
  under `docs/plans/` are planning artifacts and are allowed to remain
  uncommitted throughout execution; preserve them and include them in
  the final review rather than treating them as a dirty-code-worktree
  violation.
- Do tasks in order; each task's code and test are self-contained, but
  Task 2 depends on Task 0's header, and Tasks 3/4a/4b depend on Task 0's
  header too.
- Build with `SKIP_TESTS=1 ./build.sh`; test with
  `./build/bin/shard-db-test run[-all]` (per this repo's AGENTS.md).
- If a quoted anchor in this plan is not found **exactly** in the target
  file, do not guess, reinterpret, or patch around it. Write
  `PLAN_NOTES.md` at the repo root describing the exact mismatch (file,
  expected anchor text, what's actually there) and halt the entire
  execution run immediately — do not continue to any further task, even
  an unrelated one. Resuming requires a human (or the planning model,
  re-engaged) to read `PLAN_NOTES.md` and hand back either a patched or a
  fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
- Per this repo's AGENTS.md standing exception: leave all work
  **uncommitted** when execution finishes. A reviewing agent and the
  human review the raw `git diff` before anything is committed.
- Per this repo's AGENTS.md standing exception: this diff touches
  shared/cached state (segcache, kfcache) and object-lifetime-adjacent
  scan paths, plus a background prefetch thread's `DbCtx` lifetime — run
  the full ASan+UBSan and TSan gates locally before calling any task
  done (exact commands in Task 6). Do not defer to CI.
- Never weaken a test to make it pass, never quietly rerun-until-green a
  flaky result — a test that fails then passes on rerun is a confirmed
  bug until root-caused.

## Root cause

VARLEN segment records have no stored "next record offset" — a scanner
computes each record's size from its own header
(`24 + klen + vlen`, rounded to 8) and advances by that amount. This
works as long as every byte between one record's start and the next
record's start belongs to that first record.

It doesn't, after slot reuse. `slotcask_insert()`'s VARIABLE-format pool-
reuse branch (`src/db/slotcask.c`, the branch that calls
`pool_try_pop_for_size`) passes the **freed slot's full original
capacity** — not the new record's own natural size — as the `rec_size`
argument into `seg_write_record_varlen()`, which forwards it unchanged
into `seg_record_emit()`'s `slot_size` parameter. `seg_record_emit()`
zero-pads (`memset(dst + used, 0, slot_size - used)`) out to that full
old capacity. So when a small record reuses a large freed slot, the
entire gap between the new record's natural end and the old slot's
capacity boundary is genuinely, deterministically zero-filled — real
API behavior, not corruption.

The write-side invariant is explicit: `slotcask_insert()` rejects any
record whose `24 + klen + vlen` exceeds `db->slot_size`, and the free
pool stores capacities no larger than that same object-level maximum.
Therefore the largest possible resync distance is the current object's
cached `db->slot_size`, not `SLOTCASK_SEG_MAX_BYTES`, which is only the
total sparse segment-file capacity.

A scanner that advances by the *new* record's own natural size lands
inside that zero-filled gap instead of at the next real record's header.
Live production hex-dumps confirm this: the bytes on disk are correct
zero padding, not garbage — this is a read-side scanner bug, not
on-disk corruption.

Two scanners are affected:

- `seg_scan_o_direct_varlen()` (`src/db/io_direct.c`) hits a
  structurally-implausible header (giant `vlen`, since a run of zero
  bytes decodes as `klen=0, vlen=0, flag=0` mostly, but the two zero
  bytes straddling into whatever follows the gap can decode as garbage)
  and today aborts the whole scan with `-EIO` after printing a temporary
  diagnostic — a crash-loop in any code path that can't tolerate a
  failed scan.
- `seg_stat_one_varlen()` (`src/db/slotcask.c`, used by
  `compact_one_stream_varlen()`) hits the same desync but has no bounds
  check on `flag`/`rec_size` at all beyond a straddle check — it silently
  `break`s out of its loop and returns `0` (success) with whatever
  `live`/`total` it had accumulated so far. Its caller,
  `compact_one_stream_varlen()`, then treats a file with `live_count == 0`
  as safe to delete — a segment file that still holds live records past
  the desync point is silently deleted. **This is the silent-data-loss
  bug**, not just the crash-loop.

Two lower-frequency maintenance loops inside
`compact_migrate_records_varlen()` (`src/db/slotcask.c`) have the same
class of bug: the recipient free-slot walk treats *any* non-desync-aware
`flag != 1` byte pattern as a legitimate free slot (worse: it doesn't
even check `flag <= 2`, so a desynced garbage flag byte can be added to
the free list and later overwritten with a migrated record, destroying
whatever was actually there), and the donor scan is a raw mmap walk with
zero validation.

### Why not fix this on the write side

An earlier idea was to stash the reused slot's *actual reserved size* in
the record header's currently-unused reserved byte, so a scanner could
always trust its own header to know how far to skip. This doesn't work:
`pool_try_pop_for_size()`'s top free-pool bucket (≥ 8192 bytes) is a
catch-all only up to the object's configured `db->slot_size` (see
`slotcask.h`'s bucket comment). A freed slot's capacity therefore cannot
approach the 128 MiB `SLOTCASK_SEG_MAX_BYTES` segment-file cap unless the
object schema itself permits a record that large. The record-size
invariant is enforced at insert time by `slotcask_insert()` and
`db->slot_size` is already schema-derived and cached on every `SlotcaskDb`.
The resync bound must therefore be that per-object value, not a global
segment-file constant. Stashing the capacity in the currently-unused
header byte would still change the on-disk format for existing records;
a resync-on-demand read-side fix has no format impact or back-compat
migration.

## Task 0: shared VARLEN scan-validation header

### Test-first

There's no standalone unit test for Task 0 — its three helpers are
exercised directly by Task 1's regression test (below), which is the
first task that calls them. Task 0 is infrastructure; Task 1 proves it
works.

### Implementation

Create `src/db/seg_scan_varlen.h`:

```c
/* seg_scan_varlen.h — shared resync/validation helpers for VARLEN
 * segment scanning. Used by the O_DIRECT scanner (io_direct.c) and the
 * mmap-based maintenance walks (slotcask.c: seg_stat_one_varlen,
 * compact_migrate_records_varlen) to recover from reused-slot
 * zero-padding gaps that don't match the scanning record's own natural
 * size. See docs/plans/2026-08-07-varlen-segment-scan-resync.md for the
 * root cause.
 */
#ifndef SHARD_DB_SEG_SCAN_VARLEN_H
#define SHARD_DB_SEG_SCAN_VARLEN_H

#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include "slotcask.h"   /* SlotcaskDb / compute_hash_raw visibility */

/* Every VARLEN record is written 8-byte aligned, on-disk size
   (24 + klen + vlen) rounded up to 8 — must match
   slotcask.c's slotcask_record_size_varlen() / io_direct.c's
   od_varlen_rec_size() exactly (both already carry this same formula
   independently; this is a third deliberate copy, not a refactor, to
   avoid a header depending on either TU's private helper). */
static inline size_t seg_scan_varlen_size(uint16_t klen, uint32_t vlen) {
    size_t raw = 24 + (size_t)klen + (size_t)vlen;
    return (raw + 7) & ~(size_t)7;
}

/* Validate the record header at map[pos]: flag <= 2, header + rec_size
   fits within map_size, and rec_size itself <= max_slot_size. Does NOT
   verify the hash. max_slot_size is the owning object's cached
   SlotcaskDb.slot_size; it is deliberately not the segment file cap.
   Returns 1 and fills *out_rec_size/out_flag/
   out_klen/out_vlen on success; returns 0 on failure (outputs
   unspecified). */
static inline int seg_scan_varlen_struct_ok(const uint8_t *map, size_t map_size,
                                             size_t pos, size_t max_slot_size,
                                             size_t *out_rec_size,
                                             uint8_t *out_flag, uint16_t *out_klen,
                                             uint32_t *out_vlen) {
    if (pos + 24 > map_size) return 0;
    if (max_slot_size < 32) return 0;
    const uint8_t *rec = map + pos;
    uint8_t flag = rec[18];
    if (flag > 2) return 0;
    uint16_t klen;
    uint32_t vlen;
    memcpy(&klen, rec + 16, 2);
    memcpy(&vlen, rec + 20, 4);
    size_t rec_size = seg_scan_varlen_size(klen, vlen);
    if (rec_size > max_slot_size) return 0;
    if (pos + rec_size > map_size) return 0;
    *out_rec_size = rec_size;
    *out_flag = flag;
    *out_klen = klen;
    *out_vlen = vlen;
    return 1;
}

/* Verify the record at map[pos] carries a hash matching its key bytes
   (xxh128). Caller must already have validated the header (klen known
   in-bounds) via seg_scan_varlen_struct_ok. Only meaningful for
   flag in {1, 2} (live/tombstone) — flag == 0 (padding) records have no
   real key content and are never passed here. */
static inline int seg_scan_varlen_hash_ok(const uint8_t *map, size_t pos, uint16_t klen) {
    const uint8_t *rec = map + pos;
    uint8_t computed[16];
    compute_hash_raw((const char *)(rec + 24), (size_t)klen, computed);
    return memcmp(computed, rec, 16) == 0;
}

/* Search forward from `pos` (need not be 8-byte aligned) for the next
   offset within [pos, pos+window] that holds a structurally valid,
   hash-verified flag in {1,2} record header — the only kind of header
   safe to resume scanning from after a desync.

   Search only ever tests 8-byte-aligned candidates (record headers are
   only ever written at 8-byte-aligned offsets), computed by first
   flooring `pos` to the 8-byte grid (`pos & ~7`), not by stepping +8
   from `pos` itself — stepping from an unaligned `pos` would test only
   offsets sharing `pos`'s own residue mod 8, silently skipping the true
   grid.

   flag == 0 (padding / never-written) candidates are always skipped and
   never terminate the search: a zero-filled reuse gap reads back as a
   structurally "valid" flag==0 header (klen=0, vlen=0) at every
   8-byte-aligned offset inside it, so accepting flag==0 as a resync
   target would make resync stop at the first padding byte instead of
   finding the next real record — this was blocker #1 in the prior
   revision of this plan.

   `max_slot_size` is the owning object's cached maximum record capacity;
   `window` is the maximum forward distance to a candidate header and is
   inclusive at the upper bound. Returns 1 and sets *out_off on success (offset relative to `map`,
   i.e. an absolute offset if `map` is the whole file / a
   window-relative offset if `map` is a bounded buffer — caller adds its
   own base). Returns 0 if no valid record is found within the window. */
static inline int seg_scan_varlen_resync(const uint8_t *map, size_t map_size,
                                          size_t pos, size_t max_slot_size,
                                          size_t window, size_t *out_off) {
    if (pos >= map_size) return 0;
    size_t start = pos & ~(size_t)7;
    size_t limit = (window > map_size - pos) ? map_size : pos + window;
    for (size_t cand = start; cand < map_size && cand <= limit; cand += 8) {
        if (cand < pos) continue;
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        if (!seg_scan_varlen_struct_ok(map, map_size, cand, max_slot_size,
                                        &rec_size,
                                        &flag, &klen, &vlen)) {
            continue;
        }
        if (flag == 0) continue;
        if (!seg_scan_varlen_hash_ok(map, cand, klen)) continue;
        *out_off = cand;
        return 1;
    }
    return 0;
}

#endif /* SHARD_DB_SEG_SCAN_VARLEN_H */
```

`compute_hash_raw` is already declared in `src/db/types.h` and every
consumer of this header (`io_direct.c` via `io_direct.h` → `types.h`,
`slotcask.c` directly) already has it visible — no new include wiring
needed.

Add `src/db/seg_scan_varlen.h` to the build's header dependency tracking
if the build system enumerates headers explicitly (check `build.sh` /
any `Makefile`/`CMakeLists.txt` glob — if headers are picked up by glob,
no change needed; if listed explicitly, add this one).

The test runner's case list is also explicit — `build.sh` says that
future cases under `src/test/cases/` must be listed there and does not
glob the directory. Add every new `.c` file from this plan to the
`shard-db-test` source list before running any verification:

```text
src/test/cases/test_varlen_scan_resync.c
src/test/cases/test_varlen_scan_resync_odirect.c
src/test/cases/test_varlen_compact_stat_resync.c
src/test/cases/test_varlen_compact_recipient_resync.c
src/test/cases/test_varlen_compact_donor_resync.c
src/test/cases/test_varlen_compact_donor_preserved_on_desync.c
src/test/cases/test_startup_format_sweep.c
src/test/cases/test_varlen_compact_crash_mid_migration.c
```

`varlen_compact_fixture.h` is included by those tests and is not a
standalone build input. Confirm each new test appears in
`./build/bin/shard-db-test list` after the first build; a missing case
is a plan-execution failure, not a reason to proceed with partial
verification.

## Task 1: `seg_scan_varlen_resync` regression test (proves blocker #1 + alignment fix)

### Test-first

This test must fail against the flawed design described in the prior
revision of this plan (flag==0 accepted as a valid resync target, search
starting unaligned at `pos`) and pass against Task 0's implementation
above. Since Task 0's header doesn't exist yet on `main`, "fails before"
here means: write the test against Task 0's header, then temporarily
patch `seg_scan_varlen_resync` to the flawed prior-revision behavior
(accept flag==0, start the search loop at `pos` unaligned instead of at
`pos & ~7`), confirm the test fails for the expected reason (lands in
the gap instead of at the real record), then restore the fixed version
and confirm it passes. Paste both outputs.

Create `src/test/cases/test_varlen_scan_resync.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "seg_scan_varlen.h"
#include <string.h>
#include <stdint.h>

/* Write a VARLEN record: 24B header (16B hash + 2B klen + 1B flag +
   1B reserved + 4B vlen) + key + value, matching slotcask.c's on-disk
   format exactly. hash is computed via compute_hash_raw over the key,
   matching how a real write would populate it. */
static size_t write_record(uint8_t *buf, uint8_t flag,
                            const char *key, uint16_t klen,
                            const char *val, uint32_t vlen) {
    uint8_t hash[16];
    compute_hash_raw(key, klen, hash);
    memcpy(buf, hash, 16);
    memcpy(buf + 16, &klen, 2);
    buf[18] = flag;
    buf[19] = 0;
    memcpy(buf + 20, &vlen, 4);
    memcpy(buf + 24, key, klen);
    memcpy(buf + 24 + klen, val, vlen);
    size_t natural = 24 + (size_t)klen + (size_t)vlen;
    size_t padded = (natural + 7) & ~(size_t)7;
    memset(buf + natural, 0, padded - natural);
    return padded;
}

/* Reproduces the exact real-world desync shape: record A occupies a
   64-byte capacity slot but only needs 32 bytes naturally (32 bytes of
   real zero-fill gap, non-24-aligned relative to A's own size), then
   record C starts immediately at the old slot's 64-byte capacity
   boundary — a scanner that advances by A's own natural size (32 bytes)
   lands 32 bytes into the gap instead of at C. */
static int test_varlen_scan_resync_run(void) {
    uint8_t buf[512];
    memset(buf, 0, sizeof(buf));

    size_t a_natural = write_record(buf, 1, "ka", 2, "v", 1); /* 24+2+1=27 -> 32 */
    ASSERT_EQ_INT((int)a_natural, 32, "record A natural padded size");

    const size_t A_SLOT_CAPACITY = 64; /* reused freed slot was 64 bytes */
    /* buf[32..64) is already zero from the memset above, matching the
       real seg_record_emit() zero-pad-to-slot_size behavior exactly. */

    size_t c_off = A_SLOT_CAPACITY;
    size_t c_natural = write_record(buf + c_off, 1, "kc", 2, "cc", 2); /* 24+2+2=28 -> 32 */
    ASSERT_EQ_INT((int)c_natural, 32, "record C natural padded size");

    /* A scanner that advances by A's own natural size (32) lands at
       offset 32 — inside the zero-filled gap, not at C (offset 64). */
    ASSERT_TRUE(a_natural < A_SLOT_CAPACITY,
                "A's natural size is smaller than its old slot capacity (the gap exists)");

    size_t desync_off = a_natural; /* where a naive scanner would land: 32 */
    const size_t max_slot_size = A_SLOT_CAPACITY;

    size_t resume_off = 0;
    int found = seg_scan_varlen_resync(buf, sizeof(buf), desync_off,
                                        max_slot_size, max_slot_size,
                                        &resume_off);
    ASSERT_TRUE(found, "resync finds a valid record within the window");
    ASSERT_EQ_INT((int)resume_off, (int)c_off,
                  "resync lands exactly on record C, not on padding inside the gap");

    /* Confirm what it found really is C: struct_ok + hash_ok both pass,
       and it's C's key, not some other offset the padding also happens
       to structurally satisfy. */
    size_t rec_size;
    uint8_t flag;
    uint16_t klen;
    uint32_t vlen;
    int sok = seg_scan_varlen_struct_ok(buf, sizeof(buf), resume_off,
                                         max_slot_size,
                                         &rec_size, &flag, &klen, &vlen);
    ASSERT_TRUE(sok, "resync target is structurally valid");
    ASSERT_EQ_INT((int)flag, 1, "resync target has flag==1 (live)");
    ASSERT_EQ_INT((int)klen, 2, "resync target key length matches C");
    ASSERT_TRUE(memcmp(buf + resume_off + 24, "kc", 2) == 0,
                "resync target key content is C's key");
    ASSERT_TRUE(seg_scan_varlen_hash_ok(buf, resume_off, klen),
                "resync target hash verifies");

    /* Unaligned-pos regression: desync_off + 1 (unaligned) must still
       resync to the same 8-byte-aligned target, proving the search
       floors to the true 8-byte grid instead of stepping from the
       unaligned start (the alignment issue called out in review). */
    size_t resume_off2 = 0;
    int found2 = seg_scan_varlen_resync(buf, sizeof(buf), desync_off + 1,
                                         max_slot_size, max_slot_size,
                                         &resume_off2);
    ASSERT_TRUE(found2, "resync from an unaligned offset still finds a record");
    ASSERT_EQ_INT((int)resume_off2, (int)c_off,
                  "resync from an unaligned start still lands on the true 8-byte-aligned target");

    /* A pure zero region (no record ever follows) must fail, not hang or
       falsely accept the first flag==0 header as a target. */
    uint8_t all_zero[256];
    memset(all_zero, 0, sizeof(all_zero));
    size_t unused;
    int found3 = seg_scan_varlen_resync(all_zero, sizeof(all_zero), 0,
                                         max_slot_size, sizeof(all_zero),
                                         &unused);
    ASSERT_TRUE(!found3, "resync over pure padding with no real record finds nothing");

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-scan-resync", test_varlen_scan_resync_run)
```

### Verification (paste both outputs)

1. Temporarily edit `seg_scan_varlen_resync` in `src/db/seg_scan_varlen.h`
   to the flawed prior-revision behavior: remove the `if (flag == 0)
   continue;` line, and change `size_t start = pos & ~(size_t)7;` to
   `size_t start = pos;`. Build and run:
   `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-varlen-scan-resync`
   — confirm it fails, and that the failure is on the "lands exactly on
   record C" or the unaligned-search assertion (not a build error).
2. Revert the temporary edit. Rebuild and rerun the same command —
   confirm it passes.

## Task 2: `io_direct.c` — O_DIRECT VARLEN scanner resync integration

### Root cause (scanner-specific)

`seg_scan_o_direct_varlen()` validates a decoded header only against
`rec_size > SLOTCASK_SEG_MAX_BYTES` today (Site A, the carry-completion
path); the corrected check must use the caller-provided `max_slot_size`
and not at all (Site B, the in-chunk stride loop — confirmed by reading
the current code below, it has no plausibility check whatsoever, only
the chunk-boundary straddle check). On a desync it either aborts the
whole scan (`-EIO`, Site A) or silently strides forward through garbage
bytes reinterpreted as record headers (Site B) until it eventually hits
something structurally implausible or exhausts the file.

### Test-first

Create `src/test/cases/test_varlen_scan_resync_odirect.c` (see full code
in the "Task 2 test" section below) modeling
`test_coverity_seg_scan_varlen_overflow.c`'s hand-crafted-file approach:
build a real on-disk file with record A (small, natural size 32 bytes),
a 32-byte zero-filled gap (simulating a 64-byte reused slot), and record
C at offset 64 — call `seg_scan_o_direct_varlen()` and assert the
callback fires for both A and C (not just A, and not a scan abort). Run
it against current `main` first and confirm it fails (callback fires
only for A, or the scan returns nonzero) before making any code change;
paste that failing output, then implement the fix and paste the passing
output.

### Implementation

#### 2a. `dbctx_init` gains a `start_off` parameter

The restart offset is a logical record offset and is guaranteed only to
be 8-byte aligned. It is not necessarily aligned to the device sector
required by Linux `O_DIRECT`. The normal scan continues to use `od_open()`;
after a resync, the replacement `dbctx` must instead use a buffered
`O_RDONLY` descriptor (as shown in Task 2e) so `pread()` may start at the
exact recovered record offset. The resync path is bounded and correctness-
critical; it does not change the normal scan's cache-bypassing behavior.

Anchor (`src/db/io_direct.c`), current exact text:

```c
static int dbctx_init(DbCtx *c, int fd, off_t file_size, int single_shot)
{
    memset(c, 0, sizeof(*c));
    c->fd          = fd;
    c->file_size   = file_size;
    c->active      = 0;
    c->inactive    = 1;
    c->state       = DBS_IDLE;
    c->single_shot = single_shot;

    if (single_shot) {
        /* Allocate exactly what we need — no second buffer. */
        size_t exact = ((size_t)file_size + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (posix_memalign((void **)&c->buf[0], ODIRECT_ALIGN, exact) != 0)
            return -ENOMEM;
        c->buf[1] = NULL;
    } else {
        c->buf[0] = od_alloc_buf();
        c->buf[1] = od_alloc_buf();
        if (!c->buf[0] || !c->buf[1]) {
            free(c->buf[0]); free(c->buf[1]);
            return -ENOMEM;
        }
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    off_t  fsz  = file_size;
    size_t wanta;
    if (single_shot) {
        wanta = ((size_t)fsz + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    } else {
        size_t want = (fsz >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)fsz;
        wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (wanta > odirect_buf_size) wanta = odirect_buf_size;
    }

    ssize_t got = pread(fd, c->buf[0], wanta, 0);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = (off_t)got;
    return 0;
}
```

Replace with:

```c
static int dbctx_init(DbCtx *c, int fd, off_t file_size, off_t start_off, int single_shot)
{
    memset(c, 0, sizeof(*c));
    c->fd          = fd;
    c->file_size   = file_size;
    c->active      = 0;
    c->inactive    = 1;
    c->state       = DBS_IDLE;
    c->single_shot = single_shot;

    off_t remain = file_size - start_off;
    if (remain < 0) remain = 0;

    if (single_shot) {
        /* Allocate exactly what we need — no second buffer. */
        size_t exact = ((size_t)remain + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (posix_memalign((void **)&c->buf[0], ODIRECT_ALIGN, exact) != 0)
            return -ENOMEM;
        c->buf[1] = NULL;
    } else {
        c->buf[0] = od_alloc_buf();
        c->buf[1] = od_alloc_buf();
        if (!c->buf[0] || !c->buf[1]) {
            free(c->buf[0]); free(c->buf[1]);
            return -ENOMEM;
        }
    }

    pthread_mutex_init(&c->lock, NULL);
    pthread_cond_init(&c->prefetch_needed, NULL);
    pthread_cond_init(&c->prefetch_done,   NULL);

    /* Fill buf[0] synchronously. */
    size_t wanta;
    if (single_shot) {
        wanta = ((size_t)remain + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
    } else {
        size_t want = (remain >= (off_t)odirect_buf_size) ? odirect_buf_size : (size_t)remain;
        wanta = (want + ODIRECT_ALIGN - 1) & ~(size_t)(ODIRECT_ALIGN - 1);
        if (wanta > odirect_buf_size) wanta = odirect_buf_size;
    }

    ssize_t got = pread(fd, c->buf[0], wanta, start_off);
    if (got < 0) {
        int e = errno;
        free(c->buf[0]); free(c->buf[1]);
        pthread_mutex_destroy(&c->lock);
        pthread_cond_destroy(&c->prefetch_needed);
        pthread_cond_destroy(&c->prefetch_done);
        return -e;
    }
    c->active_len = got;
    c->next_off   = start_off + (off_t)got;
    return 0;
}
```

#### 2b. Thread the cached per-object record bound through the scanner

Change the declaration (including its parameter comment) in
`src/db/io_direct.h` and the definition in `src/db/io_direct.c` from:

```c
int seg_scan_o_direct_varlen(const char *seg_path,
                              od_record_cb cb, void *ctx);
```

to:

```c
int seg_scan_o_direct_varlen(const char *seg_path, size_t max_slot_size,
                              od_record_cb cb, void *ctx);
```

Document `max_slot_size` as the owning object's schema-derived maximum
on-disk record capacity; it is not the segment file's total capacity.

Reject `max_slot_size < 32` with `-EINVAL`. Pass this value to
`od_varlen_resync_find()`, to both structural-validation sites in the
scanner, and to `seg_scan_varlen_resync()`. The value is already cached
on every production caller; do not load the schema from `seg_path` or add
a registry/cache lookup to the scan path. Update all call sites explicitly:

- `src/db/query_find.c`: `od_seg_file_worker` passes `arg->slot_size`.
- `src/db/query_find.c`: `od_match_file_worker` passes `arg->slot_size`.
- `src/db/query_aggregate.c`: `agg_od_seg_worker` passes `arg->slot_size`.
- `src/db/index.c`: `reindex_seg_worker` passes `w->slot_size`.
- `src/db/slotcask.c`: the donor scan in
  `compact_migrate_records_varlen` passes `db->slot_size`.
- `src/test/cases/test_coverity_seg_scan_varlen_overflow.c` and the new
  O_DIRECT regression test pass their local fixture bound (64 bytes).

The production edits are these exact argument insertions:

```c
seg_scan_o_direct_varlen(arg->seg_path, arg->slot_size,
                         od_seg_record_cb, &actx);
seg_scan_o_direct_varlen(arg->seg_path, arg->slot_size,
                         varlen_match_cb, &mc);
seg_scan_o_direct_varlen(path, (size_t)w->slot_size, reindex_seg_cb, w);
```

The existing Coverity test's call becomes
`seg_scan_o_direct_varlen(path, 64, capture_cb, NULL)`. The compaction
donor call is shown in Task 4b and passes `db->slot_size`.

The existing callers already carry this value for record decoding, so
this is signature plumbing only. It makes the resync allocation and
search proportional to the actual schema's maximum record width rather
than the 128 MiB segment-file capacity.

This does not add work to point reads or other keyfile-backed hot paths.
The O_DIRECT scanner is used by full segment scans (reindex, scan-based
find/aggregate work, and compaction), not ordinary `get`/`exists` reads.
On a healthy scan the only changed work is the existing scalar
plausibility check comparing `rec_size` with the caller-provided bound;
there is no schema lookup, lock acquisition, allocation, or per-record
hash in that path. Resync allocates and scans only after a malformed or
desynchronized header, and the dynamic bound is normally much smaller
than 128 MiB. Full-scan throughput still matters, so Task 6 must include
the existing scan-heavy tests, but this signature change should not slow
normal reads.

#### 2c. Update the 4 pre-existing `dbctx_init` call sites to pass `start_off = 0`

Each call site below is disambiguated by its unique preceding context —
locate by that context, not by line number.

**Site 1** — inside `seg_scan_o_direct`, preceded by
`int seg_scan_o_direct(const char *seg_path, int slot_size, od_record_cb cb, void *ctx)`
and `if (!seg_path || slot_size < 32 || !cb) return -EINVAL;`:

```c
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
```
→
```c
    int rc = dbctx_init(&dc, fd, file_size, 0, single_shot);
```

**Site 2** — inside `seg_scan_o_direct_varlen` — covered by Task 2e below
(this whole function is being restructured; its `dbctx_init` call is part
of that replacement, not a standalone edit).

**Site 3** — inside `seg_scan_o_direct_match`, preceded by a signature
taking `FieldSchema *fs, const CompiledCriterion *single_cc, ...` and the
line `if (file_size == 0) { *out_count = 0; return 0; }`:

```c
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
```
→
```c
    int rc = dbctx_init(&dc, fd, file_size, 0, single_shot);
```

**Site 4** — inside `btree_leaf_scan_o_direct`:

```c
    dbctx_init(&dc, fd, file_size, 0);
```
→
```c
    dbctx_init(&dc, fd, file_size, 0, 0);
```

If any of these 4 anchors is not found verbatim (call sites can shift
between the summary that produced this plan and the current `main`),
halt per the execution rules and write `PLAN_NOTES.md` rather than
guessing at the right call site from surrounding logic.

#### 2d. `od_varlen_resync_find` — standalone resync search helper

Add immediately before `seg_scan_o_direct_varlen`'s definition, and add
`#include "seg_scan_varlen.h"` to `src/db/io_direct.c`'s existing
`#include` block (anchor: alongside the existing `#include "io_direct.h"`
line):

```c
/* Standalone resync search: opens its own read-only fd, entirely
   independent of any live DbCtx/O_DIRECT scan state, so a failure here
   leaves the caller's current scan context completely untouched and its
   existing teardown path works unmodified. Reads a bounded window
   starting at the 8-byte-aligned floor of desync_off and looks for the
   next structurally valid, hash-verified record header via
   seg_scan_varlen_resync(). Returns 0 and sets *out_resume_off on
   success; 1 if the bounded window has no such record; -1 on I/O or
   allocation failure. A flag==0 scan hit may treat the 1 result as the
   ordinary sparse tail; every other desync must treat it as an
   unrecoverable scan failure and must not delete or otherwise trust the
   file beyond desync_off. */
static int od_varlen_resync_find(const char *seg_path, off_t file_size,
                                  size_t max_slot_size, off_t desync_off,
                                  off_t *out_resume_off)
{
    off_t aligned_off = desync_off & ~(off_t)7;
    if (aligned_off < 0) aligned_off = 0;
    if (aligned_off >= file_size) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s desync offset %lld at/past EOF (%lld)",
                  seg_path, (long long)desync_off, (long long)file_size);
        return -1;
    }

    size_t window = max_slot_size;
    off_t remain = file_size - aligned_off;
    if ((off_t)window > remain) window = (size_t)remain;
    size_t read_cap = (max_slot_size > (size_t)remain - window)
        ? (size_t)remain : window + max_slot_size;

    int fd = open(seg_path, O_RDONLY);
    if (fd < 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "od_varlen_resync_find: %s open failed: %s",
                  seg_path, strerror(errno));
        return -1;
    }

    uint8_t *buf = malloc(read_cap);
    if (!buf) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s OOM allocating %zu-byte resync buffer",
                  seg_path, read_cap);
        close(fd);
        return -1;
    }

    size_t got_total = 0;
    while (got_total < read_cap) {
        ssize_t got = pread(fd, buf + got_total, read_cap - got_total,
                             aligned_off + (off_t)got_total);
        if (got < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "od_varlen_resync_find: %s pread failed at %lld: %s",
                      seg_path, (long long)(aligned_off + (off_t)got_total),
                      strerror(errno));
            free(buf);
            close(fd);
            return -1;
        }
        if (got == 0) break;
        got_total += (size_t)got;
    }
    close(fd);

    size_t next;
    size_t search_from = (desync_off > aligned_off)
        ? (size_t)(desync_off - aligned_off) : 0;
    int found = (search_from < got_total) &&
                seg_scan_varlen_resync(buf, got_total, search_from,
                                       max_slot_size,
                                       got_total - search_from < window
                                           ? got_total - search_from : window,
                                       &next);
    free(buf);

    if (!found) {
        LOG_DEBUG(LOG_SUB_SLOTCASK,
                  "od_varlen_resync_find: %s no valid record found within "
                  "%zu-byte per-object window starting at %lld",
                  seg_path, window, (long long)aligned_off);
        return 1;
    }

    *out_resume_off = aligned_off + (off_t)next;
    LOG_WARN(LOG_SUB_SLOTCASK,
             "od_varlen_resync_find: %s resynced desync at %lld to %lld",
             seg_path, (long long)desync_off, (long long)*out_resume_off);
    return 0;
}
```

#### 2e. `seg_scan_o_direct_varlen` — full replacement

Anchor: the entire current function, from `int seg_scan_o_direct_varlen`
through its closing `}` (current text captured in full below — replace
the whole function body).

Current text:

```c
int seg_scan_o_direct_varlen(const char *seg_path,
                              od_record_cb cb, void *ctx)
{
    if (!seg_path || !cb) return -EINVAL;

    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, single_shot);
    if (rc != 0) { close(fd); return rc; }

    size_t carry_cap = OD_VARLEN_CARRY_SIZE;
    uint8_t *carry = malloc(carry_cap);
    if (!carry) {
        if (single_shot) free(dc.buf[0]);
        else { free(dc.buf[0]); free(dc.buf[1]); }
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -ENOMEM;
    }
    int carry_len = 0;
    off_t base_off = 0; /* DIAGNOSTIC (temporary): file offset of dc.active chunk 0 */

    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
        int e2;
        if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
            e2 = errno;
            free(carry);
            free(dc.buf[0]); free(dc.buf[1]);
            pthread_mutex_destroy(&dc.lock);
            pthread_cond_destroy(&dc.prefetch_needed);
            pthread_cond_destroy(&dc.prefetch_done);
            close(fd);
            return -e2;
        }
        dbctx_kickoff(&dc);
    }

    int ret = 0;
    int padding_desync = 0;

    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) ret = (int)chunk_len;
            break;
        }

        uint8_t *chunk = dc.buf[dc.active];
        size_t   pos   = 0;

        /* Reassemble a record that straddled the previous chunk boundary. */
        if (carry_len > 0) {
            /* DIAGNOSTIC (temporary): carry always holds the trailing bytes
               of the stream ending exactly at base_off, so the record this
               carry belongs to started at base_off - carry_len regardless
               of how many earlier chunks contributed to it. */
            off_t diag_rec_start = base_off - (off_t)carry_len;
            /* Stage 1: ensure we have the 24-byte header in carry. */
            if (carry_len < 24) {
                int need = 24 - carry_len;
                if ((ssize_t)need > chunk_len) {
                    /* Still not enough — stay in carry. */
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos += (size_t)need;
                carry_len = 24;
            }

            /* Stage 2: carry has 24-byte header; complete the record. */
            uint16_t klen;
            uint32_t vlen;
            uint8_t  flag;
            memcpy(&klen, carry + 16, 2);
            memcpy(&vlen, carry + 20, 4);
            flag = carry[18];
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            /* rec_size is derived from an on-disk, unvalidated vlen.
               A corrupted vlen can make rec_size enormous; narrowing
               it into `int` below would silently wrap and produce a
               small or negative `need`, skipping the "need more data"
               branch and passing a huge vlen straight to cb() against
               the small carry buffer (CID 1696466). Reject anything
               past the largest a legitimate segment record could be. */
            if (rec_size > SLOTCASK_SEG_MAX_BYTES) {
                /* DIAGNOSTIC (temporary): dump exactly what was decoded and
                   where, to find why a legitimate stream produced this. */
                fprintf(stderr,
                        "[od_varlen_diag] %s: bogus header at file_off=%lld "
                        "(chunk base_off=%lld carry_len_at_entry=%lld) "
                        "klen=%u vlen=%u flag=%u rec_size=%zu chunk_len=%zd "
                        "single_shot=%d\n",
                        seg_path, (long long)diag_rec_start,
                        (long long)base_off,
                        (long long)(base_off - diag_rec_start),
                        (unsigned)klen, (unsigned)vlen, (unsigned)flag,
                        rec_size, chunk_len, single_shot);
                ret = -EIO;
                goto done;
            }

            int need = (int)rec_size - carry_len;
            if (need > 0) {
                if ((ssize_t)need > chunk_len) {
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                if (rec_size > carry_cap) {
                    uint8_t *nc = realloc(carry, rec_size);
                    if (!nc) { ret = -ENOMEM; goto done; }
                    carry = nc; carry_cap = rec_size;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
            }

            if (flag == 1) {
                if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            carry_len = 0;
        }

        /* Stride through whole records in this chunk. */
        while (pos + 24 <= (size_t)chunk_len) {
            uint8_t *rec  = chunk + pos;
            uint8_t  flag = rec[18];
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            if (pos + rec_size > (size_t)chunk_len) {
                /* Record straddles chunk boundary — save tail in carry. */
                break;
            }

            if (flag == 1) {
                if (cb(rec, (size_t)vlen, rec, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            pos += rec_size;
        }

        /* Save any partial bytes at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            size_t tail = (size_t)chunk_len - pos;
            if (tail > carry_cap) {
                uint8_t *nc = realloc(carry, tail);
                if (!nc) { ret = -ENOMEM; goto done; }
                carry = nc; carry_cap = tail;
            }
            carry_len = (int)tail;
            memcpy(carry, chunk + pos, tail);
        }

next_chunk:
        base_off += chunk_len; /* DIAGNOSTIC (temporary) */
        {
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done; }
            if (next == 0) {
                dc.active_len = 0;
                break;
            }
        }
    }

done:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    return ret;
}
```

New text:

```c
int seg_scan_o_direct_varlen(const char *seg_path, size_t max_slot_size,
                              od_record_cb cb, void *ctx)
{
    if (!seg_path || max_slot_size < 32 || !cb) return -EINVAL;

    if (odirect_buf_size == 0) odirect_init_buf_size();

    struct stat st;
    if (stat(seg_path, &st) != 0) return -errno;
    off_t file_size = st.st_size;
    if (file_size == 0) return 0;

    int single_shot = (file_size <= (off_t)odirect_buf_size);

    int fd = od_open(seg_path);
    if (fd < 0) return -errno;

    DbCtx dc;
    int rc = dbctx_init(&dc, fd, file_size, 0, single_shot);
    if (rc != 0) { close(fd); return rc; }

    size_t carry_cap = OD_VARLEN_CARRY_SIZE;
    uint8_t *carry = malloc(carry_cap);
    if (!carry) {
        if (single_shot) free(dc.buf[0]);
        else { free(dc.buf[0]); free(dc.buf[1]); }
        pthread_mutex_destroy(&dc.lock);
        pthread_cond_destroy(&dc.prefetch_needed);
        pthread_cond_destroy(&dc.prefetch_done);
        close(fd);
        return -ENOMEM;
    }
    int carry_len = 0;
    off_t base_off = 0;
    off_t resync_from = 0;

    pthread_t worker_tid = (pthread_t)0;
    if (!single_shot) {
        int e2;
        if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
            e2 = errno;
            free(carry);
            free(dc.buf[0]); free(dc.buf[1]);
            pthread_mutex_destroy(&dc.lock);
            pthread_cond_destroy(&dc.prefetch_needed);
            pthread_cond_destroy(&dc.prefetch_done);
            close(fd);
            return -e2;
        }
        dbctx_kickoff(&dc);
    }

    int ret = 0;
    int padding_desync = 0;

scan_top:
    for (;;) {
        ssize_t chunk_len = dc.active_len;
        if (chunk_len <= 0) {
            if (chunk_len < 0) ret = (int)chunk_len;
            break;
        }

        uint8_t *chunk = dc.buf[dc.active];
        size_t   pos   = 0;

        /* Reassemble a record that straddled the previous chunk boundary. */
        if (carry_len > 0) {
            off_t rec_start_off = base_off - (off_t)carry_len;
            /* Stage 1: ensure we have the 24-byte header in carry. */
            if (carry_len < 24) {
                int need = 24 - carry_len;
                if ((ssize_t)need > chunk_len) {
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos += (size_t)need;
                carry_len = 24;
            }

            /* Stage 2: carry has 24-byte header; complete the record. */
            uint16_t klen;
            uint32_t vlen;
            uint8_t  flag;
            memcpy(&klen, carry + 16, 2);
            memcpy(&vlen, carry + 20, 4);
            flag = carry[18];
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            /* flag==0 is sparse/pool-reuse padding, not a real record
               start for this scanner. A flag outside 0/1/2, or a
               rec_size (derived from an on-disk, unvalidated vlen) past
               the largest legitimate record, likewise means this offset
               isn't a real record start. Otherwise a reused-slot gap or
               corrupted header could either narrow into a huge/negative
               `need` below (CID 1696466) or walk `pos` through garbage.
               Resync forward instead of aborting the whole scan. */
            if (flag == 0 || flag > 2 || rec_size > max_slot_size) {
                padding_desync = (flag == 0);
                resync_from = rec_start_off;
                goto do_resync;
            }

            int need = (int)rec_size - carry_len;
            if (need > 0) {
                if ((ssize_t)need > chunk_len) {
                    size_t need_cap = (size_t)(carry_len + chunk_len);
                    if (need_cap > carry_cap) {
                        uint8_t *nc = realloc(carry, need_cap);
                        if (!nc) { ret = -ENOMEM; goto done; }
                        carry = nc; carry_cap = need_cap;
                    }
                    memcpy(carry + carry_len, chunk, (size_t)chunk_len);
                    carry_len += (int)chunk_len;
                    goto next_chunk;
                }
                if (rec_size > carry_cap) {
                    uint8_t *nc = realloc(carry, rec_size);
                    if (!nc) { ret = -ENOMEM; goto done; }
                    carry = nc; carry_cap = rec_size;
                }
                memcpy(carry + carry_len, chunk, (size_t)need);
                pos   += (size_t)need;
            }

            if (flag == 1) {
                if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            carry_len = 0;
        }

        /* Stride through whole records in this chunk. */
        while (pos + 24 <= (size_t)chunk_len) {
            uint8_t *rec  = chunk + pos;
            uint8_t  flag = rec[18];
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            if (flag == 0 || flag > 2 || rec_size > max_slot_size) {
                padding_desync = (flag == 0);
                resync_from = base_off + (off_t)pos;
                goto do_resync;
            }

            if (pos + rec_size > (size_t)chunk_len) {
                /* Record straddles chunk boundary — save tail in carry. */
                break;
            }

            if (flag == 1) {
                if (cb(rec, (size_t)vlen, rec, ctx) != 0) {
                    ret = 1; goto done;
                }
            }
            pos += rec_size;
        }

        /* Save any partial bytes at the chunk tail. */
        if (pos < (size_t)chunk_len) {
            size_t tail = (size_t)chunk_len - pos;
            if (tail > carry_cap) {
                uint8_t *nc = realloc(carry, tail);
                if (!nc) { ret = -ENOMEM; goto done; }
                carry = nc; carry_cap = tail;
            }
            carry_len = (int)tail;
            memcpy(carry, chunk + pos, tail);
        }

next_chunk:
        base_off += chunk_len;
        {
            ssize_t next = dbctx_swap(&dc);
            if (next < 0) { ret = (int)next; goto done; }
            if (next == 0) {
                dc.active_len = 0;
                break;
            }
        }
    }

    goto done;

do_resync:
    {
        off_t resume_off;
        int rrc = od_varlen_resync_find(seg_path, file_size, max_slot_size,
                                        resync_from, &resume_off);
        if (rrc != 0) {
            /* A flag==0 header with no later real record is the normal
               sparse tail; an I/O/allocation failure is still an error.
               Non-padding desyncs are never silently truncated. */
            ret = (rrc > 0 && padding_desync) ? 0 : -EIO;
            dbctx_destroy(&dc, worker_tid);
            free(carry);
            close(fd);
            return ret;
        }

        /* Old context confirmed no longer needed — safe to tear down now
           that we know the rebuild has somewhere valid to resume from. */
        dbctx_destroy(&dc, worker_tid);
        close(fd);

        /* `resume_off` is guaranteed only to be 8-byte aligned (record
           alignment), not aligned to the device sector required by
           O_DIRECT.  Restart the logical tail scan on a buffered fd; the
           normal scan still uses od_open/O_DIRECT, and this bounded fallback
           avoids turning every legitimate resync into EINVAL. */
        fd = open(seg_path, O_RDONLY);
        if (fd < 0) {
            ret = -errno;
            free(carry);
            return ret;
        }

        single_shot = ((file_size - resume_off) <= (off_t)odirect_buf_size);
        rc = dbctx_init(&dc, fd, file_size, resume_off, single_shot);
        if (rc != 0) {
            ret = rc;
            close(fd);
            free(carry);
            return ret;
        }

        carry_len = 0;
        base_off = resume_off;

        if (!single_shot) {
            int e2;
            if (pthread_create(&worker_tid, NULL, prefetch_worker, &dc) != 0) {
                e2 = errno;
                free(dc.buf[0]); free(dc.buf[1]);
                pthread_mutex_destroy(&dc.lock);
                pthread_cond_destroy(&dc.prefetch_needed);
                pthread_cond_destroy(&dc.prefetch_done);
                close(fd);
                free(carry);
                return -e2;
            }
            dbctx_kickoff(&dc);
        } else {
            worker_tid = (pthread_t)0;
        }

        ret = 0;
        goto scan_top;
    }

done:
    dbctx_destroy(&dc, worker_tid);
    free(carry);
    close(fd);
    return ret;
}
```

Note on scope: normal (non-resync) fast-path scanning at Site A/B is
deliberately **not** hash-verified per record — only structural
   plausibility (`flag <= 2`, `rec_size <= max_slot_size`) gates
entry into resync. Adding per-record hash verification to the hot path
would be scope creep beyond what the blockers ask for and would cost
real per-record hashing on every scan. Hash verification is reserved for
the resync search itself (`seg_scan_varlen_resync`, which only runs on
desync) and for the lower-frequency maintenance loops in Tasks 3/4a/4b.

### Task 2 test

Create `src/test/cases/test_varlen_scan_resync_odirect.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include "seg_scan_varlen.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

typedef struct { int count; char keys[8][16]; } CaptureCtx;

static int capture_cb(const uint8_t *rec, size_t vlen,
                       const uint8_t hash16[16], void *raw) {
    (void)hash16;
    CaptureCtx *c = (CaptureCtx *)raw;
    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    if (c->count < 8 && klen < 16) {
        memcpy(c->keys[c->count], rec + 24, klen);
        c->keys[c->count][klen] = '\0';
    }
    (void)vlen;
    c->count++;
    return 0;
}

static size_t write_record(FILE *f, uint8_t flag,
                            const char *key, uint16_t klen,
                            const char *val, uint32_t vlen) {
    uint8_t hash[16];
    compute_hash_raw(key, klen, hash);
    uint8_t hdr[24];
    memcpy(hdr, hash, 16);
    memcpy(hdr + 16, &klen, 2);
    hdr[18] = flag;
    hdr[19] = 0;
    memcpy(hdr + 20, &vlen, 4);
    fwrite(hdr, 1, 24, f);
    fwrite(key, 1, klen, f);
    fwrite(val, 1, vlen, f);
    size_t natural = 24 + (size_t)klen + (size_t)vlen;
    size_t padded = (natural + 7) & ~(size_t)7;
    size_t pad = padded - natural;
    uint8_t z[8] = {0};
    if (pad) fwrite(z, 1, pad, f);
    return padded;
}

static int test_varlen_scan_resync_odirect_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-varlen-resync-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    /* Record A: natural 32 bytes, but simulate it reusing a 64-byte
       freed slot by padding an extra 32 zero bytes after it — matching
       seg_record_emit()'s real zero-pad-to-slot_size behavior on a
       pool-reuse write. Record C starts at the 64-byte boundary. */
    size_t a_natural = write_record(f, 1, "ka", 2, "v", 1);
    ASSERT_EQ_INT((int)a_natural, 32, "record A padded size");
    uint8_t extra_pad[32] = {0};
    fwrite(extra_pad, 1, sizeof(extra_pad), f); /* total gap after A: 64 bytes */

    long c_off = ftell(f);
    ASSERT_EQ_INT((int)c_off, 64, "record C starts at the old slot's capacity boundary");
    write_record(f, 1, "kc", 2, "cc", 2);
    uint8_t sparse_tail[128] = {0};
    fwrite(sparse_tail, 1, sizeof(sparse_tail), f);

    fclose(f);

    CaptureCtx ctx = {0};
    int rc = seg_scan_o_direct_varlen(path, 64, capture_cb, &ctx);

    ASSERT_EQ_INT(rc, 0, "scan completes without error (resync recovers and sparse flag-0 tail is clean)");
    ASSERT_EQ_INT(ctx.count, 2, "both A and C are delivered to the callback");
    if (ctx.count == 2) {
        ASSERT_TRUE(strcmp(ctx.keys[0], "ka") == 0, "first record is A");
        ASSERT_TRUE(strcmp(ctx.keys[1], "kc") == 0, "second record is C, not padding");
    }

    unlink(path);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-scan-resync-odirect", test_varlen_scan_resync_odirect_run)
```

### Verification (paste both outputs)

1. On `main` (before any Task 2 edit — Task 0/1 may already be applied,
   they don't affect this scanner): build and run
   `./build/bin/shard-db-test run test-varlen-scan-resync-odirect` —
   confirm it fails (`ctx.count == 1` and/or `rc != 0`, i.e. the scan
   either misses C or aborts entirely).
2. After the Task 2 edit: rebuild and rerun — confirm it passes
   (`rc == 0`, `ctx.count == 2`, both keys correct).

## Task 3: `seg_stat_one_varlen` — return failure instead of silently truncating

### Root cause

Confirmed current caller contract (`compact_one_stream_varlen`,
`src/db/slotcask.c`):

```c
        if (seg_stat_one_varlen(db, stream_id, files[i].file_id,
                                 &files[i].live_count,
                                 &files[i].total_slots) != 0) {
            files[i].live_count = files[i].total_slots = 0;
        }
```

followed later by:

```c
        if (files[i].total_slots == 0) { i++; continue; }
```

which comes *before* the `live_count == 0` deletion branch. This means
the caller already does the right thing on failure (skip-preserve, never
delete) — **provided `seg_stat_one_varlen` actually returns nonzero**.
Today it always returns 0, even when it silently truncated a desynced
scan via `break`. That's the entire bug: the caller's safety guard exists
but never fires.

If, at fix time, `compact_one_stream_varlen`'s exact current structure
differs from the two snippets quoted above (e.g. due to unrelated
changes landed on `main` since this plan was written), halt per the
execution rules — do not assume the caller-side contract still holds
without re-confirming both snippets against the live file first.

### Test-first

Create `src/test/cases/test_varlen_compact_stat_resync.c` (full code in
"Task 3 test" below): using the real public API (`slotcask_insert` /
`slotcask_delete` / `slotcask_compact_segs`), construct the exact
reuse-gap fixture (insert large record X, delete X, insert small record
A which reuses X's freed slot leaving a real zero-filled gap, insert
record C which — with the pool now empty — appends immediately after
X's old slot capacity boundary, i.e. immediately after A's gap) across
two segment files so compaction has a donor/recipient pair to migrate,
then run `slotcask_compact_segs()` and assert C still exists and reads
back correctly afterward. C's survival is the data-loss oracle:
the old stats path can report a partial donor as empty, delete it, and
make C disappear. Run against
current `main` first, confirm it fails, paste that output; then apply
the Task 3 fix and confirm it passes, paste that output.

All three compaction tests use the same exact rotation fixture. Add
`src/test/cases/varlen_compact_fixture.h` and include it after
`slotcask.h` in each test. This replaces the previous unresolved
"force rotation" note. The fixture uses `slot_size=8192` because the
public insert API rejects values larger than the configured maximum; the
original `slot_size=64` plus 5000-byte value could never insert X.

```c
#ifndef SHARD_DB_TEST_VARLEN_COMPACT_FIXTURE_H
#define SHARD_DB_TEST_VARLEN_COMPACT_FIXTURE_H

#include "slotcask.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VARLEN_FIXTURE_VALUE_LEN 8000u

/* Build three files on stream 0:
 *   file 0: A and C live, all filler tombstoned (donor with a reuse gap)
 *   file 1: four tombstoned filler records and the rest live (recipient)
 *   file 2: active and empty
 * The two-pointer compactor therefore has an eligible donor/recipient pair
 * and must execute seg_stat_one_varlen, the recipient walk, and the donor
 * scan. Returns 0 on success, -1 on any setup failure. */
static int varlen_compact_fixture_build(SlotcaskDb *db) {
    char xval[5000];
    char fill[VARLEN_FIXTURE_VALUE_LEN];
    memset(xval, 'x', sizeof(xval));
    memset(fill, 'z', sizeof(fill));

    if (slotcask_insert(db, 0, "keyX", 4, xval, sizeof(xval)) != 0) return -1;
    if (slotcask_delete(db, "keyX", 4) != 0) return -1;
    if (slotcask_insert(db, 0, "kA", 2, "v", 1) != 0) return -1;
    if (slotcask_insert(db, 0, "kkeyC", 5, "cvalue", 6) != 0) return -1;

    size_t rec_size = (24u + 10u + VARLEN_FIXTURE_VALUE_LEN + 7u) & ~7u;
    size_t key_cap = (size_t)(SLOTCASK_SEG_MAX_BYTES / rec_size) + 4u;
    char (*file0_keys)[32] = calloc(key_cap, sizeof(*file0_keys));
    if (!file0_keys) return -1;

    size_t file0_count = 0;
    while (db->streams[0].active_file_id == 0) {
        if (file0_count >= key_cap) { free(file0_keys); return -1; }
        snprintf(file0_keys[file0_count], 32, "f0-%06zu", file0_count);
        if (slotcask_insert(db, 0, file0_keys[file0_count],
                            strlen(file0_keys[file0_count]),
                            fill, sizeof(fill)) != 0) {
            free(file0_keys);
            return -1;
        }
        file0_count++;
    }

    char file1_keys[4][32];
    size_t file1_count = 0;
    while (db->streams[0].active_file_id == 1) {
        char key[32];
        snprintf(key, sizeof(key), "f1-%06zu", file1_count);
        if (file1_count < 4) snprintf(file1_keys[file1_count], 32, "%s", key);
        if (slotcask_insert(db, 0, key, strlen(key), fill, sizeof(fill)) != 0) {
            free(file0_keys);
            return -1;
        }
        file1_count++;
    }
    if (db->streams[0].active_file_id != 2 || file1_count < 4) {
        free(file0_keys);
        return -1;
    }

    /* Create recipient capacity only after rotation, so these tombstones
       cannot be consumed by later inserts and move the fixture backward. */
    for (size_t i = 0; i < file0_count; i++) {
        if (slotcask_delete(db, file0_keys[i], strlen(file0_keys[i])) != 0) {
            free(file0_keys);
            return -1;
        }
    }
    free(file0_keys);
    for (size_t i = 0; i < 4; i++) {
        if (slotcask_delete(db, file1_keys[i], strlen(file1_keys[i])) != 0)
            return -1;
    }
    return 0;
}

#endif
```

Each test must open with `slotcask_open(&db, tmpdir, 8, 1, 8192)`, call
`slotcask_migrate_to_varlen`, then call
`varlen_compact_fixture_build(&db)` and assert success before invoking
`slotcask_compact_segs`. The fixture's `file0` filler keys are deleted
only after `file1` and `file2` exist, preventing the free pool from being
reused while the rotation is being forced.

### Implementation

Anchor (`src/db/slotcask.c`), current exact text:

```c
/* Variable-length variant: walk records by reading headers sequentially. */
static int seg_stat_one_varlen(SlotcaskDb *db, int stream_id, uint32_t file_id,
                               uint32_t *out_live, uint32_t *out_total) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;

    size_t file_size = h.map_size;
    uint32_t live = 0, total = 0;
    size_t off = 0;

    while (off + 24 <= file_size) {
        const uint8_t *rec = h.map + off;
        uint16_t klen;
        uint32_t vlen;
        uint8_t flag;
        memcpy(&klen, rec + 16, 2);
        memcpy(&vlen, rec + 20, 4);
        flag = rec[18];
        size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
        if (off + rec_size > file_size) break;
        if (flag != 0) total++;
        if (flag == 1) live++;
        off += rec_size;
    }

    segcache_release(&h);
    *out_live = live;
    *out_total = total;
    return 0;
}
```

Replace with:

```c
/* Variable-length variant: walk records by reading headers sequentially.
   Returns -1 (leaving the output counters unset) if the scan hits an
   unrecoverable desync — the caller (compact_one_stream_varlen) treats
   a nonzero return as "stats unknown, preserve the file untouched"
   rather than "file is empty, safe to delete". A silent 0-return here
   after a partial scan was the exact mechanism of a prior silent
   data-loss bug: a desync before the first live record made this
   function report live_count == 0 for a file that still held live
   records past the desync point. */
static int seg_stat_one_varlen(SlotcaskDb *db, int stream_id, uint32_t file_id,
                               uint32_t *out_live, uint32_t *out_total) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 0) != 0) return -1;

    size_t file_size = h.map_size;
    uint32_t live = 0, total = 0;
    size_t off = 0;

    while (off + 24 <= file_size) {
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int ok = seg_scan_varlen_struct_ok(h.map, file_size, off,
                                            db->slot_size, &rec_size,
                                            &flag, &klen, &vlen);
        if (ok && flag == 0) {
            /* A zero header is structurally valid padding, but it is also
               the exact shape of a reused-slot gap. Try to find the next
               real record within this object's maximum slot capacity; if
               none exists, this is ordinary sparse tail and the stats walk
               is complete. Never stride through flag==0 in 24-byte steps. */
            size_t next;
            if (!seg_scan_varlen_resync(h.map, file_size, off,
                                         db->slot_size, db->slot_size,
                                         &next))
                break;
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "seg_stat_one_varlen: %s skipped padding at offset %zu; resumed at %zu",
                     path, off, next);
            off = next;
            continue;
        }
        if (ok && flag != 0)
            ok = seg_scan_varlen_hash_ok(h.map, off, klen);
        if (!ok) {
            size_t next;
            if (!seg_scan_varlen_resync(h.map, file_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "seg_stat_one_varlen: %s unrecoverable desync at offset %zu",
                          path, off);
                segcache_release(&h);
                return -1;
            }
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "seg_stat_one_varlen: %s resynced at offset %zu (was %zu)",
                     path, next, off);
            off = next;
            continue;
        }
        if (flag != 0) total++;
        if (flag == 1) live++;
        off += rec_size;
    }

    segcache_release(&h);
    *out_live = live;
    *out_total = total;
    return 0;
}
```

The replacement must treat a structurally valid `flag == 0` header as
padding that requires the same bounded resync attempt as the recipient
walk in Task 4a. If no later hash-verified record is found within
`db->slot_size`, stop at the sparse tail; do not advance by the 24-byte
zero-header size. This is required for the real reused-slot gap and is
not optional hash validation polish.

Add `#include "seg_scan_varlen.h"` to `src/db/slotcask.c`'s include
block (anchor: immediately after `#include "io_direct.h"`, which is the
last `#include` line in the file's header block).

No change is needed at the call site in `compact_one_stream_varlen` —
its existing `if (... != 0) { files[i].live_count = files[i].total_slots
= 0; }` followed by the `total_slots == 0` skip-preserve branch already
does the right thing once this function actually returns -1 on failure.

### Task 3 test

Create `src/test/cases/test_varlen_compact_stat_resync.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "varlen_compact_fixture.h"
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_varlen_compact_stat_resync_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-varlen-compact-stat-%d", (int)getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192); /* single stream: deterministic file layout */
    ASSERT_EQ_INT(ret, 0, "slotcask_open");
    ret = slotcask_migrate_to_varlen(&db);
    ASSERT_EQ_INT(ret, 0, "migrate_to_varlen");
    ret = varlen_compact_fixture_build(&db);
    ASSERT_EQ_INT(ret, 0, "build rotated donor/recipient fixture");
    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes without an error");

    void *val_out;
    size_t vlen_out;
    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get C after compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "C vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "C value intact");
        free(val_out);
    }

    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-stat-resync", test_varlen_compact_stat_resync_run)
```

### Verification (paste both outputs)

1. On `main` (Task 0/1/2 already applied is fine, they don't touch
   `seg_stat_one_varlen`): resolve the rotation-forcing note in the test
   above first (this is a required part of Task 3, not optional
   polish — the test must genuinely exercise the resync path), then
   confirm the test fails against the unfixed function.
2. After the Task 3 fix: rebuild and rerun — confirm it passes and C's
   value round-trips correctly.

## Task 4a: recipient free-slot walk — resync-aware + failure propagation

### Root cause

The recipient walk inside `compact_migrate_records_varlen` is worse than
Task 3's bug: it doesn't check `flag <= 2` or `rec_size` bounds *at all*
— any byte pattern with `flag != 1` (including a desynced garbage flag
byte) is unconditionally treated as a legitimate free slot and added to
`free_offs`/`free_caps`, which compaction can later **write a migrated
record into**, destroying whatever real bytes were actually there.

### Test-first

Uses the shared `varlen_compact_fixture.h` fixture from Task 3. Create
`src/test/cases/test_varlen_compact_recipient_resync.c` (full code
below), asserting specifically that after compaction no record other
than the ones actually inserted is recoverable via `slotcask_get` for
any key, and that C in particular survived — proving the recipient walk
didn't corrupt anything by treating a desync gap as a writable free
slot. Run against `main` first, confirm failure; apply the fix; confirm
pass.

### Implementation

Anchor (`src/db/slotcask.c`), current exact text:

```c
    /* Walk recipient records to find free slots (flag != 1) with capacity.
       Also collects the total number of records in the file for stats. */
    uint32_t *free_offs = NULL;
    uint32_t *free_caps = NULL;
    size_t free_count = 0, free_cap = 0;
    size_t off = 0;

    while (off + 24 <= rmap_size) {
        const uint8_t *rec = rh.map + off;
        uint8_t flag = rec[18];
        if (flag != 1) {
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
            /* Only add as free if it's a complete record. */
            if (off + rec_size <= rmap_size) {
                if (free_count == free_cap) {
                    size_t nc = free_cap ? free_cap * 2 : 256;
                    uint32_t *old_o = free_offs;
                    uint32_t *old_c = free_caps;
                    uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
                    uint32_t *c = realloc(free_caps, nc * sizeof(uint32_t));
                    if (!t) { free(old_o); free(c ? c : old_c); segcache_release(&rh); return -1; }
                    if (!c) { free(t); free(old_c); segcache_release(&rh); return -1; }
                    free_offs = t;
                    free_caps = c;
                    free_cap = nc;
                }
                free_offs[free_count] = (uint32_t)off;
                free_caps[free_count] = (uint32_t)rec_size;
                free_count++;
            }
            off += rec_size;
        } else {
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
            off += rec_size;
        }
    }
```

Replace with:

```c
    /* Walk recipient records to find tombstone slots (flag == 2) with
       capacity. Never add flag==0 padding/unused tail bytes to the free
       list: segment files are sparse and mapped to the full 128 MiB cap,
       so doing so would manufacture millions of fake 24-byte slots.
       Every header trusted enough to compute rec_size from must pass
       struct_ok; flag==1 and flag==2 records must also hash-verify. */
    uint32_t *free_offs = NULL;
    uint32_t *free_caps = NULL;
    size_t free_count = 0, free_cap = 0;
    size_t off = 0;

    while (off + 24 <= rmap_size) {
        size_t rec_size;
        uint8_t flag;
        uint16_t klen;
        uint32_t vlen;
        int ok = seg_scan_varlen_struct_ok(rh.map, rmap_size, off,
                                            db->slot_size, &rec_size,
                                            &flag, &klen, &vlen);
        if (!ok) {
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "compact_migrate_records_varlen: %s unrecoverable desync at offset %zu",
                          recipient_path, off);
                free(free_offs);
                free(free_caps);
                segcache_release(&rh);
                return -1;
            }
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "compact_migrate_records_varlen: %s resynced at offset %zu (was %zu)",
                     recipient_path, next, off);
            off = next;
            continue;
        }

        if (flag == 0) {
            /* Zero-filled reuse gaps may contain a later real record;
               unused tail bytes may not. Resync to the next hash-verified
               live/tombstone record, or finish normally at the tail. */
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next))
                break;
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "compact_migrate_records_varlen: %s skipped padding at offset %zu; resumed at %zu",
                     recipient_path, off, next);
            off = next;
            continue;
        }

        if (!seg_scan_varlen_hash_ok(rh.map, off, klen)) {
            size_t next;
            if (!seg_scan_varlen_resync(rh.map, rmap_size, off,
                                         db->slot_size, db->slot_size,
                                         &next)) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "compact_migrate_records_varlen: %s hash/desync at offset %zu",
                          recipient_path, off);
                free(free_offs);
                free(free_caps);
                segcache_release(&rh);
                return -1;
            }
            LOG_WARN(LOG_SUB_SLOTCASK,
                     "compact_migrate_records_varlen: %s resynced at offset %zu (was %zu)",
                     recipient_path, next, off);
            off = next;
            continue;
        }

        if (flag == 2) {
            if (free_count == free_cap) {
                size_t nc = free_cap ? free_cap * 2 : 256;
                uint32_t *old_o = free_offs;
                uint32_t *old_c = free_caps;
                uint32_t *t = realloc(free_offs, nc * sizeof(uint32_t));
                uint32_t *c = realloc(free_caps, nc * sizeof(uint32_t));
                if (!t) { free(old_o); free(c ? c : old_c); segcache_release(&rh); return -1; }
                if (!c) { free(t); free(old_c); segcache_release(&rh); return -1; }
                free_offs = t;
                free_caps = c;
                free_cap = nc;
            }
            free_offs[free_count] = (uint32_t)off;
            free_caps[free_count] = (uint32_t)rec_size;
            free_count++;
        }
        off += rec_size;
    }
```

(The redundant `off + rec_size <= rmap_size` bounds check from the
original is dropped since `seg_scan_varlen_struct_ok` already guarantees
this whenever it returns success.) If the anchor's variable names
(`rh`, `rmap_size`, `recipient_path`) differ from current `main`, halt
per the execution rules rather than substituting names by inference.

### Task 4a test

Create `src/test/cases/test_varlen_compact_recipient_resync.c` — same
fixture and structure as Task 3's test, but additionally asserting that
after `slotcask_compact_segs`, every key inserted (`kA`, `kkeyC`) reads
back with its exact original value, and `keyX` (deleted) still reads as
not-found — i.e. compaction neither lost C nor resurrected/corrupted
anything via a bogus free-slot entry:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "varlen_compact_fixture.h"

static int test_varlen_compact_recipient_resync_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-varlen-compact-recip-%d", (int)getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(ret, 0, "slotcask_open");
    ret = slotcask_migrate_to_varlen(&db);
    ASSERT_EQ_INT(ret, 0, "migrate_to_varlen");

    ret = varlen_compact_fixture_build(&db);
    ASSERT_EQ_INT(ret, 0, "build rotated donor/recipient fixture");
    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes without an error");

    ret = slotcask_exists(&db, "keyX", 4);
    ASSERT_EQ_INT(ret, 0, "X remains deleted (not resurrected via a bogus free-slot write)");

    void *val_out;
    size_t vlen_out;
    ret = slotcask_get(&db, "kA", 2, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get A after compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 1, "A vlen intact");
        ASSERT_TRUE(memcmp(val_out, "v", 1) == 0, "A value intact");
        free(val_out);
    }

    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get C after compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "C vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "C value intact");
        free(val_out);
    }

    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-recipient-resync", test_varlen_compact_recipient_resync_run)
```

### Verification (paste both outputs)

Same procedure as Task 3: run against `main` first (with Task 0-3
already applied); apply the Task 4a fix; rebuild and rerun, confirm all
assertions pass.

## Task 4b: donor scan — consolidate onto `seg_scan_o_direct_varlen`

### Root cause / design decision

The donor scan inside `compact_migrate_records_varlen` is a raw mmap
walk with zero header validation:

```c
    /* Scan donor using the fixed O_DIRECT scanner with db->slot_size.
       For varlen, this strides by slot_size, which is the fixed max-slot
       size.  Most records are smaller, so we'll skip padding regions
       (flag=0) just like the fixed format does.  Records are still at
       variable offsets but the scanner doesn't need to care — it only
       processes flag==1 records, which have their header set correctly
       at their real offset.  The key insight: in varlen format, each
       record occupies exactly its padded size, and flag=0 regions between
       records don't exist (no fixed slot grid).  However, seg_scan_o_direct
       uses slot_size stride which is wrong for varlen records that are
       shorter than slot_size.  So we use a simpler mmap-based scan
       instead for the donor. */
    /* Donor: mmap walk of headers (not O_DIRECT — varlen records aren't
       at fixed stride).  Since the donor will be unlinked after migration,
       cache pollution is short-lived. */
    {
        SlotcaskSegHandle dh;
        if (segcache_acquire(&dh, donor_path, 0, 0, 0) != 0) {
            free(free_offs);
            free(free_caps);
            segcache_release(&rh);
            return -1;
        }
        size_t donor_size = dh.map_size;
        size_t doff = 0;
        while (doff + 24 <= donor_size) {
            const uint8_t *rec = dh.map + doff;
            uint8_t flag = rec[18];
            if (flag == 1) {
                uint32_t vlen;
                memcpy(&vlen, rec + 20, 4);
                if (varlen_compact_cb(rec, (size_t)vlen, rec, &ctx) != 0) {
                    break;
                }
            }
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            doff += slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
        }
        segcache_release(&dh);
    }
```

`varlen_compact_cb`'s signature
(`int (const uint8_t *rec, size_t vlen, const uint8_t hash16[16], void *ctx)`)
already matches `od_record_cb` exactly, it's invoked only for `flag == 1`
records, and it retains no pointer beyond the call — a genuine drop-in
match. Unlike Task 4a's recipient walk (which needs the writable mmap
kept open across the whole loop to record `free_offs`/`free_caps` for
later in-place writes — a requirement `seg_scan_o_direct_varlen` doesn't
support), the donor scan is read-only start to finish, so routing it
through Task 2's now-resync-hardened `seg_scan_o_direct_varlen()`
eliminates a third copy of the same validation/resync logic rather than
adding one. This also picks up O_DIRECT page-cache bypass for what was
previously a plain mmap walk.

This is safe with respect to write visibility: every VARLEN write
(`seg_write_record_varlen`, called with `sync_now = 1` from
`slotcask_insert`) is `durability_msync_range`'d before the write call
returns, so an O_DIRECT read of the donor (bypassing page cache) always
sees fully-committed data — no staleness risk from mixing MAP_SHARED
writes with a separate O_DIRECT reader on the same file. If, at fix
time, `sync_now` is not in fact always `1` for VARLEN writes on the path
that reaches this donor scan, halt per the execution rules — this design
depends on that guarantee holding.

### Test-first

Create `src/test/cases/test_varlen_compact_donor_resync.c` (full code
below): same fixture as Tasks 3/4a, structured so the fixture file
becomes the **donor** in a compaction pass (not the recipient). Run
against `main` first, confirm failure (or confirm it already passes
because the existing raw walk happens not to hit this fixture's exact
gap shape — either outcome must be stated explicitly, not silently
assumed); apply the fix; confirm pass.

### Implementation

Anchor (`src/db/slotcask.c`), current exact text (the whole donor block,
immediately following the `VarlenCompactCtx ctx = { ... };` initializer
and its preceding stale-reasoning comment block):

```c
    /* Scan donor using the fixed O_DIRECT scanner with db->slot_size.
       For varlen, this strides by slot_size, which is the fixed max-slot
       size.  Most records are smaller, so we'll skip padding regions
       (flag=0) just like the fixed format does.  Records are still at
       variable offsets but the scanner doesn't need to care — it only
       processes flag==1 records, which have their header set correctly
       at their real offset.  The key insight: in varlen format, each
       record occupies exactly its padded size, and flag=0 regions between
       records don't exist (no fixed slot grid).  However, seg_scan_o_direct
       uses slot_size stride which is wrong for varlen records that are
       shorter than slot_size.  So we use a simpler mmap-based scan
       instead for the donor. */
    /* Donor: mmap walk of headers (not O_DIRECT — varlen records aren't
       at fixed stride).  Since the donor will be unlinked after migration,
       cache pollution is short-lived. */
    {
        SlotcaskSegHandle dh;
        if (segcache_acquire(&dh, donor_path, 0, 0, 0) != 0) {
            free(free_offs);
            free(free_caps);
            segcache_release(&rh);
            return -1;
        }
        size_t donor_size = dh.map_size;
        size_t doff = 0;
        while (doff + 24 <= donor_size) {
            const uint8_t *rec = dh.map + doff;
            uint8_t flag = rec[18];
            if (flag == 1) {
                uint32_t vlen;
                memcpy(&vlen, rec + 20, 4);
                if (varlen_compact_cb(rec, (size_t)vlen, rec, &ctx) != 0) {
                    break;
                }
            }
            uint16_t klen;
            uint32_t vlen;
            memcpy(&klen, rec + 16, 2);
            memcpy(&vlen, rec + 20, 4);
            doff += slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
        }
        segcache_release(&dh);
    }
```

Replace with:

```c
    /* Donor: read-only, so route through the O_DIRECT VARLEN scanner
       (io_direct.c), which already validates every header and
       transparently resyncs across reused-slot zero-padding gaps —
       reusing that logic here instead of a third raw-walk copy. */
    {
        int drc = seg_scan_o_direct_varlen(donor_path, db->slot_size,
                                           varlen_compact_cb, &ctx);
        if (drc < 0) {
            /* Scan-level failure (I/O error, or the resync window was
               exhausted without finding a valid header past a desync).
               Some region of the donor could not be fully accounted
               for — must not let the caller unlink a donor whose live
               records may be incomplete. */
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "compact_migrate_records_varlen: donor scan failed for %s (rc=%d)",
                      donor_path, drc);
            free(free_offs);
            free(free_caps);
            segcache_release(&rh);
            return -1;
        }
        /* drc == 0: full scan completed. drc == 1: varlen_compact_cb
           stopped the scan early (ctx.rc != 0, e.g. recipient free-slot
           list exhausted) — the existing ctx.rc return path below
           already reports that outcome; no special-casing needed here. */
    }
```

This removes the now-unused `SlotcaskSegHandle dh` / `donor_size` /
`doff` locals entirely (they no longer exist anywhere else in the
function). `seg_scan_o_direct_varlen` is declared in `io_direct.h`,
already included by `slotcask.c` — no new include needed for this call.

Add the deterministic post-repoint crash seam required by Task 7 to the
VARLEN `varlen_compact_cb` only, immediately after its existing
`kf_repoint_at_slot` call and `kfcache_release`:

```c
    kf_repoint_at_slot(&kh, kf_slot_idx, (uint8_t)c->stream_id,
                       (uint16_t)c->recipient_fid, target_off);
    kfcache_release(&kh);
    durability_test_pause(c->db->data_dir, "compact-after-kf-repoint");
    return 0;
```

The existing `compact-after-recipient-sync` pause remains the first crash
point (after the recipient record is durable, before KF repoint). The new
phase is the second point (after KF repoint, before the caller can unlink
the donor). The named phase is deliberately distinct from the fixed-format
callback's same-shaped code.

### Task 4b test

Create `src/test/cases/test_varlen_compact_donor_resync.c` (including
`varlen_compact_fixture.h`):

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "varlen_compact_fixture.h"
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

static int test_varlen_compact_donor_resync_run(void) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/shard-db-varlen-compact-donor-%d", (int)getpid());
    mkdir(tmpdir, 0755);

    slotcask_init(64, 64);

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    int ret = slotcask_open(&db, tmpdir, 8, 1, 8192);
    ASSERT_EQ_INT(ret, 0, "slotcask_open");
    ret = slotcask_migrate_to_varlen(&db);
    ASSERT_EQ_INT(ret, 0, "migrate_to_varlen");

    ret = varlen_compact_fixture_build(&db);
    ASSERT_EQ_INT(ret, 0, "build rotated donor/recipient fixture");

    int dropped = 0;
    ret = slotcask_compact_segs(&db, &dropped);
    ASSERT_EQ_INT(ret, 0, "compact_segs completes without an error");

    void *val_out;
    size_t vlen_out;
    ret = slotcask_get(&db, "kkeyC", 5, &val_out, &vlen_out);
    ASSERT_EQ_INT(ret, 0, "get C after donor-side compaction");
    if (ret == 0) {
        ASSERT_EQ_INT((int)vlen_out, 6, "C vlen intact");
        ASSERT_TRUE(memcmp(val_out, "cvalue", 6) == 0, "C value intact");
        free(val_out);
    }

    slotcask_close(&db);
    slotcask_shutdown();

    char rmcmd[512];
    snprintf(rmcmd, sizeof(rmcmd), "rm -rf \"%s\"", tmpdir);
    system(rmcmd);

    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-donor-resync", test_varlen_compact_donor_resync_run)
```

Additionally, create
`src/test/cases/test_varlen_compact_donor_preserved_on_desync.c`,
proving the scanner-level half of the "must not delete donor on
unresolved desync" requirement directly: a hand-crafted file (like Task
2's test — the real insert/delete API cannot produce an unrecoverable
desync, since real writes always leave a real record within one segment
cap of any gap) containing one valid live record followed by
`max_slot_size` bytes of `0xFF` (which fails
`flag <= 2`, so `seg_scan_varlen_resync` can never find a valid target
within the window), asserting `seg_scan_o_direct_varlen()` returns a
negative value on that file:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include "seg_scan_varlen.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int noop_cb(const uint8_t *rec, size_t vlen,
                    const uint8_t hash16[16], void *ctx) {
    (void)rec; (void)vlen; (void)hash16; (void)ctx;
    return 0;
}

static int test_varlen_compact_donor_preserved_on_desync_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-varlen-donor-desync-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    uint8_t hash[16];
    compute_hash_raw("kd", 2, hash);
    uint8_t hdr[24];
    memcpy(hdr, hash, 16);
    uint16_t klen = 2;
    uint32_t vlen = 1;
    memcpy(hdr + 16, &klen, 2);
    hdr[18] = 1;
    hdr[19] = 0;
    memcpy(hdr + 20, &vlen, 4);
    fwrite(hdr, 1, 24, f);
    fwrite("kd", 1, 2, f);
    fwrite("v", 1, 1, f);
    uint8_t z[5] = {0}; /* pad 27 -> 32 */
    fwrite(z, 1, 5, f);

    const size_t max_slot_size = 64;
    /* Unrecoverable desync: max_slot_size bytes of 0xFF, none of which
       decode to flag <= 2. */
    uint8_t *junk = malloc(max_slot_size);
    ASSERT_NOT_NULL(junk, "alloc junk region");
    if (junk) {
        memset(junk, 0xFF, max_slot_size);
        fwrite(junk, 1, max_slot_size, f);
        free(junk);
    }
    fclose(f);

    int rc = seg_scan_o_direct_varlen(path, max_slot_size, noop_cb, NULL);
    ASSERT_TRUE(rc < 0, "scan reports failure on an unrecoverable desync, "
                         "instead of silently truncating (the exact "
                         "condition compact_migrate_records_varlen relies "
                         "on to avoid deleting a donor with unaccounted "
                         "live records)");

    unlink(path);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-varlen-compact-donor-preserved-on-desync",
              test_varlen_compact_donor_preserved_on_desync_run)
```

Note: this test only proves the scanner contract Task 4b's fix depends
on (`drc < 0` on unrecoverable desync). The donor-file-not-unlinked half
of the guarantee is structural, not independently tested here: by
inspection, `compact_migrate_records_varlen` returns -1 whenever
`drc < 0` (this task's fix), and its caller,
`compact_one_stream_varlen`, must be re-confirmed at fix time to still
gate the donor's `unlink()` call behind
`compact_migrate_records_varlen`'s return value being 0 (Task 3 touches
nearby code in the same function). Building a live donor with a
*guaranteed*-unrecoverable desync through the real insert/delete API is
not possible (real writes never leave an un-resyncable gap), and a
hand-crafted donor file can't be fed through the full
`slotcask_registry`/kf-repointing pipeline without also hand-crafting a
matching keyfile — flag this as a known test-depth limit to the human
during review, not a silent gap. The executor must paste, in the Task 4b
report, the exact post-fix excerpt of `compact_one_stream_varlen`
showing the `unlink()` call still gated on
`compact_migrate_records_varlen`'s return value.

### Verification (paste both outputs)

Same procedure as Task 3/4a for `test_varlen_compact_donor_resync.c`.
For `test_varlen_compact_donor_preserved_on_desync.c`: this exercises
Task 2's already-fixed scanner, so run it once after Task 2 lands (it
should already pass) and again after Task 4b lands (still passing,
confirming Task 4b's `drc < 0` check has something real to check
against) — paste both runs.

## Task 5: startup format sweep — unconditional FIXED→VARIABLE conversion

### Root cause

`shard_db_startup_migrate()` (`src/db/embedded.c`) short-circuits at:

```c
    if (decision == SHARD_DB_VERSION_NOOP) return 0;
```

*before* ever reaching the `#if SHARD_DB_HAS_STARTUP_MIGRATION` block
that calls `run_startup_migration()`. `shard_db_version_decide()`
returns `SHARD_DB_VERSION_NOOP` whenever the on-disk `.version` already
matches the compiled-in `SHARD_DB_VERSION` — which is true on every
normal restart after the first startup on this release. A FIXED-format
object created (or restored from an older backup, or created by a
client bypassing this daemon's own create-object path) *after* that
first startup never gets swept to VARIABLE, because every subsequent
startup takes the NOOP short-circuit before any migration logic runs at
all. This directly contradicts this plan's premise that resync-hardening
matters because production is on VARIABLE format — a FIXED object
sitting untouched defeats the whole point silently.

### Test-first

Create `src/test/cases/test_startup_format_sweep.c` (full code below):
force-create an object in FIXED format, write a few records, then call
`shard_db_startup_migrate()` again (simulating a second, NOOP-decision
startup) and assert the object is now VARIABLE format with all records
intact. Run against `main` first — confirm it fails (object remains
FIXED after the second startup call); apply the fix; confirm it passes.

### Implementation

Anchor (`src/db/embedded.c`), current exact text:

```c
int shard_db_startup_migrate(const char *db_root,
                             char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int empty = db_root_is_empty(db_root);
    if (empty < 0) return -1;
    int present = (read_rc == SHARD_DB_VERSION_FILE_OK);

    if (read_rc == SHARD_DB_VERSION_FILE_ERROR && !empty) return -4;
    int decision = shard_db_version_decide(
        present ? disk_version : NULL, present, empty,
        SHARD_DB_VERSION, SHARD_DB_MIN_VERSION,
        SHARD_DB_HAS_STARTUP_MIGRATION);
    if (decision == SHARD_DB_VERSION_DOWNGRADE && out_disk_version)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    if (decision < 0) return decision;
    if (decision == SHARD_DB_VERSION_NOOP) return 0;
#if SHARD_DB_HAS_STARTUP_MIGRATION
    if (decision == SHARD_DB_VERSION_RUN_MIGRATION &&
        run_startup_migration(db_root) != 0)
        return -1;
#endif
    return shard_db_version_file_write(db_root, SHARD_DB_VERSION) == 0 ? 0 : -1;
}
```

Replace with:

```c
int shard_db_startup_migrate(const char *db_root,
                             char *out_disk_version, size_t out_sz) {
    char disk_version[64] = {0};
    int read_rc = shard_db_version_file_read(db_root, disk_version,
                                              sizeof(disk_version));
    int empty = db_root_is_empty(db_root);
    if (empty < 0) return -1;
    int present = (read_rc == SHARD_DB_VERSION_FILE_OK);

    if (read_rc == SHARD_DB_VERSION_FILE_ERROR && !empty) return -4;

    int decision = shard_db_version_decide(
        present ? disk_version : NULL, present, empty,
        SHARD_DB_VERSION, SHARD_DB_MIN_VERSION,
        SHARD_DB_HAS_STARTUP_MIGRATION);
    if (decision == SHARD_DB_VERSION_DOWNGRADE && out_disk_version)
        snprintf(out_disk_version, out_sz, "%s", disk_version);
    if (decision < 0) return decision;

    /* Runs on every accepted startup with a non-empty schema.conf,
       independent of the .version decision — a matching .version only
       means this release's index-rebuild migration already ran once; it
       says nothing about whether every object is on VARIABLE format (an
       object can be created, or restored from backup, in FIXED format at
       any later point). This is the standing invariant-enforcement sweep,
       not a per-release migration step. Version refusal must happen first:
       a newer/unsupported database must not be mutated before rejection. */
    if (!empty && run_startup_format_sweep(db_root) != 0) return -1;

    if (decision == SHARD_DB_VERSION_NOOP) return 0;
#if SHARD_DB_HAS_STARTUP_MIGRATION
    if (decision == SHARD_DB_VERSION_RUN_MIGRATION &&
        run_startup_migration(db_root) != 0)
        return -1;
#endif
    return shard_db_version_file_write(db_root, SHARD_DB_VERSION) == 0 ? 0 : -1;
}
```

Operational exception: when a materialized object's schema stream count
differs from the on-disk stream-directory count, the sweep must log the
mismatch and leave that object untouched for the existing vacuum/rebuild
handoff. Opening it with the schema value could route new writes to the
wrong stream set. This is a deliberate safe deferral, not a sweep error;
all metadata, enumeration, registry, and migration I/O failures remain
fail-closed and reject startup.

Add the new function immediately before `shard_db_startup_migrate`,
anchored directly after `db_root_is_empty`'s closing brace (current
exact text of that function, for anchor purposes — not being modified):

```c
static int db_root_is_empty(const char *db_root) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/schema.conf", db_root);
    FILE *f = fopen(path, "r");
    if (!f) return errno == ENOENT ? 1 : -1;

    char line[4096];
    int has_object = 0;
    while (fgets(line, sizeof(line), f)) {
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || *p == '\n' || *p == '\0') continue;
        /* Any non-comment schema content makes this a non-empty root.
           Malformed metadata must not obtain the empty-root bypass. */
        has_object = 1;
        break;
    }
    if (ferror(f) || fclose(f) != 0) return -1;
    return has_object ? 0 : 1;
}
```

Insert the new function directly after it:

```c
/* Convert every FIXED-format object to VARIABLE, unconditionally, on
 * every startup with a non-empty schema.conf. Mirrors the "migrate"
 * JSON mode's semantics (idempotent — no-op if already VARIABLE) and
 * its objlock_wrlock/slotcask_migrate_to_varlen/objlock_wrunlock
 * sequence; unlike server.c's "migrate" handler (which relies on the
 * outer mode_is_schema dispatch already holding the lock), this sweep
 * has no outer dispatch to rely on and takes the lock itself. */
static int run_startup_format_sweep(const char *db_root) {
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *f = fopen(schema_path, "r");
    if (!f) return 0; /* no schema.conf — nothing to sweep */

    char line[4096];
    int failed = 0;
    while (!failed && fgets(line, sizeof(line), f)) {
        line[strcspn(line, "\n")] = '\0';
        char *p = line;
        while (*p == ' ' || *p == '\t') p++;
        if (*p == '#' || !*p) continue;

        /* Format: dir:object:splits:max_key:2:streams[...] */
        char *c1 = strchr(p, ':');
        if (!c1) { failed = 1; break; }
        *c1 = '\0';
        char *c2 = strchr(c1 + 1, ':');
        if (!c2) { failed = 1; break; }
        *c2 = '\0';

        const char *dir = p;
        const char *obj = c1 + 1;

        char obj_data[PATH_MAX];
        snprintf(obj_data, sizeof(obj_data), "%s/%s/%s", db_root, dir, obj);

        /* Objects with no materialised data have no format to convert. */
        char kf_probe[PATH_MAX];
        snprintf(kf_probe, sizeof(kf_probe), "%s/data/kf", obj_data);
        struct stat kf_st;
        if (stat(kf_probe, &kf_st) != 0) {
            if (errno == ENOENT) continue;
            failed = 1;
            break;
        }

        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);
        Schema sch = load_schema(eff_root, obj);
        if (sch.splits <= 0) { failed = 1; break; }

        SlotcaskSchemaInfo info = {
            .splits    = sch.splits,
            .slot_size = sch.slot_size,
            .streams   = sch.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(eff_root, obj, &info);
        if (!sdb) { failed = 1; break; }

        if (sdb->format == SLOTCASK_FORMAT_FIXED) {
            objlock_wrlock(eff_root, obj);
            int mrc = slotcask_migrate_to_varlen(sdb);
            objlock_wrunlock(eff_root, obj);
            if (mrc != 0) {
                LOG_ERROR(LOG_SUB_SLOTCASK,
                          "startup format sweep: migration failed for %s/%s",
                          dir, obj);
                failed = 1;
            } else {
                LOG_INFO(LOG_SUB_SLOTCASK,
                         "startup format sweep: migrated %s/%s to VARIABLE",
                         dir, obj);
            }
        }
    }
    if (ferror(f)) failed = 1;
    if (fclose(f) != 0) failed = 1;

    return failed ? -1 : 0;
}
```

If `load_schema`'s exact signature/return type (`Schema` vs a pointer
form), `SlotcaskSchemaInfo`'s exact field names, or
`slotcask_registry_get`'s exact signature differ from what's assumed
above, re-confirm against the current headers before writing this
function — do not guess field names; if they've changed since this plan
was written, halt per the execution rules.

The sweep and the version-gated migration must share the registry's
existing `SlotcaskDb` handle safely: after the sweep changes a FIXED
object to VARIABLE, a startup that also takes the version-migration path
must observe VARIABLE and not attempt a second format migration or
reopen the same object unsafely. Add a test run with an older supported
`.version` in addition to the NOOP case below; assert the format marker
and all records remain correct after both phases.

Also update the stale startup comment in `shard_db_open` (anchor: the
comment beginning `/* Auto-migrate any FIXED-format objects before thread
   pools start.`) to this complete text:

```c
    /* Sweep FIXED-format objects before thread pools start, then run this
       release's version-gated startup migration when required. The format
       invariant is checked on every accepted startup; matching .version
       only suppresses the per-release index-rebuild batch. */
```

### Task 5 test

Create `src/test/cases/test_startup_format_sweep.c`. Exact API for
forcing a FIXED-format object creation and calling
`shard_db_startup_migrate` must be confirmed against `embedded.c`'s
public header during execution (`grep -n shard_db_startup_migrate
src/db/*.h`); the test must:

1. Create a fresh `db_root` with a schema.conf entry for one object,
   `data/kf` materialized (e.g. via the same public API the "migrate"
   JSON mode / `create-object` path uses, NOT direct `slotcask_open`
   calls that bypass schema.conf registration — this test exercises
   `shard_db_startup_migrate`'s own schema.conf-driven walk, so the
   object must be reachable through it).
2. Ensure the object starts in FIXED format (default at create time —
   confirm the current default hasn't already changed to VARIABLE by
   the time this executes; `docs/plans/2026-08-07-varlen-default-and-fixed-removal.md`
   is a separate, not-yet-executed plan per "Out of scope" below — if it
   has landed by the time this executes, force FIXED explicitly via
   whatever knob it left in place, and note the discrepancy in
   `PLAN_NOTES.md` rather than silently adjusting this plan's
   assumption).
3. Insert a few records.
4. Write `$DB_ROOT/.version` to `SHARD_DB_MIN_VERSION` (or another
   confirmed older supported version) and call
   `shard_db_startup_migrate(db_root, NULL, 0)`. This exercises the
   format sweep and the version-gated migration in the same startup, so
   the registry handle is tested across both phases.
5. Assert the object's format is now VARIABLE (via the "migrate" JSON
   mode's read path, or a direct `slotcask_registry_get` + `->format`
   check) and every inserted record still reads back correctly.
6. Write `$DB_ROOT/.version` to the current `SHARD_DB_VERSION` and call
   `shard_db_startup_migrate(db_root, NULL, 0)` again. This second call
   must take the NOOP path while leaving the VARIABLE marker and all
   records unchanged, proving the unconditional sweep is independent
   of the version decision and does not double-migrate the object.

If step 1's "public API" turns out to require a running server rather
than in-process calls, use the JSON-mode dispatch functions directly (as
other embedded-mode tests in this suite already do — grep
`src/test/cases/` for an existing embedded-mode create-object test to
match its exact setup pattern) rather than starting a real daemon
process. Confirm the exact pattern during execution; if no suitable
precedent exists, stop and ask rather than inventing a new test-setup
convention.

### Verification (paste both outputs)

Run against `main` first, confirm failure (object still FIXED after the
second `shard_db_startup_migrate` call); apply the fix; rebuild and
rerun, confirm VARIABLE format and intact records. Also rerun the existing
`startup-migration-refuses-downgrade` case and verify that a newer/too-old
`.version` still refuses startup without changing the object's `.format`
marker; this guards the version-gate ordering fixed above.

### Documentation updates

Both currently describe only the index-rebuild half of automatic startup
migration; both need a sentence added about the format sweep.

**`docs/getting-started/install.md`**, anchor (current exact text):

```
As of 2026.08.1, startup migration is automatic — the daemon compares
`$DB_ROOT/.version` against its compiled-in version and runs the full index
rebuild in-process on start. The standalone `./migrate` binary is removed.
The minimum supported source release is 2026.07.3; this is recorded for
operators but is informational and not enforced in this release because
earlier releases did not write `.version`. `./shard-db reindex` remains available
for on-demand use.
```

Append a new paragraph immediately after:

```
Independent of that `.version`-gated index rebuild, every startup with a
non-empty `schema.conf` also sweeps every object for FIXED-format
segments and converts them to VARIABLE in place (idempotent, matching
the `migrate` JSON mode's semantics) — this runs unconditionally, not
just on a version change, so an object created or restored in FIXED
format is always caught on the next restart.
```

**`docs/operations/deployment.md`**, anchor (current exact text, single
paragraph near line 263):

```
As of 2026.08.1, startup migration is automatic — the daemon compares `$DB_ROOT/.version` against its compiled-in version and runs the full index rebuild in-process on start. The standalone `./migrate` binary is removed. The minimum supported source release is 2026.07.3; this is recorded for operators but is informational and not enforced in this release because earlier releases did not write `.version`. `./shard-db reindex` remains available for on-demand use. Once your objects are on the slotcask engine and compact format, point-release upgrades are a binary swap. On startup the daemon sweeps stale `.new` rebuild artifacts from interrupted resplits/vacuum runs before accepting connections. Operators who upgraded from a pre-2026.07.1 build affected by kf corruption must run the 2026.07.1 release's `rebuild-kf` against a backup before upgrading past this release.
```

Insert a new sentence after "`./shard-db reindex` remains available for
on-demand use." and before "Once your objects are on the slotcask
engine...":

```
Independent of that `.version`-gated rebuild, every startup with a
non-empty `schema.conf` also sweeps every object for FIXED-format
segments and converts them to VARIABLE in place — unconditional on every
restart, not gated by `.version`, so a FIXED-format object created or
restored after the daemon's first post-upgrade startup is still caught.
```

(`docs/getting-started/configuration.md:204` describes the unrelated
legacy v1→v2 `probe-into-slot` migration and does not need a change.)

## Task 6: dynamic-safety tooling gate

This diff touches shared/cached state (segcache reads across resync
rebuild, kfcache during recipient repointing) and a background
prefetch-worker thread's `DbCtx` lifetime (Task 2's destroy-then-rebuild
sequence). Per this repo's AGENTS.md standing exception, run both gates
locally against at least the tests added/changed by this plan before
calling any task done — not deferred to CI.

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2 --filter varlen
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2 --filter startup-format-sweep
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2   # full suite, not just the filtered subset
```

```bash
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1 --filter varlen
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1 --filter startup-format-sweep
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1   # full suite
```

Paste all four outputs (ASan filtered + full, TSan filtered + full). Any
new finding gets root-caused and either fixed now (if simple) or written
up as a new `docs/plans/<date>-<slug>.md` and, only if deliberately
deferred, added to `.tsan.supp` with a named-function suppression and a
full rationale paragraph — never a blanket suppression.

## Task 7: crash / partial-compaction tests and production verification

### Crash/partial-compaction regression tests

Create `src/test/cases/test_varlen_compact_crash_mid_migration.c`.
Use the existing `durability_test_pause` marker-file convention already
used by `test_durability_ordering.c`: the child configures
`db->durability_test_pause_phase` and `db->durability_test_pause_ms`, the
parent waits for `.durability-test-<phase>.active`, then sends SIGKILL.
Do not use a timing-only sleep. Run the child twice against the fixture
from `varlen_compact_fixture.h`, once with
`compact-after-recipient-sync` and once with
`compact-after-kf-repoint`. After each simulated kill, reopen the database
(simulating restart) and assert: no record is lost, no record is
duplicated, and a subsequent `slotcask_compact_segs` call completes
cleanly. The second phase is implemented by the exact production hook
added in Task 4b above; without observing its marker, the test must fail
and report that the requested injection point was not reached.

Run the existing first pause point against `main` first and report whether
the current recovery machinery preserves the records. The
`compact-after-kf-repoint` marker is intentionally new in Task 4b, so its
absence on `main` is an instrumentation gap, not evidence that recovery
failed or passed; record that baseline as "not instrumented" and run the
second crash case only after the hook lands. For each instrumented case,
state explicitly which outcome occurred: (1) the test exposes a real
recovery gap (fails) — root-cause it per the standard task-quality bar
before it's checked in; or (2) the existing commit-intent marker / abort
sidecar machinery already covers it and the test passes — keep it as a
standing regression test even if this plan did not need to change the
recovery logic.

### Production verification (exact commands)

Run against a **copy** of production data, never the live directory.

```bash
# 1. Snapshot production DB_ROOT to a scratch copy (adjust source path).
rsync -a --delete /path/to/production/db_root/ /path/to/scratch/db_root_copy/

# 2. Record pre-fix baseline counts per affected object (repeat per dir/object).
#    Point the CLI explicitly at the production snapshot; do not rely on
#    db.env's DB_ROOT, and do not substitute `size` (disk bytes can change
#    during reindex and is not a record-count invariant).
DB_ROOT=/path/to/production/db_root \
  ./build/bin/shard-db count <dir> <object> > /tmp/pre_fix_counts_<object>.txt

# 3. Point the fixed binary at the scratch copy and run a full reindex,
#    which exercises the same VARLEN scan paths this plan changes.
DB_ROOT=/path/to/scratch/db_root_copy ./build/bin/shard-db reindex

# 4. Confirm no scan errors/crash-loop in the daemon log for the reindex run
#    (adjust log path/grep per this repo's LOG_LEVEL config).
grep -E 'unrecoverable desync|seg_stat_one_varlen|od_varlen_resync_find' \
  /path/to/scratch/db_root_copy/*.log || echo "no resync warnings logged"

# 5. Re-run counts against the scratch copy post-fix and diff against baseline.
DB_ROOT=/path/to/scratch/db_root_copy \
  ./build/bin/shard-db count <dir> <object> \
  > /tmp/post_fix_counts_<object>.txt
diff /tmp/pre_fix_counts_<object>.txt /tmp/post_fix_counts_<object>.txt

# 6. Cross-check against the kf-header source of truth directly (per
#    AGENTS.md's "Record counts (v2)" section — live = total - deleted,
#    summed from kf shard headers, independent of the segment-scan path
#    this plan changes) to confirm the fix didn't just make the scanner
#    agree with itself:
DB_ROOT=/path/to/scratch/db_root_copy \
  ./build/bin/shard-db query '{"mode":"stats"}' | grep -A2 '"<object>"'
```

Confirm previously-failing objects (the incident's affected-object list,
if still available from the incident record) now scan clean with zero
`-EIO` aborts and zero silently-dropped files, and that live-record
counts match between the reindex run and the kf-header-derived stats for
every affected object. Paste the diff output (should be empty) and the
grep output from step 6.

## Out of scope

`docs/plans/2026-08-07-varlen-default-and-fixed-removal.md` covers
changing `create-object`'s default format to VARIABLE and removing
FIXED-format code entirely — a separate, larger change that depends on
this plan's resync-hardening landing first (removing FIXED-format code
while FIXED objects can still exist in the wild, per Task 5's own
finding that objects can persist in FIXED format indefinitely, would be
premature). Not addressed here.

## Review history

**Revision 1 → Revision 2**: addressed a code review that found 5
blockers (flag==0 falsely terminating resync; a 4 MiB resync window
against a 128 MiB segment cap; `seg_stat_one_varlen` not propagating
scan failure to its already-correct caller guard, enabling silent donor
deletion; Tasks 3/4a/4b lacking dedicated regression tests; prose
patches and a literal `...` elision violating this repo's anchor/code-
block plan format) and 5 additional required changes (alignment
clarification in the resync search; Task 5's version-gating fix needing
to be unconditional, not just extending the already-gated migration
path; documentation updates for the new sweep behavior; explicit crash/
partial-compaction tests and exact verification commands; rewritten
execution rules matching this repo's actual standing exceptions). All
incorporated above. The diagnostics/logging refactor already merged to
`main` (process-global log handler for embedded mode) is unrelated,
intentional prior work, out of scope for this plan.

**Revision 2 → Revision 3**: corrected the Task 3 comment-block compile
error; made resync restarts use a buffered descriptor because recovered
record offsets are not O_DIRECT-sector aligned; excluded flag==0 sparse
padding from the recipient free list; moved the startup format sweep
after version refusal checks; replaced the no-op/invalid compaction
fixtures with an exact three-segment public-API fixture; added the
post-KF-repoint crash pause seam; and made production verification
commands explicitly target the production snapshot and scratch copy.

**Revision 3 → Revision 4**: replaced the incorrect segment-file-sized
resync bound with the owning object's cached `db->slot_size`; parameterized
the shared structural validator and resync helper with that bound; threaded
`max_slot_size` through `seg_scan_o_direct_varlen()` and every production
caller; expanded the O_DIRECT resync buffer only enough to validate a
candidate record; updated all tests to use a small schema-specific bound;
and documented why healthy point reads have no new work while full scans
retain only the existing scalar plausibility check.

**Revision 4 → Revision 5**: added the eight explicit `build.sh` test-list
entries required by this repository; made both O_DIRECT scan sites and
`seg_stat_one_varlen` resync on structurally valid flag==0 padding instead
of striding through it; replaced vacuous compaction return assertions with
exact success assertions; clarified that the startup sweep and
version-gated migration must share the registry handle without a second
format migration; and extended the startup test to cover both migration
and NOOP decisions.

**Revision 5 → Revision 6**: added the missing `padding_desync` declaration
to the replacement O_DIRECT scanner block, not only the captured current
implementation anchor.
