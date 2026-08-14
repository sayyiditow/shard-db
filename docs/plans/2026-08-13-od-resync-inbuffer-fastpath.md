# do_resync in-buffer fast path

Branch: `refactor/variable-only-segments`
Execution mode: per this repo's standing exception — leave all work
**uncommitted**. Build with `SKIP_TESTS=1 ./build.sh`; test with
`./build/bin/shard-db-test run[-all]`. If a quoted anchor below isn't
found exactly in the file at edit time, write `PLAN_NOTES.md` describing
the mismatch and halt the entire run immediately — do not guess,
reinterpret, or continue to any other task. If you hit a decision this
plan doesn't cover, stop and ask.

This plan is a direct follow-up to
`docs/plans/2026-08-13-varlen-pool-capacity-mismatch.md`, whose
post-implementation correction explains why that fix (kept, and still a
real bug fix) does not resolve the `keys`-latency regression, and points
here for the actual fix.

## Root cause

`seg_scan_o_direct` (`src/db/io_direct.c`) scans a segment file's
records via a double-buffered O_DIRECT prefetch: two large aligned
buffers (`ODIRECT_BUF_SIZE_DEFAULT = 32 MiB`, `io_direct.h:25`), one
being read by the main thread while a worker thread prefetches the
other.

Whenever the scanner's main stride loop lands on a record header with
`flag == 0` (a zeroed gap — the padding tail of an undersized record
written into an oversized free-pool slot, or a genuine reused-but-not-
yet-rewritten slot) it currently treats this identically to real
corruption: it jumps to the `do_resync:` label, which does a **full
teardown and rebuild of the double-buffered prefetch context** —
`dbctx_destroy` (joins the prefetch worker thread, frees both 32 MiB
buffers, destroys the mutex/cond), `od_disable_odirect(fd)`, a fresh
blocking `pread`-based probe via `od_varlen_resync_find()`, then
`dbctx_init()` and (unless the remaining file fits in one shot) a new
`pthread_create` + `dbctx_kickoff()` — before resuming the scan.

This is the right machinery for a **genuine, unrecoverable desync**
(corruption, or a gap whose header straddles a chunk boundary so it
can't be resolved from the buffer alone). It is drastically overkill for
the **common case**: a small zeroed gap that is still fully contained in
the chunk already sitting in memory, where the next real record header
is only a few dozen bytes further into the very buffer the loop is
already iterating over.

Confirmed via the debug instrumentation already present in
`seg_scan_o_direct` (`dbg_resync_count` + its `fprintf`s at the
`do_resync:` and `done:` labels, added earlier this investigation):
running `test-varlen-pool-donation-stride`'s 8,000-op sequential mixed
insert/update/delete workload produces **43–134 resyncs per stream
file** across the object's 8 streams, each paying the full
teardown/rebuild cost, for an observed `keys` latency of **3.97 s**.
Byte-level inspection of the affected segment files (this investigation,
prior session) confirmed these gaps are typically a handful of 8-byte
words — always far smaller than the 32 MiB chunk already in memory, and
never anywhere near a chunk boundary in the vast majority of cases,
since `ODIRECT_BUF_SIZE_DEFAULT` is many orders of magnitude larger than
any individual record.

The exact primitive needed to resolve these in-chunk gaps cheaply
already exists and is already used for the slow-path probe:
`seg_scan_varlen_resync()` in `src/db/seg_scan_varlen.h` — a
side-effect-free, header-only helper that searches forward from a given
position in an in-memory buffer for the next structurally-valid,
hash-verified `flag ∈ {1,2}` record header, correctly skipping
`flag == 0` candidates. It takes a plain `(map, map_size, pos,
max_slot_size, window, *out_off)` — no file handle, no I/O — so it can
be called directly on the chunk buffer the main stride loop already has
in hand.

## Fix

In `seg_scan_o_direct`'s main stride loop, before falling through to the
expensive `do_resync:` path, first attempt the same resync search
directly against the in-memory `chunk` buffer. Only fall back to the
existing teardown-and-rebuild path if that in-buffer search fails to
find a valid record within the buffer (which correctly covers both a
gap that runs to the end of the chunk, and genuine corruption/EOF).

**Scope**: this fast path applies **only** to the main stride loop's
desync check (`src/db/io_direct.c`, currently lines 785–789). It does
**not** apply to the carry-reassembly desync check (currently lines
739–743), where the candidate record's header was reconstructed from
bytes split across the previous chunk and the new one — by construction
that candidate isn't a plain slice of one in-memory buffer, and
`seg_scan_varlen_resync` can't be pointed at it without extra buffer
gymnastics for a case this investigation's data shows is comparatively
rare (32 MiB chunks vs. tens-of-bytes records — boundary-straddling
desyncs are a small minority of the 43–134/file total). That path is
untouched by this plan.

### Edit 1 — `src/db/io_direct.c`, main stride loop desync check

Anchor (current exact text):

```c
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
```

Replace with:

```c
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
                /* Cheap in-buffer fast path: the vast majority of gaps
                   (undersized-record padding tails, tiny reused-slot
                   remnants) are fully contained in the 32 MiB chunk
                   already resident here, with the next real record only
                   a handful of bytes further in. Search this buffer
                   directly via the same primitive the slow path uses
                   (seg_scan_varlen_resync) before paying for a full
                   prefetch-context teardown/rebuild (dbctx_destroy +
                   pread probe + dbctx_init [+ pthread_create]) via
                   do_resync below. Only genuinely unresolvable gaps —
                   one that runs past the end of this chunk, or actual
                   corruption — fall through to the slow path, which is
                   unchanged. */
                size_t remain = (size_t)chunk_len - pos;
                size_t window = (remain < max_slot_size) ? remain : max_slot_size;
                size_t next_in_chunk;
                if (seg_scan_varlen_resync(chunk, (size_t)chunk_len, pos,
                                           max_slot_size, window,
                                           &next_in_chunk)) {
                    dbg_fastpath_count++;
                    pos = next_in_chunk;
                    continue;
                }
                padding_desync = (flag == 0);
                resync_from = base_off + (off_t)pos;
                goto do_resync;
            }
```

### Edit 2 — temporary diagnostic counter

Add `dbg_fastpath_count` alongside the existing `dbg_resync_count`
(same lifetime/scope) so the fix's effect is directly observable during
verification, and print it next to the existing resync count at the
`done:` label. This is temporary instrumentation, in the same category
as the pre-existing `dbg_resync_count` and its `fprintf`s — all of it
gets reverted together in the cleanup pass already tracked from the
prior plan, not before.

Anchor (current exact text, `dbg_resync_count` declaration — find via
`grep -n "dbg_resync_count" src/db/io_direct.c` for the exact
declaration line, since it's part of the existing not-yet-reverted
instrumentation this plan builds on):

```c
    int dbg_resync_count = 0;
```

Replace with:

```c
    int dbg_resync_count = 0;
    int dbg_fastpath_count = 0;
```

Anchor (current exact text, `done:` label):

```c
done:
    fprintf(stderr, "[DBG seg_scan_o_direct] %s completed, resync_count=%d ret=%d\n",
            seg_path, dbg_resync_count, ret);
```

Replace with:

```c
done:
    fprintf(stderr, "[DBG seg_scan_o_direct] %s completed, resync_count=%d "
            "fastpath_count=%d ret=%d\n",
            seg_path, dbg_resync_count, dbg_fastpath_count, ret);
```

## Edge cases

- **Gap runs to (or past) the end of the current chunk.** `window` is
  clamped to `remain = chunk_len - pos`, and `seg_scan_varlen_resync`
  itself bounds every candidate via `seg_scan_varlen_struct_ok`'s
  `pos + rec_size > map_size` check — it can never return an offset
  whose record would run past `chunk_len`. If the gap doesn't resolve
  within the chunk, the call returns 0 and control falls through to the
  existing `do_resync:` path unchanged — same behavior as today.
- **Genuine corruption (not a zeroed gap).** `seg_scan_varlen_resync`
  requires hash-verification (`seg_scan_varlen_hash_ok`) in addition to
  structural validity, so it will not accept garbage as a false-positive
  resync target — identical safety guarantee to the existing slow path,
  since both call the same function. If no valid record is found in the
  window, falls through to `do_resync:` exactly as before, preserving
  the existing "flag==0 padding tail with nothing after it is the normal
  sparse EOF case, anything else is a hard error" distinction (the
  `padding_desync` flag is still computed from the *original* `flag` at
  `pos`, unchanged from current behavior).
- **Chains of consecutive gaps** (a gap immediately followed by another
  before the next live record). Handled naturally: after the fast path
  advances `pos = next_in_chunk` and `continue`s, the loop re-reads
  `flag` at the new `pos`; if that's also a gap, the same fast path
  fires again from there. No special-casing needed.
- **No infinite-loop risk.** `seg_scan_varlen_resync` only ever returns
  an offset with `flag ∈ {1,2}` (it explicitly skips `flag == 0`
  candidates internally), and the branch that calls it only runs when
  the header at the *current* `pos` failed the `flag == 0 || flag > 2 ||
  rec_size > max_slot_size` check — so a found `next_in_chunk` can never
  equal `pos` itself; it strictly increases, and the loop's own bound
  (`pos + 24 <= chunk_len`) still terminates it.
- **Thread-safety / no new concurrency surface.** `chunk` is
  `dc.buf[dc.active]`, which is exclusively owned by the main thread for
  the duration of this synchronous stride loop — the prefetch worker
  thread only ever fills `dc.buf[dc.inactive]`, and the two only swap
  roles inside `dbctx_swap()` after this loop has finished with the
  active buffer. `seg_scan_varlen_resync` only reads `chunk`; this adds
  no new shared-state access beyond what the loop already does on every
  iteration (`rec = chunk + pos`). No lock changes, no new thread
  interaction — this fix does not touch the prefetch worker's lifecycle
  at all in the common case, only bypasses invoking it.
- **`max_slot_size` semantics unchanged.** Passed through verbatim to
  `seg_scan_varlen_resync` exactly as `od_varlen_resync_find` already
  does for the slow path — no new validation logic, no risk of the fast
  and slow paths disagreeing on what counts as a valid candidate, since
  both ultimately call the same `seg_scan_varlen_struct_ok`/
  `seg_scan_varlen_hash_ok` primitives.

## Regression test

No new test file — `test-varlen-pool-donation-stride`
(`src/test/cases/test_varlen_pool_donation_stride.c`, already written
and registered under the prior plan) is the correct regression signal
for this fix. It is currently (test-first, already proven) **red** on
its `dt < 2000` latency assertion after the `pool_split_leftover` fix
landed (observed 3.97 s at 8,000 ops) — its correctness assertion
(`counted == key_count`) already passes and is untouched by this
change. This plan's fix is expected to turn that same assertion green
without modifying the test itself. Steps to prove it, pasted in full for
the record:

1. Before this plan's edits: `./build/bin/shard-db-test run
   test-varlen-pool-donation-stride` → confirm the `dt < 2000` assertion
   still fails (re-confirming the currently-red state carries into this
   plan's starting point, in case anything drifted since the prior
   session).
2. Apply Edit 1 and Edit 2 above.
3. `SKIP_TESTS=1 ./build.sh` — clean rebuild, no new warnings.
4. `./build/bin/shard-db-test run test-varlen-pool-donation-stride` →
   both assertions pass. Capture the `[DBG seg_scan_o_direct] ...
   fastpath_count=... resync_count=...` stderr lines from the run to
   confirm `dbg_fastpath_count` accounts for the bulk of what were
   previously slow-path resyncs and `dbg_resync_count` has dropped
   sharply (not necessarily to 0 — genuine boundary-straddling desyncs
   still exist and still take the slow path, by design).
5. `./build/bin/shard-db-test run test-slotcask-v2-concurrent` → confirm
   it passes (this was the original flaky/hanging test that motivated
   the whole investigation).
6. `./build/bin/shard-db-test run-all` → confirm no new failures
   introduced elsewhere (this touches a shared scan helper used by every
   sequential `keys`/`fetch`/reindex path).

## Cleanup (do this only after step 6 above is green)

Revert all temporary debug instrumentation added across this
investigation — not before, since it's the tool used to verify both
this fix and the prior one:

- `src/db/io_direct.c`: `dbg_resync_count`, `dbg_fastpath_count`, and
  their `fprintf`s at `do_resync:`/`done:` (this plan's Edit 2, plus the
  pre-existing ones from the prior session).
- `src/db/query_find.c`: the `scan_shards_v2_o_direct` entry debug
  print, the `od_seg_file_worker` timing/rc debug print, and the
  `#include <time.h>` addition if nothing else in the file needs it.
- `src/test/cases/test_slotcask_v2_concurrent.c`: the
  `fprintf(stderr, "[concurrent][debug] raw keys resp...")` line.

After cleanup: full rebuild, full `run-all` again to confirm removing
the prints didn't change behavior (they're stderr-only, should be a
no-op), then the ASan/TSan gate below.

## Dynamic-safety tooling gate (mandatory before done, per AGENTS.md)

This diff touches the O_DIRECT double-buffered prefetch worker-thread
lifecycle's *call site* (even though the fast path itself only reads an
already-owned buffer and never touches the worker), so both sanitizers
run locally against at least the affected cases before this is called
done:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

Any new finding gets root-caused and either fixed now (if simple) or
written up as its own `docs/plans/<date>-<slug>.md` — never silently
suppressed.

## After this plan

Once this lands and both regression tests are green and the sanitizer
gate is clean: proceed to the `test-request-timeout` "count tight
timeout trips" investigation and the remaining items from
`docs/plans/2026-08-07-varlen-default-and-fixed-removal.md`, per the
standing task order. The previously-reported UUID key serialization
bugs and `find total:true` gap remain explicitly deferred to a separate
future plan, not this one.
