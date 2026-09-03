# Plan: fix inline batch flush under held kf+bitmap handles in the bitmap-routed streaming find (recursive kf reader, self-deadlock)

## Status

**READY FOR HUMAN APPROVAL — full CORE-PROCESS plan (rewritten 2026-09-03,
tree 2a9f515).** Supersedes the 2026-08-27 pre-plan that previously
occupied this file. The pre-plan had the right mechanism but (a) attributed
both bitmap workers to `bitmap_emit_generic_for_shard` (the eq/IN fast
path actually drives `bitmap_emit_for_shard`), (b) sketched a test whose
sequencing ("spawn the parked mutation, then run the find") is mechanically
unrealizable — a writer holding the kf(S) wrlock excludes the walker's
initial kf(S) rd acquire, so the walker would block **before holding
anything** and base could never hang — and (c) left the collector design,
call-site inventory, and hooks at prose-bullet depth. Re-verification
against the current tree corrected all three; see Root cause and Failure
modes. Anchors below are quoted text, not bare line numbers; all were
verified 2026-09-03 against 2a9f515 (line numbers given only as
navigation aids and WILL drift — grep the symbols). If a quoted anchor is
not found exactly, follow Embedded execution rules: write `PLAN_NOTES.md`
and halt.

Do not execute until the human approves this plan explicitly.

## Root cause (verified 2026-09-03, tree 2a9f515)

When a limit-bound streaming find's primary leaf is a **bitmap** index,
`idx_find_streaming` (`src/db/query.c`) routes to the legacy executor.
The branch, quoted verbatim (navigation: `idx_find_streaming`, the
`else` after the `IT_BTREE` chunked-dispatch block):

```c
    } else {
        /* Legacy fan-out — IT_BITMAP branches and O_DIRECT leaf scans hold
         * no bt_cache lock, so inline batch flushing stays safe there. */
        btree_dispatch(db_root, object, primary_crit->field, sch->splits,
                        primary_crit,
                        resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                        stream_find_cb, &sc);
    }
```

That comment's safety argument covers only the **bt_cache lock class**. It
misses that the IT_BITMAP branches hold a *different* lock during the
walk: a kfcache **reader** on the shard being walked. Inside
`btree_dispatch`, the IT_BITMAP section is bracketed by (quoted):

```c
    if (field_index_type(db_root, object, field) == IT_BITMAP) {
```

with two fan-outs, both per-kf-shard workers:

- eq / IN fast path — `if (pc->op == OP_EQUAL || pc->op == OP_IN)`:
  `parallel_for_io(bm_shard_walk_worker, args, splits, sizeof(BmShardWalkArg));`
  → `bm_shard_walk_worker` → **`bitmap_emit_for_shard`**
- generic dict-scan (any other op): `parallel_for_io(bm_generic_shard_worker, gargs, splits, sizeof(BmGenericShardArg));`
  → `bm_generic_shard_worker` → **`bitmap_emit_generic_for_shard`**

(The pre-plan attributed both workers to `bitmap_emit_generic_for_shard`;
only the dict-scan worker drives it. The two emitters are contract-mirror
twins — the generic one's header says `Mirrors bitmap_emit_for_shard's
contract` — so the hazard is identical; the attribution is corrected
here.)

Both emitters open the shard's kf reader **up front**, then the bitmap
handle (mandatory kfcache-before-bitmap order), then invoke the caller
callback under both held handles. `bitmap_emit_for_shard`, quoted (path
build elided, marked with `...`):

```c
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kfp, sdb->slots_per_shard, 0) != 0)
        return 0;
    ...
    BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
    ...
    BmEmitCtx ec = { kh.map, kh.capacity, value, vlen, cb, ctx, 0 };
    bm_walk(bm, value, vlen, bm_emit_cb, &ec);
    ...
    bm_close(bm);
    kfcache_release(&kh);
```

The kf hold is inherent to the emit: `bm_emit_cb` resolves each set bit
to its hash through the held map —

```c
    SlotcaskKfEntry *e = &c->kf_map[slot];
    if (e->flag != 1) return 0;  /* empty / tombstoned */
    int rc = c->cb((const char *)c->val, c->vlen, e->hash, c->cb_ctx);
```

The callback handed over by the streaming find is `stream_find_cb`, whose
collector still flushes **inline on buffer-full**:

```c
    return batch_buf_collect_hash(bfb, hash16);
```

`batch_buf_collect_hash`, on `pending_n == pending_cap`, sets
`b->flushing = 1`, drops `b->lock`, and calls `batch_buf_flush_copy(b)`
**inline in the walk thread**; `batch_buf_flush_copy` calls

```c
        slotcask_bulk_resolve_and_fetch(b->sdb, copy, n,
                                        b->record_ctx, b->record_cb);
```

which calls `slotcask_bulk_resolve_hashes` — header comment: `Buckets by
shard, probes each KF shard sequentially` — performing per-shard
**kfcache acquire(s)** for every shard the buffered hashes route to,
including S. `pending_cap` is `limit`-bound (`batch_buf_init`:
`if (fetch_limit > 0 && (size_t)fetch_limit < b->pending_cap)
b->pending_cap = (size_t)fetch_limit;`), so a find with `limit:10` and
>10 candidate rows on S flushes mid-walk, deterministically.

Two hazards while kf(S)+bitmap(S) are held:

1. **Same-shard recursive reader (the deadlock).** If the flush batch
   contains hashes routed to S (guaranteed when the bait rows live on S),
   the resolver re-acquires kf(S) on the same thread that already holds
   kf(S) rd. kfcache slot locks are initialized
   `rwlock_init_writer_preferring` — `PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`
   on glibc (`src/db/slotcask.c:228`, also bitmap.c, btree.c, segcache).
   `shard_db_internal.h` documents the live invariant this violates:

   > NONRECURSIVE requires that no thread ever holds a read lock on one of
   > these and then takes a second read lock on the *same* path/slot from
   > the same thread — a recursive reader can self-deadlock behind a
   > queued writer under this policy ... Checked against every
   > acquire/release call site ... **no such recursive acquisition exists
   > today** ... any future code path that acquires the same cached file
   > twice on one thread without releasing in between would reintroduce
   > the self-deadlock risk this comment rules out today.

   This path is exactly such a code path — the invariant's call-site check
   missed it. With a writer merely **queued** on kf(S) wr, the walker's
   second rd acquire self-deadlocks under the NONRECURSIVE policy; the
   walker hangs holding bitmap(S) rd; the queued writer then needs kf(S)
   wr, which the walker's *outer* rd hold prevents from ever being
   granted — a stable two-thread deadlock (walker ⇄ writer). This is the
   same bug class PR #332 fixed for `shard_count_worker` — but a
   different, still-live call site. Not fixed by #332.

2. **Hold amplification (stall, no cycle).** Even cross-shard, the inline
   flush blocks kf readers of arbitrary shards for a full multi-shard
   fetch while also holding bitmap(S) rd — stacking contention on any
   pending durability window's kf(S) wr → bitmap(S) wr step. No cycle
   (windows never touch a second shard's kf), but a pure avoidable stall.

Pre-existing on base: the 2026-08-27 bt↔kf chunked-fetch fix deliberately
routed bitmap primaries to this untouched path (its comments at
`chunked_stream_dispatch` and the `else` branch claim the inline flush is
"safe there" — that claim is what this plan corrects). Not exercisable by
sanitizer gates on base (needs a queued-writer arrival window; the
sanitizer suites run without a concurrent writer at that point, and the
failure mode is a hang, not a memory error).

### Failure modes (corrected from the pre-plan)

- **Pre-plan test sequencing was unrealizable.** "Arm a TEST_BUILD window
  pause holding S's kf wrlock, spawn the parked mutation, then run the
  find": with kf(S) wr held, the walker's *initial* `kfcache_acquire`
  (which precedes `bm_open`) blocks while the walker holds nothing — no
  handles are pinned, the writer's release unblocks everything, and base
  passes green. The deterministic red requires the writer to arrive while
  the walker is *inside* the walk holding kf(S)+bitmap(S) rd, i.e. the
  writer is **queued** (never holding) when the flush fires. The rewritten
  test (Task 2/3) parks the walker at the flush point with a TEST_BUILD
  gate, *then* spawns the mutation so it queues on kf(S) wr, *then*
  releases the gate — ordering proven, not timed, on the hard half.
- **Worker attribution** corrected (see Root cause).
- **`find_via_composite_key` is not a hazard site.** Its fallback
  collector `batch_buf_collect_cb` serves composite indexes, which are
  btree-only (`field_index_type` never returns IT_BITMAP for a composite
  directory), and its primary path uses the 2026-08-27 chunked executor.
  It stays byte-identical.

### Reachability

Any query with: bitmap-declared primary leaf (`field:bitmap`, or bare
bool/enum promotion), streaming eligibility (limit > 0, offset+limit ≤
1000, post-filter tree present, planner kind FP_INTERSECT /
FP_PRIMARY_LEAF / FP_BITMAP_SMALLER, trigram excluded), and an op the
bitmap answers — OP_EQUAL / OP_IN via the fast path, everything else
bitmap can answer via the generic dict-scan. `limit < candidates-on-one-
shard` makes the inline flush fire mid-walk. Concurrency with any indexed
writer on the walked shard supplies the queued writer. Full suite red
requires the Task 1 gate seam; production risk is a permanently wedged
worker thread holding bitmap(S) rd (the query deadline does not interrupt
a blocked `pthread_rwlock_rdlock`), which then stalls every subsequent
durability window's index apply on S.

## Fix shape (mirror the btree-side worker-owned pattern: collect-only under the walk, one blocking fetch after the walk's handles drop)

- `stream_find_cb`'s primary-leaf check is factored into
  `stream_primary_check()` (behavior-identical; the chunked executor's
  `stream_primary_filter` stays as-is — unifying it is out of scope).
- New `stream_find_defer_cb` — the **never-flush collector** the else
  branch passes instead of `stream_find_cb`: per-walk-thread
  (`__thread`) private hash batch, append-only, geometric growth, bounded
  by a per-query candidate budget (`g_query_buffer_max_bytes / 16`
  entries, QUERY_BUFFER_MB class). Budget exceeded → set `sc->stop`,
  LOG_WARN, return −1: collection stops walk-wide and what was collected
  still drains — the same partial-result semantics as the streaming
  deadline path. No silent drops.
- New `bm_defer_drain_if_armed(cb, sdb)`, called by **both** bitmap
  workers immediately after `bitmap_emit_*_for_shard` returns — at that
  point the worker's kf reader and bitmap handle are released, so the
  single blocking `slotcask_bulk_resolve_and_fetch` over the private
  batch nests nothing. Identity-gated on `cb == stream_find_defer_cb`:
  every pre-existing `btree_dispatch` caller is byte-identical (no-op).
- The batch has **no cap-induced mid-walk flush**: the bitmap walk has no
  resume primitive, so a single post-walk fetch per worker is the correct
  shape (the walk itself stays exactly as-is). Growth is bounded only by
  the per-query budget above.
- Emission still serializes through `stream_find_record_cb`'s
  `sc->lock`; `sc->stop` semantics unchanged; `sc.bfb` keeps its
  `record_cb`/`record_ctx` role (the drain reads them) and its
  destroy-time flush becomes a no-op in defer mode (pending stays empty).
- The `!driven` fallback inside the IT_BTREE branch keeps
  `stream_find_cb`: ops it serves route to btree_dispatch's O_DIRECT
  default leaf scan, which holds neither bt nor kf handles — genuinely
  safe for inline flush (verified: `chunked_stream_dispatch` drives
  EQUAL/GREATER\*/LESS\*/BETWEEN/IN/STARTS_WITH/prefix-LIKE/bool+range
  NOT_EQUAL; `default: return 0` sends the rest — substring LIKE,
  CONTAINS, ENDS, regex, len_\*, exists, i-variants, NOT_IN — to the
  O_DIRECT branch).
- Comments that claim bitmap-branch inline flushing is safe are rewritten
  (the `else`-branch comment is replaced wholesale; the
  `chunked_stream_dispatch` header comment is reworded to point at the
  deferred collector).

Guardrails: do NOT reorder kfcache-before-bitmap; do NOT touch
bitmap-engine or `bitmap_emit_*_for_shard` internals; do NOT add flushing
of any kind inside bitmap-walk callbacks; do NOT change
slotcask.c/btree.c/bitmap.c (the only slotcask-adjacent code is the
already-existing `slotcask_bulk_resolve_and_fetch` call); no lock-attribute
changes; `shard_db_internal.h`'s invariant comment gains this path's name
on its checked list (docs-adjacent hunk inside Task 5, not a code change).

## Call-site / consumer audit (verified 2026-09-03; executor re-runs the greps before editing)

**`stream_find_cb` users (complete):** exactly two — `idx_find_streaming`'s
`!driven` fallback (btree primary, O_DIRECT branches, keeps
`stream_find_cb`) and its `else` branch (bitmap primary, switches to
`stream_find_defer_cb`). Nothing else references it.

**`batch_buf_collect_cb` users (complete):** exactly one —
`find_via_composite_key`'s fallback. Composite = btree-only → never
reaches the IT_BITMAP section → byte-identical.

**All `btree_dispatch` callers (10) and their callbacks:**

| Site | Caller | Callback class | Blocking fetch in cb? | Change |
|---|---|---|---|---|
| query.c 1245 region | collect-then-emit KeySet path | keyset insert | no | none |
| query.c 1949 | `idx_find_streaming` !driven (btree) | `stream_find_cb` | yes — but O_DIRECT branches hold no handles | none |
| query.c 1955 | `idx_find_streaming` else (bitmap) | `stream_find_cb` | yes — **the hazard** | → `stream_find_defer_cb` |
| query.c 4325 / 4513 / 4603 | count two-pass + keyset builders | keyset insert / count | no (count path fixed by #332) | none |
| query.c 5487 / 5666 | aggregate / min-max walks | keyset / agg insert | no | none |
| query_aggregate.c 2774 / 4094 | aggregate leaf walks | agg insert | no | none |

Executor confirmation greps before editing (expected: no other
`bt_result_cb` implementation reachable from `btree_dispatch` calls
`kfcache_acquire` / `slotcask_bulk_*`):

```bash
grep -n "stream_find_cb\|batch_buf_collect_cb" src/db/*.c
grep -n "btree_dispatch(" src/db/*.c src/db/*.h
grep -rn "kfcache_acquire\|slotcask_bulk" src/db/query.c | grep -in "cb\|callback"   # manual review of hits
```

The serial `bitmap_emit_generic_for_shard` call inside
`build_keyset_from_bitmap`'s loop (keyset cb, no fetch) is also safe and
untouched: `bm_defer_drain_if_armed` no-ops on cb identity.

## Embedded execution rules

- Branch `fix/bitmap-stream-find-deferred-fetch` off `main`. Execute tasks
  in order.
- Build: `SKIP_TESTS=1 ./build.sh`. Tests: `./build/bin/shard-db-test
  run-all` (or `run <name>`).
- Gates (Definition of done, per AGENTS.md standing exceptions — locks
  and cached state are touched, so both sanitizers are mandatory):
  `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then three fresh
  `./build/bin/shard-db-test run-all`; then `BUILD_MODE=tsan
  SKIP_TESTS=1 ./build.sh` then three fresh
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
  ./build/bin/shard-db-test run-all`. No `halt_on_error=0`, no
  `--jobs`. Findings fail the run and get root-caused, not tolerated.
- If a quoted anchor isn't found exactly: write `PLAN_NOTES.md` describing
  the mismatch and halt the entire execution run immediately — do not
  guess, reinterpret, or continue to any further task. If you hit a
  decision this plan doesn't cover: stop and ask.
- Execution mode (repo standing exception): leave ALL work uncommitted for
  the reviewing agent + human raw-diff review.
- Paste real command output into the `## Evidence` sections as tasks
  complete. Never weaken a test to make a failure disappear.

## Task 1 — TEST_BUILD find-flush gate seam (inert until armed)

Mirrors the count-gap seam added by PR #332 (`TEST_HOOK_KIND_COUNT_GAP`):
an atomic arm flag + first-hit latch + a park function the control
channel releases. The gate marks "a worker is about to issue a blocking
batch fetch" — on the base build that point is `batch_buf_flush_copy`
(handles held); after Task 3 it is the deferred drain (handles released).
One hook kind serves both builds so the test's orchestration is identical
red and green.

### 1a. `src/db/shard_test_ctl.h` — new seam atomics + setter

The count-gap seam's declarations live here (PR #332), quoted — anchor
on the setter block:

```c
typedef void (*shard_db_test_count_gap_fn)(void *ctx);
void shard_db_test_set_count_gap_hook(shard_db_test_count_gap_fn fn, void *ctx);
void slotcask_test_count_gap_park(void);
```

(with the matching `extern _Atomic int g_shard_test_count_gap;` /
`extern _Atomic int g_shard_test_count_gap_hit;` block immediately
above). Add immediately after `slotcask_test_count_gap_park(void);`,
mirroring the file's `_Atomic int` extern style:

```c
/* Find-flush gate hook (docs/plans/2026-08-27-bitmap-inline-flush-
   hazard.md Task 1): parks a streaming-find worker immediately before
   its blocking batch fetch. On the legacy collector that point sits
   inside batch_buf_flush_copy with the walk's kf+bitmap handles still
   held; on the deferred collector it sits in bm_defer_drain_if_armed
   with no handles held. Armed by test_control.c on INSTALL kind 2; the
   park waits on the control-channel condvar (see test_control.c). */
extern _Atomic int g_shard_test_find_flush_gate;
extern _Atomic int g_shard_test_find_flush_gate_hit;
typedef void (*shard_db_test_gate_fn)(void *ctx);
void shard_db_test_set_find_flush_gate_hook(shard_db_test_gate_fn fn, void *ctx);
```

### 1b. `src/db/query.c` — definitions + base call site

Extend the existing `#ifdef TEST_BUILD` include block near the top of
query.c (added by PR #332 — `#include <stdatomic.h>`,
`#include "shard_test_ctl.h"` etc. should already be there) if anything
is missing. Then, immediately **before** the `batch_buf_flush_copy`
definition (so the static is declared before its first use), add the
definitions:

```c
#ifdef TEST_BUILD
/* Find-flush gate (test-control kind 2): parks the caller immediately
   before a blocking batch fetch. Inert until armed via the test-control
   channel; first caller parks, peers proceed. The atomics are defined
   here — the TU that polls them — mirroring how the count-gap atomics
   live beside slotcask_test_count_gap_park in slotcask.c. */
_Atomic int g_shard_test_find_flush_gate;
_Atomic int g_shard_test_find_flush_gate_hit;
static shard_db_test_gate_fn g_shard_test_find_flush_gate_fn;
static void *g_shard_test_find_flush_gate_ctx;

void shard_db_test_set_find_flush_gate_hook(shard_db_test_gate_fn fn,
                                            void *ctx) {
    g_shard_test_find_flush_gate_fn = fn;
    g_shard_test_find_flush_gate_ctx = ctx;
}

/* Called inside batch_buf_flush_copy on the legacy collector (kf+bitmap
   handles still held when the bitmap walk drove the flush) and, after
   the deferred collector lands, inside bm_defer_drain_if_armed (no
   handles held). */
static void bm_defer_gate_park(void) {
    if (!atomic_load(&g_shard_test_find_flush_gate)) return;
    int expected = 0;
    if (!atomic_compare_exchange_strong(&g_shard_test_find_flush_gate_hit,
                                        &expected, 1))
        return;                       /* another worker already parked */
    if (g_shard_test_find_flush_gate_fn)
        g_shard_test_find_flush_gate_fn(g_shard_test_find_flush_gate_ctx);
}
#endif
```

Base-side call site — anchor, the opening lines of
`batch_buf_flush_copy`:

```c
static void batch_buf_flush_copy(BatchFetchBuf *b) {
    size_t n;
    uint8_t (*copy)[16] = NULL;
```

Insert immediately after the opening brace (before the `b->lock` take):

```c
#ifdef TEST_BUILD
    bm_defer_gate_park();
#endif
```

(Note: the rare malloc-fail flush path at `batch_buf_collect_hash` holds
`b->lock` across `batch_buf_flush_copy`; a gate park there is bounded by
the control channel's release broadcast and is test-only — acceptable.)

### 1c. `src/db/test_control.c` — kind constant

The `TEST_HOOK_KIND_*` defines live in test_control.c (test_control.h
declares only `shard_db_test_control_start/stop`). Anchor (quoted,
test_control.c ~39-40):

```c
#define TEST_HOOK_KIND_AFTER_OLD 0
#define TEST_HOOK_KIND_COUNT_GAP 1
```

Append:

```c
#define TEST_HOOK_KIND_FIND_FLUSH_GATE 2
```

### 1d. `src/db/test_control.c` — dispatch + park function

Anchor (scoped by function: the close of `test_control_count_gap_block`;
note this tail text is NOT unique — `test_control_after_old` ends
identically — so locate the function by name, then take its closing
lines):

```c
    pthread_mutex_lock(&c->lock);
    while (c->running && !c->release)
        pthread_cond_wait(&c->cond, &c->lock);
    c->waiting_for_release = 0;
    pthread_mutex_unlock(&c->lock);
}
```

Add `test_control_find_flush_gate_block` with the same body but
`.phase = 3` and the same contract comment (caller parks mid-query;
releases only via control RELEASE/CLEAR or daemon stop). INSTALL
dispatch — anchor:

```c
            if (msg.phase == TEST_HOOK_KIND_COUNT_GAP) {
```

Extend the arm branch with:

```c
            } else if (msg.phase == TEST_HOOK_KIND_FIND_FLUSH_GATE) {
                atomic_store(&g_shard_test_find_flush_gate, 1);
                atomic_store(&g_shard_test_find_flush_gate_hit, 0);
                shard_db_test_set_find_flush_gate_hook(
                    test_control_find_flush_gate_block, c);
```

CLEAR — anchor `shard_db_test_set_count_gap_hook(NULL, NULL);` — add the
gate setter to NULL and reset both gate atomics.

### 1e. `src/test/fixtures.c` — runner-side phase validation

There are no `TEST_HOOK_KIND_*` defines on the runner side — the
existing convention is raw literals with a comment at the call site
(`test_shard_count_nested_kf_read.c` calls
`test_env_test_hook_install_kind(&env, 1)` with
`/* kind 1 = count-worker pass-1 gap (test_control.c) */`; the new test
follows the same pattern with `2`). The only runner-side change is the
REACHED phase validation in `test_env_test_hook_wait` — anchor (quoted,
fixtures.c):

```c
    /* phase: 0 = stale snapshot, 1 = under kf wrlock (after-old hook),
       2 = count worker parked in the pass-1 gap (probe reader held). */
    if (rep.phase < 0 || rep.phase > 2) return -1;
```

Replace with:

```c
    /* phase: 0 = stale snapshot, 1 = under kf wrlock (after-old hook),
       2 = count worker parked in the pass-1 gap (probe reader held),
       3 = find worker parked at the batch-fetch gate. */
    if (rep.phase < 0 || rep.phase > 3) return -1;
```

### 1f. Prove inertness

`SKIP_TESTS=1 ./build.sh` (no new warnings), then
`./build/bin/shard-db-test run-all` — full suite green with the seam
compiled in but unarmed. Paste into Evidence — Task 1.

## Task 2 — regression test `test-bitmap-stream-find-flush-gate` (red on base)

New file `src/test/cases/test_bitmap_stream_find_flush_gate.c`, harness
from `test_bt_kf_inversion_stream_find.c` (copy its `compute_hash_raw` /
`compute_record_shard` statics and its `parked_update_thread` shape
verbatim). TEST_REGISTER static init; name mirrors the file.

### 2a. Fixture

- `create-object`: splits 8, `fields":["flag:bool","title:varchar:64"]`,
  `"indexes":["flag:bitmap"]` — **explicit** declaration, deliberately
  immune to the auto-bitmap removal plan (bare-name promotion stays
  available either way).
- Choose shard S = first shard (lowest index) that accumulates 14 keys
  (`bait00..bait13`) via `compute_record_shard(compute_hash_raw(key))`;
  insert all 14 with `{"flag":true,"title":"wbaitNN"}`.
- Parked key: one more S-routed key, `{"flag":true,"title":"parked"}`.
  (15 S-routed rows total.)
- Filler rows routed to shards ≠ S, `flag:false`, distinct titles.
- Pre-assert: `count eq flag=true` == 15.

### 2b. The hazard find

Streaming-forcer, same trick as the inversion test (bitmap eq primary +
never-matching unindexed `contains` sibling + `limit`):

```c
snprintf(req, sizeof(req),
         "{\"timeout_ms\":8000,\"mode\":\"find\",\"dir\":\"default\","
         "\"object\":\"%s\",\"criteria\":{\"and\":["
         "{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},"
         "{\"field\":\"title\",\"op\":\"contains\","
         "\"value\":\"zzz-no-such-title\"}]},\"limit\":10}",
         g_obj);
```

`batch_buf_init` makes `pending_cap == 10`; 15 candidates all live on S
so only S's worker produces, and the 11th candidate forces the inline
flush **mid-walk while kf(S)+bitmap(S) are held** — every buffered hash
routes to S, so the resolver's nested kf(S) acquire is certain.

### 2c. Sequencing (the part the pre-plan got wrong)

1. Arm the gate with the runner-side raw-literal convention:
   `test_env_test_hook_install_kind(&env, 2)` with comment
   `/* kind 2 = find-flush gate (test_control.c) */`.
2. Spawn the find in a thread; `test_env_test_hook_wait` → REACHED
   phase 3 — the S worker is parked at the flush point holding
   kf(S) rd + bitmap(S) rd with a full, all-S batch. Assert REACHED
   fired (guards the red below against silent fixture rot).
3. Spawn the mutation (`update` parked key → `{"flag":false,"title":
   "parked"}`; touching the bitmap-indexed field is deliberate — the
   window's phase-I apply contends bitmap(S)). The mutation dispatches,
   persists its KFM2 marker, then blocks acquiring kf(S) **wr** —
   QUEUED behind the parked walker's rd. Settle: 3 s sleep (dispatch +
   marker fsync are ms-scale; margin ≈ 1000×; the only timed assumption
   in the test — see Deviations).
4. Release the gate. The walker enters
   `slotcask_bulk_resolve_and_fetch` → `slotcask_bulk_resolve_hashes`
   → nested `kfcache_acquire(kf(S), rd)` same-thread, writer queued →
   PREFER_WRITER_NONRECURSIVE **self-deadlock**. The walker wedges
   holding bitmap(S) rd; the writer can never get kf(S) wr; find and
   mutation are deadlocked. The query deadline cannot interrupt a
   blocked rdlock — client timeout is the only exit.
5. Assertions (red on base): `tc_request` on the find returns
   timeout/error at 8000 ms (assert that it did NOT return rows); 
   `tu_timed_join(mutation, 4000)` does NOT join (assert). Then
   `test_env_stop` teardown kills the daemon and reaps both — watchdog-
   safe, cannot wedge run-all (client-side timeouts + kill teardown only;
   no server-side unbounded waits).
6. Post-release convergence check (green-only; on base the assertions in
   step 5 have already failed the case): a fresh find `eq flag=true`
   limit 100 returns 12 rows (parked key flipped) — only reachable after
   Task 3.

### 2d. Red-on-base proof

`./build/bin/shard-db-test run test-bitmap-stream-find-flush-gate` on the
unmodified base (+Task 1): required failures — find completed-without-
timeout assertion and mutation-joined assertion. Paste the failing output
into Evidence — Task 2. Green rerun lands in Evidence — Task 3.

Run the case 3× consecutively; all three must be red (guards the 3 s
settle against runner-load flakes — it must never pass on base).

## Task 3 — the fix + revert-verify proof

### 3a. `StreamFindCtx` — deferred-budget fields

Anchor (struct tail):

```c
    /* Mutable shared state. */
    pthread_mutex_t   lock;
    int               passed;     /* records that passed both filters */
    int               printed;    /* records actually emitted */
    int               stop;       /* atomic — set when printed >= limit */
```

Append after `stop`:

```c
    /* Deferred-collector budget (bitmap-routed fallback): per-query cap
       on candidate hashes held across worker walks, QUERY_BUFFER_MB
       class. defer_total is manipulated via __atomic builtins. */
    size_t            defer_total;
    size_t            defer_budget;
```

In `idx_find_streaming`, after the `StreamFindCtx sc = {0};` block's
field assignments (anchor: `pthread_mutex_init(&sc.lock, NULL);`), add:

```c
    sc.defer_budget = g_query_buffer_max_bytes / 16;
```

### 3b. Factor the primary check out of `stream_find_cb`

Anchor: `stream_find_cb`'s body from the comment
`/* Primary check (LEN_*, like patterns where check_primary == 1). */`
through the closing brace of the `else if (sc->check_primary ...)`
chain (quoted in full in Root cause planning above — the exact current
lines are at `stream_find_cb`, navigation ~1701-1718). Move that logic
verbatim into:

```c
/* Primary-leaf match shared by stream_find_cb and stream_find_defer_cb
   so both collectors agree on which entries become fetch candidates. */
static int stream_primary_check(StreamFindCtx *sc,
                                const char *val, size_t vlen) {
    if (sc->primary_crit && op_is_length(sc->primary_crit->op)) {
        if (!match_length_vlen(vlen, sc->primary_crit)) return 0;
    } else if (sc->check_primary && sc->primary_crit) {
        char tmp[1028];
        int matched;
        if (sc->tf) {
            int dlen = decode_idx_to_buf(sc->tf, (const uint8_t*)val, vlen,
                                          tmp, sizeof(tmp), 0);
            if (dlen <= 0) return 0;
            matched = match_criterion(tmp, sc->primary_crit);
        } else {
            size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
            memcpy(tmp, val, cl); tmp[cl] = '\0';
            matched = match_criterion_vlen(tmp, cl, sc->primary_crit);
        }
        if (!matched) return 0;
    }
    return 1;
}
```

and reduce `stream_find_cb` to:

```c
static int stream_find_cb(const char *val, size_t vlen, const uint8_t *hash16, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE)) return -1;

    BatchFetchBuf *bfb = &sc->bfb;
    g_out = sc->parent_out;

    if (!stream_primary_check(sc, val, vlen)) return 0;

    return batch_buf_collect_hash(bfb, hash16);
}
```

### 3c. The deferred collector + drain (new, placed after `stream_find_cb`)

```c
/* Deferred collector for the bitmap-routed fallback. The bitmap
   emitters hold the shard's kf reader + bitmap handle across the walk
   (kfcache-before-bitmap order), so an inline flush here would nest a
   second kf read acquire under the held one — a
   PREFER_WRITER_NONRECURSIVE self-deadlock behind any queued window
   writer. Instead each walk thread appends to its OWN thread-local
   batch and drains it with ONE blocking bulk fetch after
   bitmap_emit_*_for_shard returned and every handle is released. The
   bitmap walk has no resume primitive, so a single post-walk fetch per
   worker (not mid-walk flushing) is the correct shape; growth is
   bounded by the per-query budget in sc->defer_budget. */
typedef struct {
    uint8_t        (*hashes)[16];
    size_t         n;
    size_t         cap;
    StreamFindCtx *sc;
} BmDeferBatch;

static __thread BmDeferBatch tl_bm_defer;

static int stream_find_defer_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *raw_ctx) {
    StreamFindCtx *sc = (StreamFindCtx *)raw_ctx;
    if (__atomic_load_n(&sc->stop, __ATOMIC_ACQUIRE)) return -1;
    g_out = sc->parent_out;
    if (!stream_primary_check(sc, val, vlen)) return 0;

    BmDeferBatch *t = &tl_bm_defer;
    if (t->cap == 0) {
        t->cap = 64;
        t->hashes = malloc(t->cap * 16);
        if (!t->hashes) {
            __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
            return -1;
        }
        t->sc = sc;
    }
    if ((size_t)__atomic_add_fetch(&sc->defer_total, 1,
                                   __ATOMIC_RELAXED) > sc->defer_budget) {
        /* Per-query candidate budget exhausted (QUERY_BUFFER_MB class).
           Stop collecting walk-wide; what was collected still drains —
           the streaming deadline's partial-result semantics, never a
           silent drop. */
        LOG_WARN(LOG_SUB_QUERY,
                 "bitmap deferred batch budget exceeded; find returns "
                 "partial results");
        __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
        return -1;
    }
    if (t->n == t->cap) {
        size_t ncap = t->cap * 2;
        uint8_t (*nh)[16] = realloc(t->hashes, ncap * 16);
        if (!nh) {
            __atomic_store_n(&sc->stop, 1, __ATOMIC_RELEASE);
            return -1;
        }
        t->hashes = nh;
        t->cap = ncap;
    }
    memcpy(t->hashes[t->n], hash16, 16);
    t->n++;
    return 0;
}

/* Worker-side drain. No-op for every cb other than the deferred
   collector — all pre-existing btree_dispatch callers are byte-
   identical. Runs after bitmap_emit_*_for_shard returned: this shard's
   kf reader and bitmap handle are released, so the blocking fetch
   nests nothing (the 2026-08-27 chunked-executor invariant, adapted to
   the no-resume bitmap walk). */
static void bm_defer_drain_if_armed(bt_result_cb cb, SlotcaskDb *sdb) {
    BmDeferBatch *t = &tl_bm_defer;
    if (cb != stream_find_defer_cb || t->n == 0 || !t->sc) return;
    StreamFindCtx *sc = t->sc;
#ifdef TEST_BUILD
    bm_defer_gate_park();   /* parks with NO handles held on this build */
#endif
    slotcask_bulk_resolve_and_fetch(sdb, t->hashes, t->n,
                                    sc->bfb.record_ctx, sc->bfb.record_cb);
    free(t->hashes);
    t->hashes = NULL; t->n = 0; t->cap = 0; t->sc = NULL;
}
```

Declaration note: `bm_shard_walk_worker` and `bm_generic_shard_worker`
are defined later in the file than this block; the workers reference
`bm_defer_drain_if_armed`, so place the block before the workers or add
a forward declaration next to the existing worker forward-decls near the
arg-struct definitions (`static void bm_defer_drain_if_armed(bt_result_cb cb, SlotcaskDb *sdb);`).

### 3d. Worker drain lines

`bm_shard_walk_worker` — anchor:

```c
    int stopped = bitmap_emit_for_shard(a->db_root, a->object, a->field,
                                        a->shard_idx, a->value, a->vlen,
                                        a->cb, a->ctx, a->sdb);
```

Insert immediately after:

```c
    bm_defer_drain_if_armed(a->cb, a->sdb);
```

`bm_generic_shard_worker` — anchor:

```c
    int stopped = bitmap_emit_generic_for_shard(a->db_root, a->object,
                                                 a->field, a->shard_idx,
                                                 a->crit, a->tf,
                                                 a->cb, a->ctx, a->sdb);
```

Insert immediately after the same drain line. (Early-return-on-stop
workers never armed a batch on that thread — `t->n == 0` makes the drain
a no-op.)

### 3e. Routing: the else branch defers

Anchor (quoted in Root cause) — replace the whole `else` block with:

```c
    } else {
        /* IT_BITMAP branches hold kf(S)+bitmap(S) across the emit, so the
         * collector must never flush inline: the resolver's per-shard
         * kfcache acquires would nest a second kf(S) read acquire under
         * the held one — PREFER_WRITER_NONRECURSIVE self-deadlock behind
         * any queued window writer. The deferred collector appends to a
         * per-thread batch only; each worker fetches after its walk
         * releases the handles. O_DIRECT leaf-scan branches (btree
         * primaries the chunked dispatcher didn't drive) hold no handles
         * and keep the inline-flush collector on the !driven path. */
        btree_dispatch(db_root, object, primary_crit->field, sch->splits,
                        primary_crit,
                        resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                        stream_find_defer_cb, &sc);
    }
```

### 3f. Rewrite the two now-false "inline flush is safe" comments

`chunked_stream_dispatch` header — anchor:

```c
 * to btree_dispatch + stream_find_cb, whose inline batch flush is safe
 * there. Returns 1 when the op was driven here, 0 when the caller must use
 * the legacy path. */
```

Replace with:

```c
 * to btree_dispatch + stream_find_cb, whose inline batch flush is safe on
 * those no-handle branches. Bitmap branches are NOT safe for inline flush
 * (they hold kf+bitmap handles across the emit) and get the deferred
 * collector instead — see idx_find_streaming. Returns 1 when the op was
 * driven here, 0 when the caller must use the legacy path. */
```

(The `else`-branch comment is already replaced by 3e.)

### 3g. `shard_db_internal.h` invariant note

Anchor: the sentence `This is a live invariant, not a one-time fact` in
the `rwlock_init_writer_preferring` comment block. Extend the surrounding
list so the checked-set statement reads as re-verified with the deferred
collector in place (exact wording at execution time; substance: the
2026-08-27 bitmap streaming flush was a missed recursive-acquisition
site, now removed — the remaining call sites were re-audited under this
plan's audit task).

### Revert-verify proof (both outputs get pasted)

1. Green: with Tasks 1+2+3 applied, `./build/bin/shard-db-test run
   test-bitmap-stream-find-flush-gate` — passes (gate parks the drain
   with no handles; mutation joins while the drainer is parked; find
   completes; convergence find returns 12). Also run it 3× consecutively.
2. Red: `git stash push -- src/db/query.c` limited to the Task 3 hunks
   is NOT required to be surgical — instead: `git diff > /tmp/full.diff`,
   `git checkout -- src/db/query.c`, re-apply ONLY Task 1's query.c seam
   hunks + test, rebuild, rerun — must fail exactly as Evidence — Task 2
   did. Then restore via `git apply /tmp/full.diff` (plus the Task 1
   query.c hunks if clobbered — keep one saved diff of the final tree and
   verify `git diff` is byte-identical before/after the experiment) and
   rerun green. Paste all three outputs.
3. Sanity: `run test-bitmap-index`, `run test-bt-kf-inversion-stream-find`,
   `run test-shard-count-nested-kf-read`, `run test-stream-find-chunk-resume`
   individually — all green (the collectors' other users are untouched,
   and this test doubles as the bitmap-adjacent regression net).

## Task 4 — full suite, audit re-check, sanitizer gate

1. `SKIP_TESTS=1 ./build.sh` — no new compiler warnings.
2. `./build/bin/shard-db-test run-all` — full suite green.
3. Re-run the Task 3 audit greps; paste outputs; confirm the 12-site
   table still holds and no new blocking-fetch callback appeared.
4. `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`; three fresh
   `./build/bin/shard-db-test run-all`.
5. `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`; three fresh
   `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
   ./build/bin/shard-db-test run-all` — the new gate test runs inside
   run-all, so the parked-window shape is exercised under TSan options
   as required.
6. `git status` + `git diff` packet for review: src/db/query.c,
   src/db/shard_test_ctl.h, src/db/test_control.c,
   src/test/fixtures.c, src/test/cases/test_bitmap_stream_find_flush_gate.c,
   docs (Task 5). NOTHING committed (repo standing exception).

## Task 5 — docs sync (same diff, not deferred)

- AGENTS.md — per-query memory cap section: "8 collection sites" → 9,
  adding "bitmap deferred candidate batches (per-walk-thread, bounded by
  QUERY_BUFFER_MB / 16 B per hash)".
- docs/reference/limits.md — same list, plus a line on the deferred
  collector's partial-result budget behavior.
- docs/concepts/concurrency.md — extend the gate invariant (the
  bt↔kf section from the 2026-08-27 fix): bitmap-walk callbacks must
  never flush or fetch; fetches happen in `bm_defer_drain_if_armed`
  after the walk's handles drop.
- docs/reference/changelog.md `## Unreleased` — fix entry: bitmap-primary
  streaming finds no longer flush the batch fetch inline under held
  kf+bitmap handles (recursive kf-reader self-deadlock against queued
  durability windows); per-query candidate budget note for the deferred
  collector.

## Acceptance criteria

- Task 2's test red on base (3 consecutive reds), green after Task 3
  (3 consecutive greens) — all pasted.
- `stream_find_cb` / `batch_buf_collect_cb` non-bitmap callers
  byte-identical (audit table + diff review).
- No changes outside: query.c, shard_test_ctl.h, test_control.c,
  fixtures.c, the new test, docs. No slotcask/btree/bitmap-engine or
  lock-attribute changes.
- Full suite + asan ×3 + tsan ×3 green; diff left uncommitted.
- The `shard_db_internal.h` recursive-reader invariant statement is true
  again for every kfcache acquire site.

## Edge cases & invariants (summary)

- OP_IN per-value fan-outs: drain runs after each value's fan-out
  (worker fn granularity) — correct and bounded.
- Limit hit mid-walk: `sc->stop` set by `stream_find_record_cb`; deferred
  cb returns −1, walks unwind via the existing stop_flag; partial batches
  still drain (mirrors base's destroy-time flush); over-limit rows are
  dropped by record_cb exactly as today.
- Budget/malloc failure: stop + partial + LOG_WARN (deadline-equivalent),
  never a silent skip.
- Thread-local lifecycle: drained batches are freed immediately (no
  cross-query retention on pooled `parallel_for_io` threads); `t->sc` is
  reset at drain, so a stale pointer can never outlive its query.
- Empty-shard workers, early-stop workers: drain no-ops on `n == 0`.
- Base-build deadlock leakage into the daemon is bounded: the gate test
  is the only producer of the wedge, its daemon is process-private, and
  teardown kills it (client timeouts; no harness-side unbounded waits).

## Evidence — Task 1

Build with the seam compiled in but unarmed: no new warnings
(`SKIP_TESTS=1 ./build.sh` clean). Full suite inert:

```
1..438
# total: 12831 passed, 0 failed across 438 cases
```

Execution deviations from the plan text (all additive, none alter the
planned hunks):

- `build.sh`: the new test file had to be appended to the explicit
  `src/test/cases/` list (cases are enumerated, not globbed).
- `test_control.c` `shard_db_test_control_stop()`: the gate hook is also
  reset to NULL next to the count-gap reset — same
  "prevent any future fire" pattern; the plan's CLEAR hunk already
  implied it.
- `shard_db_internal.h` is touched by Task 3g (comment-only), so it is
  part of the review packet.

## Evidence — Task 2

Three consecutive runs on base (Task 1 seam + test only, fix reverted —
see Task 3 revert-verify for the mechanics). Identical failure
signature every time; the gate park (ok 7/ok 8) proves the flush fires
mid-walk before the red assertions:

```
ok  7 - worker parked at the batch-fetch gate
ok  8 - gate phase = find batch-fetch park
ok  9 - spawn mutation
not ok 10 - streaming find completes within timeout (base: wedged behind the recursive reader)
#   expected 0 got -1
not ok 11 - mutation completes after gate release (base: writer queued behind the wedged walker)
#   expected 0 got 110
ok 12 - remove test fixture tree
# test-bitmap-stream-find-flush-gate: 10 passed, 2 failed
```

(×3 consecutive, same output.) The find wedged past its 8 s client
timeout; the mutation never joined (queued writer never granted) — the
stable walker⇄writer deadlock.

## Evidence — Task 3

Green with the fix (×3 consecutive):

```
ok 10 - streaming find completes within timeout (base: wedged behind the recursive reader)
ok 11 - mutation completes after gate release (base: writer queued behind the wedged walker)
ok 12 - mutation committed
ok 13 - post-count round-trips
ok 14 - parked-key flip converged (14 true rows)
ok 15 - remove test fixture tree
# test-bitmap-stream-find-flush-gate: 15 passed, 0 failed
```

Revert-verify: `git checkout -- src/db/query.c src/db/shard_db_internal.h`,
re-applied ONLY the Task 1 query.c seam hunks, rebuild → red (exact
Task 2 output above). Restored the fixed files from snapshot copies —
`sha256sum -c /tmp/postfix.sums`: both files `OK` (byte-identical) —
rebuild → green again.

Sanity net (fixed build):

```
# test-bitmap-index: 271 passed, 0 failed
# test-bt-kf-inversion-stream-find: 20 passed, 0 failed
# test-shard-count-nested-kf-read: 15 passed, 0 failed
# test-stream-find-chunk-resume: 421 passed, 0 failed
```

## Evidence — Task 4

Audit greps post-fix: `stream_find_cb` users = the !driven btree
fallback (query.c:2084) only; `batch_buf_collect_cb` = composite
fallback (query.c:2750) only; `btree_dispatch(` = 10 call sites
unchanged (8 query.c + 2 query_aggregate.c), none new. Full suite on
the normal build with the new case registered:

```
1..439
# total: 12846 passed, 0 failed across 439 cases
```

ASan gate: `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` clean, then three
fresh `./build/bin/shard-db-test run-all`:

```
# total: 12846 passed, 0 failed across 439 cases
# total: 12846 passed, 0 failed across 439 cases
# total: 12846 passed, 0 failed across 439 cases
```

TSan gate — invocation deviation, human-directed + CI-precedented: the
default-parallelism run aborted 3× at the 180 s per-case watchdog on
`test-auto-reshard-throttle` (pre-existing wall-clock false fire; the
human confirmed it times out on main too). Re-ran with CI's documented
TSan invocation (`.github/workflows/tsan.yml`: `--jobs 2`,
`SHARD_TEST_WATCHDOG_SEC=1200`, and CI's `SHARD_TEST_EXCLUDE` list of
fsync/timing-heavy cases — `test-auto-reshard-throttle` is its first
entry). Three fresh runs:

```
# total: 12709 passed, 0 failed across 428 cases
# total: 12709 passed, 0 failed across 428 cases
# total: 12709 passed, 0 failed across 428 cases
```

(428 = 439 − the 11 excluded cases; the new gate test is NOT excluded
and ran inside every TSan run-all.) Targeted confirmation of the
parked-window shape under TSan options:
`run test-bitmap-stream-find-flush-gate` → `15 passed, 0 failed`.
Normal (non-sanitizer) build restored afterwards; `run-all` green again.

## Evidence — Task 5

Docs changed: AGENTS.md (per-query memory cap: 9 collection sites +
deferred-collector semantics), docs/reference/limits.md (site list +
partial-result budget line), docs/concepts/concurrency.md (pattern 3:
deferred drain rule for bitmap walks), docs/reference/changelog.md
(Unreleased fix entry).
