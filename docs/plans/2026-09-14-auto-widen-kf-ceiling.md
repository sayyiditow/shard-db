# W1 — unbounded per-shard kf growth + nightly reshard default-on

Date: 2026-09-14  
Reissued: 2026-10-07 under the human's growth-model decision (see
Revision history) — this file now covers the **uncapped growth** design
and supersedes the auto-widen design that previously lived here.  
Status: draft for re-review — not approved or executable until the
human approves.  
All anchors verified against `main`@`1dac05a` on 2026-10-07.

## Revision history

- **2026-09-14 (committed at HEAD):** original auto-widen design —
  16M slots/shard ceiling, ceiling-ENOSPC triggers inline widen+retry.
- **2026-10-07, six blind-review rounds (uncommitted revisions, now
  discarded):** the reviews established that the 16M "ceiling" was
  incoherent — enforced only in the bulk pre-grow/stage loops, while
  the insert paths' inline resplits (`kf_plan_insert_slot`,
  `kf_plan_window_insert_slot`) were already uncapped and tunneled
  through it. Successive findings (objlock/registry-lifetime defects
  in widening from a locked request, a drop+recreate identity race, a
  data race on test controls, a 24-bit radix-sort assumption) patched
  that design until it reached ~2,200 lines.
- **2026-10-07, growth-model decision (human):** per-shard growth is
  **uncapped** — "unlimited records" is the product stance; the
  nightly auto-reshard sweep (default-on) is the rebalancer; the
  inline-resplit stall at scale is the accepted trade-off. This
  deletes the entire auto-widen machinery (no capacity-ENOSPC exists
  on the write path to trigger it) and reduces the plan to: consistent
  uncapped growth, the radix-sort correctness fix the uncapping
  requires, test seams, and the sweep default flip.
- **2026-10-07, approval-review direction (human):** the guarantee
  being purchased is that an 8-splits object grows **boundlessly with
  no opt-in** — inserts never error for capacity, whether or not the
  user ever touched a knob. Two additions: (1) the nightly sweep is
  opt-**out** — already the plan's D3 (default on, explicit
  `AUTO_RESHARD_ENABLE=0` disables); (2) a proactive **RESHARD-HINT**
  log: when a growth event reveals that an object has outgrown its
  splits per the sizing table, the daemon logs an actionable hint
  (run `vacuum --splits=N` now, or wait for the nightly job) — added
  as D2b.
- **2026-10-07, blind review of the uncapped design:** **High** —
  `reshard_hint_last_target` was a plain `int` written and read by
  concurrent shard workers (parallel pre-grow tasks, per-shard
  insert paths) with no common lock — undefined behavior TSan must
  flag. The field is now atomic and the once-per-level suppression is
  an atomic RMW. **Medium** — the unit test built an `rm -rf` shell
  command from `SHARD_TEST_TMPDIR` (injection/metacharacters);
  the test now creates its directory with `mkdtemp` and removes it
  with an `nftw` walker. **Medium** — the radix-sort fix had no
  regression test at its failure boundary (5,000 records never reach
  2^24); a TEST_BUILD wrapper exposes the sort and the unit test now
  feeds values straddling the old bound, with a revert-check-reapply
  proof. Also corrected en passant: `slotcask_sum_kf_totals` returns
  (total, deleted) — the hint computes `live = total − deleted`.
- **2026-10-07, blind review round two:** **Medium** — the
  `atomic_exchange` suppression did not actually deliver at-most-once
  per level: reordered workers could re-log a level after a stale
  recommendation overwrote the field (16→32→16 re-logs 32 later).
  The suppression is now an atomic **bitmask** (`reshard_hint_logged`;
  levels are powers of two, so bit = log2(level)) — exactly one
  worker can claim a level, and nothing un-claims it within an open.
  **Medium** — Task 4 missed the core references still describing the
  old ceiling: `docs/concepts/storage-model.md` (:137, "no per-shard
  slot cap ... until the global 16M ceiling, at which point
  kf_put_new refuses"), `docs/reference/limits.md` (:19, the
  `SLOTCASK_MAX_SLOTS_PER_SHARD` constants row), and the AGENTS.md
  storage-model bullet — all three now in Task 4.
- **2026-10-07, blind review round three:** **High** — Task 1's test
  referenced `reshard_hint_logged` and
  `slotcask_test_radix_sort_sizes`, which land in Task 2, so the
  claimed Task-1 red stage could not compile. The case is split by
  task: Task 1 ships Parts 1–3; Task 2 appends Part 4 (reshard hint)
  and Part 5 (radix across 2^24) with insertion anchors.
  **Medium** — Task 3 implemented the default flip before the test;
  reordered test-first (export infra → red on the old default →
  flip → green). **Medium** — Task 4's documentation edits now carry
  complete replacement Markdown for storage-model.md, limits.md, and
  AGENTS.md. **Accuracy** — the plan claimed slotcask_open leaves the
  hint field untouched; it actually `memset`s the whole struct at
  entry (slotcask.c), so the field is zeroed by open and the test's
  own memset was removed.

## Goal and root cause

`SLOTCASK_MAX_SLOTS_PER_SHARD` (16M, slotcask.h:87) caps per-shard kf
capacity growth — but only in two of the four growth sites. The bulk
pre-grow loop (slotcask.c:2311) and the bulk stage path (:7081) stop
doubling at 16M; the inline resplits at the top of
`kf_plan_insert_slot` (:2493) and `kf_plan_window_insert_slot`
(:2562) never had a cap and double unboundedly. The result is
incoherent: the documented "hard ceiling" (AGENTS.md, tuning.md) does
not actually hold on the insert paths, while pre-grow/stage refuse to
grow past it and leave the inline resplit to pay the cost mid-write.

The human's decision: **there is no per-shard ceiling.** Per-shard
doubling continues for the life of the object; the nightly auto-reshard
sweep is how big objects get rebalanced into more splits. This plan:

1. makes growth uniformly uncapped (the two capped loops join the two
   uncapped inline resplits),
2. fixes the one real dependency on the old constant —
   `radix_sort_sizes` assumes slot indices fit 24 bits, which silently
   mis-sorts once a single shard grows past 16,777,216 slots,
3. flips the nightly sweep's default on, so rebalancing happens
   without operator action,
4. documents the growth model and its trade-off honestly.

## Known trade-offs (documented, accepted by the human)

- **Inline resplit cost scales with entries, not capacity.** The
  pre-grow docstring records ~4–5 s per resplit at ~25M-entry scale
  when it collides with concurrent segment writes (vs ~50 ms
  pre-grown). Uncapped, that doubles each tier: a hot shard at 100M+
  records takes tens of seconds of write stall per doubling, on the
  write path, under the kf wrlock. The nightly sweep bounds how many
  records share a shard, which bounds the stall; between sweeps, big
  shards stall proportionally.
- **Practical per-shard limit ≈ 4G slots** (the probe start
  `hash[2..5] % cap` is 32-bit; a 4G-slot kf table is ~96 GB) —
  unreachable in practice before disk.
- The object remains bounded by disk, and by `MAX_SPLITS = 4096` for
  the *splits* axis (the sweep's tool) — the per-shard axis no longer
  contributes a ceiling.

## Design

### D1 — test seams (initial slots, test-only max) + control channel

Tests need to start shards tiny (64 slots) so doublings are cheap, and
to impose a test-only cap on the growth loops so the loop wiring is
observably testable (production is uncapped: no seam set → `SIZE_MAX`).

Definitions — quoted anchor (slotcask.c:49–53):

```c
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
_Atomic long g_shard_test_gate_held_max;
_Atomic long g_shard_test_bulk_chains;
```

becomes:

```c
#ifdef TEST_BUILD
#include "shard_test_ctl.h"
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
_Atomic long g_shard_test_gate_held_max;
_Atomic long g_shard_test_bulk_chains;
/* W1: TEST_BUILD seams for the per-shard kf slot tier. initial
 * replaces slotcask_default_slots_for_splits() at slotcask_open; max
 * imposes a test-only growth cap on the pre-grow/stage loops (0 =
 * uncapped, the production behavior — there is no production cap).
 * _Atomic: written on the control thread, read on request threads;
 * the channel ACK is not C synchronization. */
_Atomic size_t g_shard_test_kf_initial_slots;
_Atomic size_t g_shard_test_kf_max_slots;
```

Declarations — quoted anchor (shard_test_ctl.h:105–108):

```c
extern _Atomic int g_shard_test_find_flush_gate;
extern _Atomic int g_shard_test_find_flush_gate_hit;
```

becomes:

```c
extern _Atomic int g_shard_test_find_flush_gate;
extern _Atomic int g_shard_test_find_flush_gate_hit;
/* W1: per-shard kf slot tier seams (defined in slotcask.c under
 * TEST_BUILD). _Atomic: control-thread writes, request-thread reads. */
extern _Atomic size_t g_shard_test_kf_initial_slots;
extern _Atomic size_t g_shard_test_kf_max_slots;
```

`shard_test_ctl_reset()` appends two lines. Quoted anchor
(shard_test_ctl.h, inside `shard_test_ctl_reset`):

```c
    atomic_store(&g_shard_test_count_gap, 0);
    atomic_store(&g_shard_test_count_gap_hit, 0);
```

becomes:

```c
    atomic_store(&g_shard_test_count_gap, 0);
    atomic_store(&g_shard_test_count_gap_hit, 0);
    atomic_store(&g_shard_test_kf_initial_slots, 0);
    atomic_store(&g_shard_test_kf_max_slots, 0);
```

The open-site seam (the unit test starts shards tiny) — quoted anchor
(slotcask.c:4303, verified unique):

```c
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);
```

becomes:

```c
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);
#ifdef TEST_BUILD
    size_t test_initial = atomic_load(&g_shard_test_kf_initial_slots);
    if (test_initial)
        db->slots_per_shard = test_initial;
#endif
```

Daemon arming goes through the existing test-control channel
(`shard-db-test-server --test-control-fd`, src/db/test_control.c; the
message layout is deliberately duplicated in src/test/fixtures.c —
both copies change identically; the socketpair is private TEST_BUILD
plumbing). Quoted anchor (test_control.c:14–21, the kinds enum):

```c
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
};
```

becomes:

```c
enum {
    TEST_HOOK_INSTALL = 1,
    TEST_HOOK_RELEASE = 2,
    TEST_HOOK_CLEAR   = 3,
    TEST_HOOK_ACK     = 4,
    TEST_HOOK_REACHED = 5,
    TEST_HOOK_SET_GLOBALS = 6,
};
```

Quoted anchor (test_control.c struct):

```c
typedef struct {
    uint32_t kind;   /* INSTALL=1, RELEASE=2, CLEAR=3, ACK=4, REACHED=5 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
} TestHookMessage;
```

becomes:

```c
typedef struct {
    uint32_t kind;   /* INSTALL..REACHED; SET_GLOBALS=6 */
    int32_t  phase;  /* REACHED: 0=stale snapshot, 1=under kf wrlock; else 0 */
    uint64_t a;      /* SET_GLOBALS: initial slots (0 = default) */
    uint64_t b;      /* SET_GLOBALS: test-only max slots (0 = uncapped) */
} TestHookMessage;
```

Existing senders use designated initializers (`{ .kind = …, .phase =
… }`), so `a`/`b` zero-fill wherever unused.

Quoted anchor (test_control.c includes, :23–25):

```c
#include "slotcask.h"
#include "shard_test_ctl.h"
#include "test_control.h"
```

becomes:

```c
#include "slotcask.h"
#include "shard_test_ctl.h"
#include "test_control.h"
```

(no new includes needed — the handler only writes the two seams.)

Daemon-side handler — in `test_control_thread_main`'s switch, quoted
anchor:

```c
        case TEST_HOOK_RELEASE:
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
```

becomes:

```c
        case TEST_HOOK_RELEASE:
            pthread_mutex_lock(&c->lock);
            c->release = 1;
            pthread_cond_broadcast(&c->cond);
            pthread_mutex_unlock(&c->lock);
            break;
        case TEST_HOOK_SET_GLOBALS:
            atomic_store(&g_shard_test_kf_initial_slots, (size_t)msg.a);
            atomic_store(&g_shard_test_kf_max_slots, (size_t)msg.b);
            break;
```

The ACK gate below the switch — quoted anchor:

```c
        if (msg.kind == TEST_HOOK_INSTALL || msg.kind == TEST_HOOK_RELEASE ||
            msg.kind == TEST_HOOK_CLEAR) {
```

becomes:

```c
        if (msg.kind == TEST_HOOK_INSTALL || msg.kind == TEST_HOOK_RELEASE ||
            msg.kind == TEST_HOOK_CLEAR || msg.kind == TEST_HOOK_SET_GLOBALS) {
```

Runner-side mirror (src/test/fixtures.c): apply the same enum and
struct changes to the duplicated copy, and add one sender mirroring
`test_env_test_hook_install_kind`'s write-and-wait-ACK pattern
(fixtures.c:807):

```c
int test_env_ctl_set_kf_seams(TestEnv *env, size_t initial, size_t max) {
    if (!env || env->test_control_fd < 0) return -1;
    TestHookMessage msg = { .kind = TEST_HOOK_SET_GLOBALS,
                            .phase = 0, .a = (uint64_t)initial,
                            .b = (uint64_t)max };
    if (test_hook_write_full(env->test_control_fd, &msg, sizeof(msg)) != 0)
        return -1;
    TestHookMessage rep = {0};
    if (test_hook_read_full(env->test_control_fd, &rep, sizeof(rep)) != 0)
        return -1;
    if (rep.kind != TEST_HOOK_ACK) return -1;
    return 0;
}
```

Declaration — quoted anchor (fixtures.h:57):

```c
int test_env_test_hook_release(TestEnv *env);
```

becomes:

```c
int test_env_test_hook_release(TestEnv *env);
int test_env_ctl_set_kf_seams(TestEnv *env, size_t initial, size_t max);
```

### D2 — uncapped growth, consistently

The effective-max helper — placed immediately before the pre-grow
worker. Quoted anchor (slotcask.c:2293–2294):

```c
} SlotcaskPregrowArg;

static void *slotcask_pregrow_worker(void *raw) {
```

becomes:

```c
} SlotcaskPregrowArg;

/* W1: effective per-shard growth cap. Production is UNCAPPED (the
 * human's 2026-10-07 growth-model decision): SIZE_MAX lets the 75%
 * projected-load condition be the only terminator. TEST_BUILD tests
 * lower it via the seam to make loop wiring observable. */
static size_t kf_max_slots(void) {
#ifdef TEST_BUILD
    size_t test_max = atomic_load(&g_shard_test_kf_max_slots);
    if (test_max) return test_max;
#endif
    return SIZE_MAX;
}

static void *slotcask_pregrow_worker(void *raw) {
```

The two capped loops join the inline resplits. Quoted anchor
(identical text at slotcask.c:2311 in `slotcask_pregrow_worker` and
:7081 in the bulk stage path — exactly two occurrences):

```c
        while (kh.capacity < SLOTCASK_MAX_SLOTS_PER_SHARD &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
```

becomes (both sites):

```c
        while (kh.capacity < kf_max_slots() &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
```

The constant and its comment go away — quoted anchor (slotcask.h
tier-sizing comment tail + define, :82–88):

```c
   Each tier targets ~50 % load at the documented 78K-200K rec/shard
   sweet spot. Per-shard auto-resplit at 80 % load doubles slots in place
   (no global rehash) up to SLOTCASK_MAX_SLOTS_PER_SHARD = 16M. The
   floor for all tiers is 64K so even max-splits objects have headroom.
   Tuning history: started at 2M flat, dropped to 512K, then 128K flat,
   now per-tier so the kf stays bounded at all splits.
   Public via slotcask_default_slots_for_splits(). */
#define SLOTCASK_MAX_SLOTS_PER_SHARD      (16u * 1024 * 1024)
size_t slotcask_default_slots_for_splits(int splits);
```

becomes:

```c
   Each tier targets ~50 % load at the documented 78K-200K rec/shard
   sweet spot. Per-shard auto-resplit at 80 % load doubles slots in place
   (no global rehash) with NO ceiling: growth is uncapped for the life
   of the object (2026-10-07 growth-model decision); the nightly
   auto-reshard sweep rebalances big objects into more splits. Resplit
   cost scales with the shard's entry count (~4-5 s at ~25M entries).
   The floor for all tiers is 64K so even max-splits objects have headroom.
   Tuning history: started at 2M flat, dropped to 512K, then 128K flat,
   now per-tier so the kf stays bounded at all splits.
   Public via slotcask_default_slots_for_splits(). */
size_t slotcask_default_slots_for_splits(int splits);
```

The inline resplits in `kf_plan_insert_slot` (:2493) and
`kf_plan_window_insert_slot` (:2562) are already uncapped and are
**not touched** — after this plan all four growth sites agree.

**The radix-sort dependency.** `radix_sort_sizes` (slotcask.c:6192)
assumes slot indices are `< 2^24` ("the SLOTCASK_MAX_SLOTS_PER_SHARD
ceiling") and sorts with three 8-bit passes — silently mis-sorting
once one shard grows past 16,777,216 slots. With growth uncapped that
is a correctness bug on the kf-compaction path (caller slotcask.c:7387).
Pass count becomes adaptive to the actual maximum. Quoted anchor
(slotcask.c:6192–6217, the full function):

```c
/* LSD radix sort for kf slot indices: values are < 2^24 (the
   SLOTCASK_MAX_SLOTS_PER_SHARD ceiling), so three 8-bit counting passes
   suffice and there are no comparator calls. qsort's function-pointer
   comparator over hash-scattered slot vectors was ~2% of bench cycles.
   Scratch must hold n elements; caller falls back to qsort on OOM. */
static void radix_sort_sizes(size_t *a, size_t *scratch, size_t n) {
    if (n < 2) return;
    size_t *src = a, *dst = scratch;
    for (int pass = 0; pass < 3; pass++) {
        size_t count[256] = {0};
        int shift = pass * 8;
        for (size_t i = 0; i < n; i++)
            count[(src[i] >> shift) & 0xff]++;
        size_t sum = 0;
        for (int b = 0; b < 256; b++) {
            size_t c = count[b];
            count[b] = sum;
            sum += c;
        }
        for (size_t i = 0; i < n; i++)
            dst[count[(src[i] >> shift) & 0xff]++] = src[i];
        size_t *t = src;
        src = dst;
        dst = t;
    }
    if (src != a) memcpy(a, src, n * sizeof(*a));
}
```

becomes:

```c
/* LSD radix sort for kf slot indices. Per-shard growth is uncapped
   (2026-10-07 growth-model decision), so slot indices have no constant
   bound — the pass count adapts to the largest value (the old fixed
   three-pass/2^24 assumption silently mis-sorted past 16M slots in one
   shard). No comparator calls; the final memcpy handles odd pass
   parity. qsort's function-pointer comparator over hash-scattered slot
   vectors was ~2% of bench cycles. Scratch must hold n elements;
   caller falls back to qsort on OOM. */
static void radix_sort_sizes(size_t *a, size_t *scratch, size_t n) {
    if (n < 2) return;
    size_t maxv = 0;
    for (size_t i = 0; i < n; i++)
        if (a[i] > maxv) maxv = a[i];
    int npasses = 1;
    while (maxv >= 0x100) { maxv >>= 8; npasses++; }
    size_t *src = a, *dst = scratch;
    for (int pass = 0; pass < npasses; pass++) {
        size_t count[256] = {0};
        int shift = pass * 8;
        for (size_t i = 0; i < n; i++)
            count[(src[i] >> shift) & 0xff]++;
        size_t sum = 0;
        for (int b = 0; b < 256; b++) {
            size_t c = count[b];
            count[b] = sum;
            sum += c;
        }
        for (size_t i = 0; i < n; i++)
            dst[count[(src[i] >> shift) & 0xff]++] = src[i];
        size_t *t = src;
        src = dst;
        dst = t;
    }
    if (src != a) memcpy(a, src, n * sizeof(*a));
}
```

TEST_BUILD wrapper (mirrors `slotcask_test_set_kf_total`'s unguarded
test-helper pattern, slotcask.c:4611) immediately after the function —
without it, the sort's failure boundary is untestable below 16M real
entries (blind-review finding):

```c
/* TEST-ONLY: direct access to the kf slot radix sort so the unit test
   can exercise the >2^24 pass-count behavior without materializing
   16M+ real entries. */
void slotcask_test_radix_sort_sizes(size_t *a, size_t *scratch, size_t n) {
    radix_sort_sizes(a, scratch, n);
}
```

Declaration beside the existing test helper — quoted anchor
(slotcask.h:309–310):

```c
int slotcask_test_set_kf_total(SlotcaskDb *db, int shard_id,
                               uint64_t total, uint64_t deleted);
```

becomes:

```c
int slotcask_test_set_kf_total(SlotcaskDb *db, int shard_id,
                               uint64_t total, uint64_t deleted);
/* W1: TEST access to the kf slot radix sort (static in slotcask.c) —
   unit-tests the adaptive pass count above 2^24. */
void slotcask_test_radix_sort_sizes(size_t *a, size_t *scratch, size_t n);
```

Audit (Task 2 pastes the evidence): the only other 24-bit-masked
values in slotcask.c are a candidate-array index packed into a sort
key (`(base + c2) & 0xffffff`, :5487 — bounded by the bulk window,
not slots) and 32-bit segment-offset masks (:5295, :5317 — unrelated
to slots). Nothing else assumes the 16M ceiling.

### D2b — the reshard-hint log

The growth model removes the ceiling, but "boundless" must not mean
"silent": when a growth event shows that an object has outgrown its
splits per the sizing table, the daemon logs an actionable hint so the
operator can widen now (manual `vacuum --splits=N`) or wait for the
nightly job — which is default-on (D3), so doing nothing is also a
valid answer.

Trigger points are the completions of the growth events themselves —
rare by construction (once per per-shard doubling, plus once per
pregrow call). `reshard_hint_check` recomputes the object's live count
from the kf headers (the same O(1)-per-shard header sum the sweep and
`get_live_count` use) and compares it against the same sizing table
the sweep acts on (`reshard_target_for_count`, types.h:1270). It is
edge-triggered: at most one hint per recommended level per open
(never spam during a bulk load that walks several doublings), and it
stays quiet once the object is widened past the recommendation.

Field — quoted anchor (slotcask.h:268):

```c
    uint64_t reg_refs;
```

becomes:

```c
    uint64_t reg_refs;

    /* W1: bitmask of recommended levels already logged by the reshard
       hint (reshard_hint_check). Levels are powers of two (8..4096),
       so bit = log2(level) — bits 3..12. 0 = nothing logged yet for
       this open. _Atomic: the check runs from parallel shard workers
       and per-shard insert paths with no common lock; fetch_or claims
       a level exactly once. The registry path callocs the struct
       (zero-init); slotcask_open leaves it as-is. Resets on daemon
       restart — fine for a hint. */
    _Atomic unsigned reshard_hint_logged;
```

Helper — placed immediately after `kf_max_slots` (D2's hunk), so every
caller below can see it:

```c
/* W1: after a growth event, check whether the OBJECT has outgrown its
 * splits per the sizing table, and log an actionable hint at most
 * once per recommended level, per open. Concurrent shard workers race
 * here with no common lock, so the level is claimed with a single
 * atomic_fetch_or on a bitmask (levels are powers of two, so bit =
 * log2(level)): exactly one worker can claim a level, and a stale or
 * reordered recommendation cannot re-log a claimed level — an
 * atomic_exchange on "the last level" does not survive reordering
 * (16 -> 32 -> 16 re-logs 32; blind-review round two). The header sum
 * is ~1 ms cold, behind a rare event; the nightly sweep acts on the
 * same table. */
static void reshard_hint_check(SlotcaskDb *db) {
    if (!db || db->num_shards <= 0 || db->num_shards >= MAX_SPLITS)
        return;
    uint64_t total = 0, deleted = 0;
    if (slotcask_sum_kf_totals(db, &total, &deleted) != 0) return;
    long long live = (long long)(total - deleted);
    int target = reshard_target_for_count(live);
    if (target <= db->num_shards) return;
    unsigned bit = 0;
    for (unsigned v = (unsigned)target; v > 1; v >>= 1) bit++;
    unsigned mask = 1u << bit;
    unsigned prev = atomic_fetch_or(&db->reshard_hint_logged, mask);
    if (prev & mask) return;      /* this level was already hinted */
    char eff_root[PATH_MAX], object[256];
    split_data_dir(db->data_dir, eff_root, sizeof(eff_root),
                   object, sizeof(object));
    LOG_INFO(LOG_SUB_SLOTCASK,
             "RESHARD-HINT: %s/%s holds ~%llu live records at splits=%d "
             "— the sizing table recommends splits=%d. Run "
             "`vacuum --splits=%d` now, or let the nightly auto-reshard "
             "handle it (AUTO_RESHARD_ENABLE, default on).",
             eff_root, object, (unsigned long long)live,
             db->num_shards, target, target);
}
```

(`MAX_SPLITS` comes from types.h; `slotcask_sum_kf_totals` is declared
in slotcask.h:319 and returns **(total, deleted)** — live is the
difference, the hint is about live records; `split_data_dir` is static
in slotcask.c:1351 — this helper lives in slotcask.c, so both are in
scope. Executor verifies slotcask.c sees `MAX_SPLITS`/
`reshard_target_for_count` via its existing types.h include.)

Call sites — the four growth completions. The twin plan-function
blocks (identical text at kf_plan_insert_slot :2493 and
kf_plan_window_insert_slot :2562; apply at both):

```c
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
        }
    }
```

becomes (both sites):

```c
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            if (kfcache_resplit_locked(kh, kh->capacity * 2) == 0)
                reshard_hint_check(db);
        }
    }
```

The pre-grow worker — quoted anchor (after D2's loop hunk):

```c
        while (kh.capacity < kf_max_slots() &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
    writer_gate_unlock(a->db, a->kf_shard_id);
    return NULL;
```

becomes:

```c
        while (kh.capacity < kf_max_slots() &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
    writer_gate_unlock(a->db, a->kf_shard_id);
    reshard_hint_check(a->db);
    return NULL;
```

The bulk stage path — quoted anchor (slotcask.c:7080–7086):

```c
    if (kh.hdr) {
        uint64_t projected = kh.hdr->total + (uint64_t)n;
        while (kh.capacity < kf_max_slots() &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
```

becomes:

```c
    if (kh.hdr) {
        uint64_t projected = kh.hdr->total + (uint64_t)n;
        while (kh.capacity < kf_max_slots() &&
               projected * 4 >= (uint64_t)kh.capacity * 3) {
            if (kfcache_resplit_locked(&kh, kh.capacity * 2) != 0) break;
        }
    }
    kfcache_release(&kh);
    reshard_hint_check(req->db);
```

(`req` is in scope — the txn assembly immediately below reads
`req->db`.)

### D3 — nightly reshard default-on

With growth uncapped, the nightly auto-reshard sweep is the only thing
that keeps shards near the sweet spot — its default flips on. The
field is zero-init today (off) and overridden by `AUTO_RESHARD_ENABLE`
in db.env when set. `db_defaults_set` is exported for the defaults
unit case (Task 3). Quoted anchor (embedded.c:39):

```c
static void db_defaults_set(ShardDb *db) {
```

becomes:

```c
/* W1: exported for test_kf_auto_defaults — the defaults unit case
 * asserts the knob default on a scratch instance (a process hosts
 * one ShardDb, so the case must not boot a second). */
void db_defaults_set(ShardDb *db) {
```

Declaration — quoted anchor (shard_db_internal.h:570–571):

```c
/* Internal open — used by cmd_server and shard_db_open. */
ShardDb *shard_db_open_internal(const char *db_root);
```

becomes:

```c
/* Internal open — used by cmd_server and shard_db_open. */
ShardDb *shard_db_open_internal(const char *db_root);

/* W1: instance-default knobs applied at allocation, before the db.env
 * parse. Exported so the defaults unit case can assert them directly
 * without booting a second instance. */
void db_defaults_set(ShardDb *db);
```

Default inside `db_defaults_set` — quoted anchor (embedded.c:75–76):

```c
    db->durability_test_pause_ms       = 0;
    memcpy(db->warmup_mode, "async", 6);
}
```

becomes:

```c
    db->durability_test_pause_ms       = 0;
    memcpy(db->warmup_mode, "async", 6);
    /* W1: nightly reshard sweep default-on — with growth uncapped it
     * is the mechanism that keeps shards near the sweet spot. The
     * db.env parse below overrides it. */
    db->auto_reshard_enable             = 1;
}
```

Behavior change for existing installs that never set the knob: the
nightly sweep (AUTO_RESHARD_HOUR, default 3am) widens out-of-band
objects automatically, each reshard holding the exclusive objlock for
its duration. Explicit `AUTO_RESHARD_ENABLE=0` keeps today's behavior.

## Call-site / consumer inventory

- slotcask.c: pre-grow loop (:2311), stage loop (:7081), the
  `kf_max_slots` helper, the `reshard_hint_check` helper + its four
  call sites (both plan functions, pre-grow worker, stage path), the
  open-site seam (:4303), `radix_sort_sizes` (:6192), the two seam
  globals (:49 block).
- slotcask.h: `SLOTCASK_MAX_SLOTS_PER_SHARD` removed; comment updated;
  one new `SlotcaskDb` field (`reshard_hint_logged`, `_Atomic`
  bitmask); one new TEST decl
  (`slotcask_test_radix_sort_sizes`).
- embedded.c: `db_defaults_set` un-static'd + default; the
  `auto_reshard_sweep_one`/`auto_reshard_thread` (server.c) are not
  modified — the default flip makes them run for installs that never
  set the knob.
- shard_test_ctl.h: two seam declarations + two reset lines.
- test_control.c / fixtures.c / fixtures.h: SET_GLOBALS kind, struct
  grows two `uint64_t`, one fixture sender.
- **No changes** to query_bulk.c, storage.c, query_find.c, server.c,
  btree.c, index.c. Not a wire or on-disk format change (kf files
  simply grow past the sizes the old cap allowed; reopen already
  accepts larger-than-expected kf files — see
  `kf_open_file`'s "existing file is bigger than expected" branch,
  exercised by test-slotcask-resplit).
- New test case: `test_kf_uncapped_growth.c`; modified: none.
- Docs: tuning.md (growth model rewrite), configuration.md
  (AUTO_RESHARD_ENABLE default), changelog.

## Tasks

Execution on a fresh branch off `main`, work left uncommitted per this
repo's standing mode. Builds: `SKIP_TESTS=1 ./build.sh`; focused tests:
`./build/bin/shard-db-test run <name>`. Standard halt rule: any quoted
anchor not found exactly → write `PLAN_NOTES.md` and halt the run.
No benches as executor; sanitizer gates are Task 5.

### Task 1 — D1 seams + the red test

Implement all of D1. Then add `src/test/cases/test_kf_uncapped_growth.c`,
mirroring `test_slotcask_resplit.c`'s standalone-slotcask idiom
(`slotcask_init` / `slotcask_open(&db, dir, 8, 1, 256)` /
`slotcask_close` / `slotcask_shutdown` / tmpdir cleanup). Red mode:
**compile-red on base** (the seam globals and the radix-sort wrapper
do not exist yet — repo precedent, cf. test-request-flush-batching's
header note), and after Task 1 lands (seams exist, loops still read
the old constant) **behaviorally red** at the first assertion: the
armed max must hold the pre-grow loop down and doesn't.

`src/test/cases/test_kf_uncapped_growth.c`, complete:

```c
/* src/test/cases/test_kf_uncapped_growth.c
 *
 * W1 — unbounded per-shard kf growth
 * (docs/plans/2026-09-14-auto-widen-kf-ceiling.md, reissued 2026-10-07).
 *
 * Standalone slotcask unit case (no daemon — mirrors
 * test-slotcask-resplit.c):
 *   1. the TEST_BUILD max seam holds the pre-grow loop down
 *      (red while the loops still read the old 16M constant),
 *   2. without the seam, growth is unbounded: thousands of inserts
 *      into 64-slot shards tier through many doublings with zero
 *      refusals (regression guard for the growth model),
 *   3. every record survives the doublings and a close/reopen.
 *
 * Task 2 appends: Part 4 (the reshard hint records its
 * recommendation) and Part 5 (the kf slot radix sort stays correct
 * across the old 2^24 bound).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "slotcask.h"
#include "shard_test_ctl.h"

#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* W1 review fix: no shell interpolation — mkdtemp creates the
 * directory (0700, collision-free) and an nftw depth-first walk
 * removes it, so SHARD_TEST_TMPDIR metacharacters or spaces can
 * neither execute anything nor split the path. */
static int rm_rf_callback(const char *fpath, const struct stat *sb,
                          int typeflag, struct ftw *ftwbuf) {
    (void)sb; (void)typeflag; (void)ftwbuf;
    return remove(fpath);   /* depth-first: files first, then the dir */
}

static void rm_rf(const char *path) {
    if (!path || !*path) return;
    nftw(path, rm_rf_callback, 16, FTW_DEPTH | FTW_PHYS);
}

static void unique_tmpdir(char out[256]) {
    const char *base = getenv("SHARD_TEST_TMPDIR");
    if (!base || !*base) base = "/tmp";
    snprintf(out, 256, "%s/shard_kf_growth_XXXXXX", base);
    if (!mkdtemp(out)) out[0] = '\0';
}

static size_t shard0_capacity(const char *dir) {
    char kf_path[512];
    snprintf(kf_path, sizeof(kf_path), "%s/data/kf/000.kf", dir);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, 64, 0) != 0) return 0;
    size_t cap = kh.capacity;
    kfcache_release(&kh);
    return cap;
}

static int test_kf_uncapped_growth_run(void) {
    char dir[256];
    unique_tmpdir(dir);
    ASSERT_TRUE(dir[0] != '\0', "mkdtemp tmpdir created");
    if (!dir[0]) return 1;
    slotcask_init(64, 64);

    /* --- Part 1: the max seam holds the growth loops down ---
     * initial = max = 64: a pregrow projection of 1000 records must
     * NOT grow capacity past the test-only cap. Red while the loops
     * read the old 16M constant (they grow to 2048). */
    atomic_store(&g_shard_test_kf_initial_slots, 64);
    atomic_store(&g_shard_test_kf_max_slots, 64);

    SlotcaskDb db;
    int rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds (initial=64)");
    if (rc != 0) { rm_rf(dir); return 1; }

    /* pregrow creates any not-yet-materialised shard files at the
     * seam's initial capacity, then (new code) declines to grow past
     * the seam's max. */
    ASSERT_EQ_INT(slotcask_pregrow_kf(&db, 1000), 0, "pregrow runs");
    ASSERT_EQ_INT(shard0_capacity(dir), 64,
                  "pregrow honored the max seam (capacity stays 64)");

    slotcask_close(&db);
    rm_rf(dir);

    /* --- Part 2: uncapped growth, end to end ---
     * No max seam (0 = uncapped, production behavior). 5,000 records
     * into 64-slot shards forces ~4 doublings per shard; nothing may
     * refuse. (Green-only on base too — the inline resplits were
     * always uncapped; this locks the model in.) */
    atomic_store(&g_shard_test_kf_initial_slots, 64);
    atomic_store(&g_shard_test_kf_max_slots, 0);

    rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "slotcask_open succeeds (uncapped)");
    if (rc != 0) { rm_rf(dir); return 1; }

    enum { NREC = 5000 };
    int inserted = 0;
    for (int i = 0; i < NREC; i++) {
        char k[32], v[64];
        snprintf(k, sizeof(k), "grow-%06d", i);
        snprintf(v, sizeof(v), "value-%d", i);
        if (slotcask_insert_with_hooks(&db, -1, k, strlen(k), v,
                                       strlen(v), NULL, NULL) == 0)
            inserted++;
    }
    ASSERT_EQ_INT(inserted, NREC,
                  "all 5000 inserts succeed across the doublings");

    size_t cap_after = shard0_capacity(dir);
    ASSERT_TRUE(cap_after > 64,
                  "shard 0 tiered up past its initial 64 slots");

    int readable = 0;
    for (int i = 0; i < NREC; i += 97) {         /* spot-check stride */
        char k[32];
        snprintf(k, sizeof(k), "grow-%06d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable++;
            free(v);
        }
    }
    ASSERT_TRUE(readable > 0, "spot-checked records readable post-growth");

    slotcask_close(&db);

    /* Reopen: the larger kf files are picked up automatically and all
     * spot-checked records survive. */
    rc = slotcask_open(&db, dir, 8, 1, 256);
    ASSERT_EQ_INT(rc, 0, "reopen after growth succeeds");
    readable = 0;
    for (int i = 0; i < NREC; i += 97) {
        char k[32];
        snprintf(k, sizeof(k), "grow-%06d", i);
        void *v = NULL; size_t vl = 0;
        if (slotcask_get(&db, k, strlen(k), &v, &vl) == 0) {
            readable++;
            free(v);
        }
    }
    ASSERT_TRUE(readable > 0, "spot-checked records readable after reopen");

    atomic_store(&g_shard_test_kf_initial_slots, 0);
    atomic_store(&g_shard_test_kf_max_slots, 0);
    shard_test_ctl_reset();
    slotcask_close(&db);
    slotcask_shutdown();
    rm_rf(dir);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-uncapped-growth", test_kf_uncapped_growth_run)
```

Notes for the executor: `kfcache_acquire`'s third argument is the
create-if-absent capacity (writer=0 never creates, so the value is
inert here); kf paths are `<data_dir>/data/kf/NNN.kf` (storage model,
AGENTS.md); the file-size observation trick from test-slotcask-resplit
(`st_size == 24 + cap×24`) is the alternative if `kh.capacity` ever
changes shape. `slotcask_open` zeroes the whole struct (`memset(db, 0,
sizeof(*db))` at entry), so no test-side initialization is needed.
Paste the real red output (Task-1 state: "pregrow honored the max
seam" fails, got 256 — pregrow distributes 1000 over 8 shards, 125
per shard, and the base loop grows 64 → 128 → 256, stopping at the
first tier where 125×4 < capacity×3).

### Task 2 — D2 + D2b (uncapped loops, radix fix, reshard hint)

Implement all of D2 and D2b. Then extend the Task 1 case with Parts 4
and 5 below (they compile only once this task's additions exist):
append Part 4 immediately after the Part-1 assertion, and Part 5
immediately before the seam-reset block — insertion anchors:

```c
    ASSERT_EQ_INT(shard0_capacity(dir), 64,
                  "pregrow honored the max seam (capacity stays 64)");

    slotcask_close(&db);
    rm_rf(dir);
```

becomes:

```c
    ASSERT_EQ_INT(shard0_capacity(dir), 64,
                  "pregrow honored the max seam (capacity stays 64)");

    /* --- Part 4 (D2b): the reshard hint --- spike every shard's
     * header total past the 1M tier boundary, then nudge a pregrow
     * (its hint check sums the live headers): the hint bitmask must
     * record the recommendation (16 for ~1.6M records at splits=8).
     * This is the state behind the RESHARD-HINT log line. */
    for (int s = 0; s < 8; s++)
        ASSERT_EQ_INT(slotcask_test_set_kf_total(&db, s, 200000, 0), 0,
                      "synthetic header totals written");
    ASSERT_EQ_INT(slotcask_pregrow_kf(&db, 1), 0, "pregrow nudge runs");
    /* splits=16 recommendation → bit 4 → mask 0x10. */
    ASSERT_EQ_INT(atomic_load(&db.reshard_hint_logged), 16,
                  "reshard hint recorded the splits=16 recommendation");

    slotcask_close(&db);
    rm_rf(dir);
```

and:

```c
    ASSERT_TRUE(readable > 0, "spot-checked records readable after reopen");

    atomic_store(&g_shard_test_kf_initial_slots, 0);
```

becomes:

```c
    ASSERT_TRUE(readable > 0, "spot-checked records readable after reopen");

    /* --- Part 5 (D2): radix sort beyond the old 24-bit ceiling ---
     * Uncapped growth lets one shard exceed 16,777,216 slots; the old
     * fixed three-pass radix silently mis-sorted there (it ordered
     * 0x1000000 before 5, since only the low 24 bits were compared).
     * These values straddle the bound; the adaptive pass count must
     * produce the full ordering. */
    {
        enum { RN = 9 };
        size_t vals[RN] = {
            0, 5, 0xfffffe, 0xffffff, 0x1000000, 0x1000001,
            0x12345678, 0x7fffffff, 0xffffffff
        };
        size_t scratch[RN], expect[RN];
        memcpy(expect, vals, sizeof(vals));
        for (size_t i = 1; i < RN; i++) {      /* reference insertion sort */
            size_t key = expect[i];
            long long j = (long long)i - 1;
            while (j >= 0 && expect[j] > key) {
                expect[j + 1] = expect[j];
                j--;
            }
            expect[j + 1] = key;
        }
        slotcask_test_radix_sort_sizes(vals, scratch, RN);
        ASSERT_TRUE(memcmp(vals, expect, sizeof(vals)) == 0,
                    "radix sort correct across the 2^24 boundary");
    }

    atomic_store(&g_shard_test_kf_initial_slots, 0);
```

Focused greens: Part 1 passes (the max seam now bites), Part 4 passes
(the hint records the splits=16 recommendation), Part 5 passes (radix
across the 2^24 boundary); Parts 2–3 unchanged green;
`test-slotcask-resplit` (inline resplit untouched);
`test-slotcask-v2-bulk`; `test-auto-reshard`.
Radix regression proof (revert-check-reapply): with Part 5 green,
temporarily restore the fixed `for (int pass = 0; pass < 3; pass++)`
loop in `radix_sort_sizes` and rerun — Part 5 must FAIL (0x1000000
sorts before 5, since only the low 24 bits are compared) — then
re-apply and confirm green. Paste all outputs. Also paste the audit
grep: `grep -n 'SLOTCASK_MAX_SLOTS_PER_SHARD\|2\^24\|0xffffff' src/db/slotcask.c`
shows only the candidate-index and segment-offset masks (not slot
values) after this task.

### Task 3 — D3 (nightly reshard default-on), test first

**Red before the fix.** First apply only the two `db_defaults_set`
export hunks (infra — no behavior change), then add the test below and
run it: the assertion fails with got 0 on the old default. Paste that
red output. Only then apply the default hunk and rerun: green.

`src/test/cases/test_kf_auto_defaults.c`, complete:

```c
/* src/test/cases/test_kf_auto_defaults.c
 *
 * W1 D3: the nightly reshard sweep defaults ON (pre-W1 it defaulted
 * to 0). Asserts on a calloc'd scratch instance rather than the
 * runner's process-local ShardDb so neither db.env parsing nor case
 * order can influence the result.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "shard_db_internal.h"

#include <stdlib.h>
#include <string.h>

static int test_kf_auto_defaults_run(void) {
    ShardDb *db = calloc(1, sizeof(ShardDb));
    ASSERT_NOT_NULL(db, "scratch ShardDb alloc");
    if (!db) return 1;

    db_defaults_set(db);
    ASSERT_EQ_INT(db->auto_reshard_enable, 1, "AUTO_RESHARD_ENABLE default 1");

    free(db);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-kf-auto-defaults", test_kf_auto_defaults_run)
```

Run it against the export-only tree: the assertion must fail with
got 0 (the old zero-init default) — paste that red output. Then apply
the `db->auto_reshard_enable = 1;` hunk from D3 above and rerun:
green. `test-auto-reshard` must stay green throughout (it manipulates
the flag explicitly; the default flip does not change explicit-set
behavior). Paste both greens.

### Task 4 — documentation

- `docs/operations/tuning.md`: rewrite the capacity ladder — per-shard
  growth is uncapped (no 16M ceiling anywhere in the current docs
  should survive); the tier table stays as the *initial* sizing; the
  nightly sweep is the rebalancer (AUTO_RESHARD_ENABLE now default
  on); the resplit-stall trade-off is stated with the observed
  ~4–5 s @ ~25M-entry scaling; the sweet-spot guidance stays.
  Document the `RESHARD-HINT` log line: when it fires (a growth event
  on an object whose live count outgrows its splits per the sizing
  table), that it fires at most once per recommended level, and what
  the two responses are (manual `vacuum --splits=N`, or wait for the
  default-on nightly sweep).
- `docs/getting-started/configuration.md`: AUTO_RESHARD_ENABLE /
  AUTO_RESHARD_HOUR default-table update (default on at 3am; explicit
  0 keeps the old behavior).
- **Core references still describing the old ceiling (blind-review
  rounds two and three)** — complete replacement content, all in the
  same branch.
  - `docs/concepts/storage-model.md` — quoted current paragraph
    (:137):

    ```markdown
    There is **no per-shard slot cap** in normal operation — the shard keeps doubling until the global `SLOTCASK_MAX_SLOTS_PER_SHARD = 16M` ceiling, at which point `kf_put_new` refuses further inserts and the operator must `vacuum --splits=N` to widen the keyspace. At 80 % load that's ~12.8M live records per shard; a `splits=4096` object can hold tens of billions of live records before any single shard hits the ceiling — well into "you should be partitioning" territory.
    ```

    becomes:

    ```markdown
    There is **no per-shard slot cap**: the shard keeps doubling for the life of the object, and inserts never refuse for capacity. At 80 % load a doubling tier holds ~12.8M live records per shard, and the resplit cost scales with the shard's entry count (~4–5 s at ~25M entries), which is the practical reason not to let one shard grow forever: when the object's live count outgrows its splits, the sizing table recommends more of them — the daemon logs a `RESHARD-HINT` line at the growth event, and the nightly auto-reshard sweep (default on; `AUTO_RESHARD_ENABLE=0` to disable) widens the object into the recommended `splits` automatically. A `splits=4096` object spans tens of billions of live records — well into "you should be partitioning" territory.
    ```

    (The resplit-timing sentence above it — "~80–160 ms at 16M slots"
    — stays as-is; it describes per-doubling latency, not a cap.)
  - `docs/reference/limits.md` — quoted current table row (:19):

    ```markdown
    | `SLOTCASK_MAX_SLOTS_PER_SHARD` | 16 777 216 (16M) | Per-kf-shard slot ceiling. Resplits stop here; further inserts to a full shard refuse and require `vacuum --splits=N` to widen the keyspace. |
    ```

    becomes:

    ```markdown
    | Per-kf-shard slot growth | unbounded | Auto-resplit doubles a shard's slots at ~75–80 % load for the life of the object — inserts never refuse for capacity. When the sizing table says the object outgrew its splits, a `RESHARD-HINT` log line fires and the nightly auto-reshard sweep (default on) widens it. |
    ```
  - `AGENTS.md` — quoted current storage-model bullet (:88):

    ```markdown
    - **Dynamic growth**: at 75–80% kf load the shard doubles `slots_per_shard` **in place** (per-shard, no global rehash; pre-grow targets ≤75%, inline resplit fires at 80%), up to `SLOTCASK_MAX_SLOTS_PER_SHARD` = 16M. `MAX_SPLITS=4096` caps shard *files*, not slots.
    ```

    becomes:

    ```markdown
    - **Dynamic growth**: at 75–80% kf load the shard doubles `slots_per_shard` **in place** (per-shard, no global rehash; pre-grow targets ≤75%, inline resplit fires at 80%) with **no ceiling** — growth continues for the life of the object and inserts never refuse for capacity. `MAX_SPLITS=4096` caps shard *files*, not slots; the nightly auto-reshard sweep (default on) widens objects whose record counts outgrow their splits, and a `RESHARD-HINT` log line fires inline when the sizing table says they have.
    ```
- `db.env.example`: AUTO_RESHARD_ENABLE default comment flip if the
  file states the default.
- `docs/reference/changelog.md` `## Unreleased`: per-shard kf growth
  is uncapped (the documented 16M ceiling no longer exists; growth
  tiers continue for the life of the object); nightly reshard sweep
  default-on (behavior change headline); radix-sort fix note
  (correctness for shards past 16M slots).

### Task 5 — human definition-of-done gates

Per AGENTS.md: full suite once, then ASan+UBSan
(`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, three fresh `run-all`)
and TSan (`BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, three fresh
`TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
./build/bin/shard-db-test run-all --jobs 2`), no suppressions, no
`halt_on_error=0`. This diff touches shared growth state — the local
sanitizer gate is mandatory.

## Out of scope

- Auto-widen in any form — moot: with no slot ceiling there is no
  capacity refusal to recover from. (Disk-full ENOSPC surfaces
  unchanged.)
- Any change to `MAX_SPLITS = 4096`, the 3-hex-digit kf naming, or
  `slotcask_default_slots_for_splits` tiering (unchanged initial
  sizing).
- Per-shard extendible hashing / local depth — unnecessary under the
  uncapped model.
- Raising the practical 32-bit probe-start limit — unreachable at
  human scales.
