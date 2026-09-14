# W1 — auto-widen on kf slot ceiling + reshard defaults

Date: 2026-09-14  
Status: draft for review — not approved or executable until the human
approves.  
Depends on: nothing. Independent of B3b
(`2026-09-09-b3b-bulk-window-merge.md`): no file overlap. Branch off
`main` when executed.

## Goal and root cause

An object's per-shard kf capacity doubles in place automatically up to
`SLOTCASK_MAX_SPLITS`-independent ceiling of 16M slots/shard
(`SLOTCASK_MAX_SLOTS_PER_SHARD`, src/db/slotcask.h:87). Past that, the
operator must widen `splits` (`vacuum --splits=N`, online but holding
the exclusive objlock). Three problems make this boundary a trap:

1. **The refusal is silent and generic.** When a shard is genuinely
   full at the ceiling, `kf_plan_insert_slot` (src/db/slotcask.c:2458)
   and `kf_plan_window_insert_slot` (:2516) fall out of their probe loop
   to a plain `return -1` with whatever errno happens to be set — a
   client cannot distinguish "kf slot ceiling reached" from any other
   hard failure, and nothing names the fix.
2. **The ceiling is not even enforced on the single-record path.** The
   inline resplit at the top of `kf_plan_insert_slot` (line 2465,
   `if (cap > 0 && total * 4 >= cap * 3)` → resplit ×2) has no
   `SLOTCASK_MAX_SLOTS_PER_SHARD` guard — only the bulk pre-grow loop
   (slotcask.c:2281) and the bulk stage path (slotcask.c:7024) have it.
   `kfcache_resplit_locked` has no max guard either, so a hot
   single-record shard silently grows 32M, 64M, … slots — the documented
   "hard ceiling" leaks exactly where records trickle in one at a time.
3. **Recovery requires an operator who read the docs.**
   `AUTO_RESHARD_ENABLE` defaults to 0, and a bulk load that crosses the
   boundary mid-day fails until a human runs `vacuum --splits=N` — even
   though the fix is fully mechanical: the sizing table
   (`reshard_target_for_count`, query_maint.c:229) already computes the
   recommended target, and `cmd_vacuum` (query_maint.c:171) already
   performs the online widening.

The design decision (human-approved 2026-09-14): when a bulk write hits
the ceiling, the **server dispatch layer auto-widens and retries the
batch once**. The widening must NOT run mid-insert inside slotcask: it
rehashes every key in the object (splits routing is `hash[0..1] %
splits`, so 8→16 redistributes all shards) under the exclusive objlock,
which is only safe from a context that holds no shard writer gate — the
command dispatch layer (`bulk_execute_prepared` in query_bulk.c, the
choke point all seven bulk call sites route through). Single-record
inserts do not auto-widen (fail with ENOSPC + actionable error); the
nightly sweep covers them.

## Design

### D1 — reachable-ceiling and initial-slots test seams (TEST_BUILD)

The regression tests need a genuinely full kf table without inserting
12.8M records. `SLOTCASK_MAX_SLOTS_PER_SHARD` is consulted at exactly
three sites (slotcask.c:2281, :7024, and the new guard from D2), and the
initial per-shard slot count is assigned once at slotcask_open
(slotcask.c:4251: `db->slots_per_shard =
slotcask_default_slots_for_splits(num_shards);`). Add two TEST_BUILD
override globals in slotcask.c beside the existing test controls
(under the existing `#ifdef TEST_BUILD` block that defines
`g_shard_test_bulk_chains`), with declarations in `shard_test_ctl.h`
beside the chain counter:

```c
/* W1: TEST_BUILD overrides for the per-shard kf slot tier. 0 = off.
 * initial: replaces slotcask_default_slots_for_splits() at open.
 * max:     the effective SLOTCASK_MAX_SLOTS_PER_SHARD at every grow
 *          guard. Tests set both to the same tiny value (e.g. 64) so a
 *          handful of inserts genuinely exhausts a shard, then reset
 *          them to 0. Never leave them set across scenarios — they
 *          change slotcask_open sizing for every later test. */
extern size_t g_shard_test_kf_initial_slots;
extern size_t g_shard_test_kf_max_slots;
```

```c
size_t g_shard_test_kf_initial_slots = 0;
size_t g_shard_test_kf_max_slots = 0;
```

`slotcask.c` gains an effective-max helper immediately after the
`bulk_free_stream_buckets` definition (any position before first use at
the D2 guard sites):

```c
/* W1: effective per-shard slot ceiling — TEST_BUILD tests shrink it to
 * make the ENOSPC path reachable without million-record fixtures. */
static size_t kf_max_slots(void) {
#ifdef TEST_BUILD
    if (g_shard_test_kf_max_slots) return g_shard_test_kf_max_slots;
#endif
    return (size_t)SLOTCASK_MAX_SLOTS_PER_SHARD;
}
```

The open-site hunk replaces the quoted:

```c
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);
```

with:

```c
    db->slots_per_shard = slotcask_default_slots_for_splits(num_shards);
#ifdef TEST_BUILD
    if (g_shard_test_kf_initial_slots)
        db->slots_per_shard = g_shard_test_kf_initial_slots;
#endif
```

The two existing grow guards replace their ceiling comparisons:

```c
        while (kh.capacity < SLOTCASK_MAX_SLOTS_PER_SHARD &&
```

becomes (both sites — slotcask.c:2281 and :7024, identical text):

```c
        while (kh.capacity < kf_max_slots() &&
```

`shard_test_ctl_reset()` appends:

```c
    g_shard_test_kf_initial_slots = 0;
    g_shard_test_kf_max_slots = 0;
```

so any test that arms the overrides and forgets to disarm cannot poison
a later scenario's `slotcask_open`.

### D2 — distinct ceiling refusal + the missing ceiling guard

In `kf_plan_insert_slot`, replace the quoted inline-resplit block:

```c
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
        }
    }
```

with:

```c
    if (kh->hdr) {
        uint64_t total = kh->hdr->total;
        uint64_t cap = (uint64_t)kh->capacity;
        if (cap > 0 && total * 4 >= cap * 3 &&
            cap < (uint64_t)kf_max_slots()) {
            (void)kfcache_resplit_locked(kh, kh->capacity * 2);
        }
    }
```

Both probe-full exits — the final `return -1;` that terminates
`kf_plan_insert_slot` (after its `first_tomb` block) and the final
`return -1;` that terminates `kf_plan_window_insert_slot` (after its
`first_tomb` block, quoted below as it stands today) — become a
distinct, documented refusal:

```c
    if (first_tomb != (size_t)-1) {
        out_plan->target_slot = first_tomb;
        out_plan->reused_tomb = 1;
        memcpy(out_plan->hash, hash, 16);
        out_plan->key = key;
        out_plan->klen = klen;
        return 0;
    }
    return -1;
}
```

becomes:

```c
    if (first_tomb != (size_t)-1) {
        out_plan->target_slot = first_tomb;
        out_plan->reused_tomb = 1;
        memcpy(out_plan->hash, hash, 16);
        out_plan->key = key;
        out_plan->klen = klen;
        return 0;
    }
    /* Probe exhausted: table full at the per-shard slot ceiling (the
     * 75% inline resplit above is a no-op once kf_max_slots() is
     * reached). ENOSPC is the W1 auto-widen trigger; the dispatcher
     * distinguishes it from disk-full ENOSPC by re-checking the actual
     * ceiling condition before acting (see D3). */
    errno = ENOSPC;
    return -1;
}
```

Both functions end with that identical trailing block, so the executor
applies it at both sites (the enclosing function name in each edit
disambiguates).

Server-side error text: bulk commands that fail with the ceiling error
after D3's retry also return it. The wire mapper for insert errors
already renders `errno`-derived messages; the plan adds no new wire
shape — the ENOSPC path after a failed retry surfaces through the
existing hard-error mapping. No wire-format change.

### D3 — dispatch-layer auto-widen + retry once

New public helper in slotcask.c/h — `slotcask_at_slot_ceiling`
(declared in slotcask.h beside `slotcask_sum_kf_totals`, defined beside
its implementation). It re-checks the real ceiling condition so disk-full
`ENOSPC` never triggers a widening:

```c
/* W1: 1 when any kf shard is at its per-shard slot ceiling and at/over
 * the 75% resplit trigger — i.e. the state that makes inserts refuse
 * with ENOSPC. Cheap: one kf rdlock per shard, header read only. */
int slotcask_at_slot_ceiling(SlotcaskDb *db);
```

```c
int slotcask_at_slot_ceiling(SlotcaskDb *db) {
    if (!db || db->num_shards <= 0) return 0;
    size_t max_slots = kf_max_slots();
    for (int s = 0; s < db->num_shards; s++) {
        SlotcaskKfHandle kh;
        if (kf_shard_acquire(&kh, db, s, 1) != 0) continue;
        int full = kh.hdr && kh.capacity >= max_slots &&
                   (uint64_t)kh.hdr->total * 4 >=
                   (uint64_t)kh.capacity * 3;
        kfcache_release(&kh);
        if (full) return 1;
    }
    return 0;
}
```

In query_bulk.c, `bulk_execute_prepared` (line 1074) — the choke point
through which all seven bulk call sites execute — is replaced wholesale.
It currently reads:

```c
static int bulk_execute_prepared(SlotcaskDb *sdb,
                                 SlotcaskBulkShardInput *inputs,
                                 size_t ninputs, size_t pregrow_count) {
    (void)pregrow_count;
    if (ninputs == 0) return 0;
    return slotcask_bulk_request_execute(sdb, inputs, ninputs);
}
```

and becomes:

```c
static int bulk_execute_prepared(SlotcaskDb *sdb,
                                 SlotcaskBulkShardInput *inputs,
                                 size_t ninputs, size_t pregrow_count) {
    (void)pregrow_count;
    if (ninputs == 0) return 0;
    int rc = slotcask_bulk_request_execute(sdb, inputs, ninputs);
    if (rc == 0 || errno != ENOSPC) return rc;
    if (!g_auto_widen_on_full) return rc;
    /* Distinguish the kf slot ceiling from disk-full ENOSPC: only the
     * former is widened. This call holds no shard writer gate, so the
     * exclusive objlock cmd_vacuum takes is safe here. */
    if (!slotcask_at_slot_ceiling(sdb)) return rc;

    uint64_t live = 0, deleted = 0;
    if (slotcask_sum_kf_totals(sdb, &live, &deleted) != 0) return rc;
    int target = reshard_target_for_count((long long)live);
    while (target <= sdb->num_shards) target *= 2;
    if (target > MAX_SPLITS) return rc;

    char eff_root[PATH_MAX], object[256];
    split_data_dir(sdb->data_dir, eff_root, sizeof(eff_root),
                   object, sizeof(object));
    LOG_INFO(LOG_SUB_VACUUM,
             "AUTO-WIDEN: %s/%s hit the kf slot ceiling at splits=%d — "
             "widening to splits=%d online, then retrying the batch once",
             eff_root, object, sdb->num_shards, target);
    if (cmd_vacuum(eff_root, object, 0, target) != 0) {
        LOG_WARN(LOG_SUB_VACUUM,
                 "AUTO-WIDEN: %s/%s vacuum --splits=%d failed; "
                 "reporting the original ENOSPC",
                 eff_root, object, target);
        errno = ENOSPC;
        return rc;
    }
    LOG_INFO(LOG_SUB_VACUUM, "AUTO-WIDEN: %s/%s now splits=%d",
             eff_root, object, target);
    /* Retry once. execute() re-initializes every input's rc/error_no
     * and the degraded flags itself; records keep their caller-owned
     * buffers. If the retry fails too (e.g. MAX_SPLITS reached), its
     * outcome is the answer — no further widening. */
    return slotcask_bulk_request_execute(sdb, inputs, ninputs);
}
```

Prerequisites this hunk relies on, all verified: `cmd_vacuum` and
`reshard_target_for_count` are already declared in types.h (:1268,
:1270); `split_data_dir` and `slotcask_sum_kf_totals` are declared to
query_bulk.c today; `LOG_INFO`/`LOG_WARN`/`LOG_SUB_VACUUM` come from
log.h (already included by query_bulk.c's existing includes — executor
verifies the include line, adds `#include "log.h"` if absent).
`MAX_SPLITS` comes from types.h. `errno` discipline: the widening path
preserves the failing errno when it declines to act.

New knob `AUTO_WIDEN_ON_FULL`, default **1** (opt-out), following the
`auto_vacuum_enable` precedent:

- `shard_db_internal.h` — beside `int auto_vacuum_enable;` (line 287):
  `int auto_widen_on_full;` and beside the `g_auto_vacuum_enable` macro
  (line 469): `#define g_auto_widen_on_full (g_db->auto_widen_on_full)`.
- config.c parse loop — beside the `AUTO_RESHARD_ENABLE` block
  (line 528):

```c
        } else if (strncmp(p, "AUTO_WIDEN_ON_FULL=", 19) == 0) {
            if (g_db) g_auto_widen_on_full = (atoi(p + 19) != 0);
```

- default-on: db.env parse defaults. The daemon sets defaults where the
  ServerDb instance is initialized (fields are zero-init today). The
  executor locates the instance initialization (the same site that must
  gain the D4 default below — one insertion covers both) and adds:

```c
    db->auto_widen_on_full = 1;
```

- `db.env.example` + `docs/getting-started/configuration.md` rows.

### D4 — `AUTO_RESHARD_ENABLE` default 0 → 1

At the same ServerDb initialization site as D3's default:

```c
    db->auto_reshard_enable = 1;
```

Behavior change for existing installs that never set the knob: the
nightly sweep (3am) now widens out-of-band objects automatically, each
reshard holding the exclusive objlock for its duration. This must be a
changelog headline and a configuration.md default-table update. Explicit
`AUTO_RESHARD_ENABLE=0` in db.env keeps today's behavior (config parse
overwrites the default).

## Call-site / consumer inventory

- `kf_plan_insert_slot` / `kf_plan_window_insert_slot` (slotcask.c):
  ceiling guard + ENOSPC. Callers: every insert path, single and bulk —
  behavior-identical except at the previously-unreachable/leaky ceiling.
- Grow guards at slotcask.c:2281 and :7024: comparison-only change
  (`kf_max_slots()`), production value identical
  (`SLOTCASK_MAX_SLOTS_PER_SHARD`).
- `slotcask_open` slot-tier assignment: TEST_BUILD-only override.
- `bulk_execute_prepared` (query_bulk.c): all seven bulk call sites
  (1784, 2542, 3082, 3848, 4389, 5470, 5900) gain the widen+retry
  behavior with zero signature changes.
- `cmd_vacuum` / `reshard_target_for_count`: reused, not modified.
- `auto_reshard_sweep_one` / `auto_reshard_thread` (server.c): not
  modified; the default flip makes them run for installs that never set
  the knob.
- `SlotcaskDb`/ServerDb struct: one new int field; daemon, test, bench,
  library targets rebuild. Not a wire or on-disk format change.
- TEST-only: two new override globals consumed by the new regression
  test only.
- Docs: `docs/getting-started/configuration.md`,
  `docs/operations/tuning.md` (capacity ladder: ~12.8M live/shard
  ceiling, widening trigger, `vacuum --splits`, auto behaviors),
  `db.env.example`, `docs/reference/changelog.md`.

## Tasks

Execution on a fresh branch off `main`, work left uncommitted, standard
halt rule on any missing quoted anchor. Builds: `SKIP_TESTS=1
./build.sh`; focused tests: `./build/bin/shard-db-test run <name>`. No
benches, no sanitizer gates as executor.

### Task 1 — red test

New case `src/test/cases/test_kf_ceiling_auto_widen.c` driving the
server (the auto-widen lives at the dispatch layer, so the test must go
through the wire, mirroring an existing server-exercising case's
daemon-start pattern). With the D1 seams armed
(`initial = max = 64`, splits=8 → 512-record object capacity):

1. Insert 400 unique records via bulk — expect 0.
2. Insert 400 more via bulk — on base this FAILS with a generic error;
   after D3 it must return 0 (auto-widened to splits=16, capacity
   per-shard back at tier size), and `describe-object` must report
   splits=16; every record from both batches must be readable.
3. Disarm the seams; insert one more record normally — sanity rc 0.
4. Unit-level: with seams armed, a direct
   `slotcask_upsert_with_hooks` into a full shard must return -1 with
   `errno == ENOSPC` (D2) — on base errno is not ENOSPC (red).
5. Auto-widen opt-out: set `g_db->auto_widen_on_full = 0` (or run with
   the knob armed-off via a second daemon), repeat scenario 2, expect
   failure with errno ENOSPC and splits unchanged.

Run and paste the real red output (scenario 2 fails, scenario 4's
errno assertion fails). Do not use sleeps to await the widening — the
retry is synchronous inside the same request.

### Task 2 — D1 + D2 (seams, ceiling guard, ENOSPC)

Implement D1 and D2. Focused green: the new case's scenario 4 passes;
`test-request-flush-batching`, `test-bulk-upsert`,
`test-slotcask-v2-bulk`, `test-auto-reshard` (existing auto-reshard
coverage) all pass. Paste outputs.

### Task 3 — D3 (auto-widen + retry) and the knob

Implement `slotcask_at_slot_ceiling` and the `bulk_execute_prepared`
replacement; add the knob. Scenario 2 and 5 of the red test go green.
Also cover: ceiling hit on ONE shard only (hash-targeted keys) widens
and succeeds; disk-full-shaped ENOSPC (inject a segment-write failure
via an existing failure-injection control if one applies, else
`ENOSPC`-staged via a read-only segment path) does NOT widen — verify
via logs (no AUTO-WIDEN line) and the passed-through error. Paste
outputs.

### Task 4 — D4 (default flip)

`auto_reshard_enable = 1` default + knob wiring. Existing
`test-auto-reshard` must stay green (it manipulates the flag); add a
default assertion if the harness exposes the flag post-open.

### Task 5 — documentation

configuration.md (two knobs), tuning.md capacity-ladder section (the
`~12.8M × splits` ceiling, widening trigger, auto behaviors, the
"create-time pre-sizing for known-big loads" rule), db.env.example,
changelog `## Unreleased` headline (default flip is a behavior change).

### Task 6 — human definition-of-done gates

Identical protocol to prior plans: full suite once, then ASan and TSan
builds, three fresh `run-all` runs each, no suppressions, all-core
parallelism. Sanitizer findings are root-caused, never papered over.

## Out of scope

- Mid-insert (inside-slotcask) widening — rejected by design: whole-
  object rehash under exclusive objlock cannot run from a context
  holding a shard writer gate.
- Per-shard independent splitting (extendible-hashing-style local
  depth) — a storage-format change; revisit only if the widening pause
  at multi-billion-record objects ever becomes a problem.
- Changes to `MAX_SPLITS = 4096` or the 3-hex-digit kf naming.
- B3b branch work (runs independently off `main`).
