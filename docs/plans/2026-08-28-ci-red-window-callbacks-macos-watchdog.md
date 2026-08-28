# Fix the red CI runs: window-callback leak, macOS reallocarray, CI test-seeding watchdog, sanitizer-gate blind spot

Date: 2026-08-28
Status: awaiting human approval
Context: latest `sanitizers.yml` run <https://github.com/sayyiditow/shard-db/actions/runs/33142405031> (red) and `ci.yml` run <https://github.com/sayyiditow/shard-db/actions/runs/33142405088> (red: macOS compile, Linux x86 watchdog). All findings below were re-verified against the working tree by reading the code; the previous agent's diagnosis was confirmed with two corrections (noted inline).

Standing rules for execution: build with `SKIP_TESTS=1 ./build.sh`; test with `./build/bin/shard-db-test run[-all]`. Leave all work **uncommitted**. If any quoted anchor is not found exactly, write the mismatch to `PLAN_NOTES.md` and halt the entire run.

---

## Findings and root causes (all verified in the tree)

### F1 — Every successful indexed bulk window leaks its staged index state (the ASan 12,504-byte leak)

**Root cause, specific mechanism.** The 2026-08-23 commit_done split (PLAN_NOTES) moved all release of `prepare_window`-staged state out of `v2_bulk_ins_apply_window` into two new hooks: `v2_bulk_ins_commit_done` and `v2_bulk_ins_abort_window` (query_bulk.c), wired into `SlotcaskBulkOpts` at the `slotcask_bulk_upsert_in_kfshard` call site. But the window coordinator in slotcask.c **never invokes either hook** — `grep -n "commit_done\|abort_window(" src/db/slotcask.c` finds only the adapter *definitions* (`upsert_adapter_abort_window`, `delete_adapter_abort_window`) and wiring assignments, zero call sites. `bulk_commit_one_kf_window`'s single exit (`out:`) frees only coordinator-owned state via `bulk_window_plan_destroy`. Therefore every record staged by `v2_bulk_ins_prepare_window` — the `BitmapPrepareWindow` ops/entries arrays (CI's 12,336 + 168 bytes), `bw_bufs`, `tg_ops`, `bt_del_ops`, and the `idx_pairs[fi][k].value` buffers — leaks on every window that reaches C, and also on every pre-M failure (nothing calls abort either).

Leak is by-design invisible to assertion counts: the daemon exits nonzero under LSan at process end, but the case's assertions already passed; only the workflow's daemon-log grep backstop catches it — and that step is skipped when the suite step fails (F4).

**Correction vs. the previous agent's writeup:** it claimed the post-M unresolved path "may need a neutral release_window callback" as an open question. It does, and the design is safe — verified: the gate/startup replay is marker-driven and self-contained. `kf_batch_marker_gate` reads the KFM2 marker and calls `kf_marker_replay_entry_locked` per entry, which re-derives index diffs from segment bytes via `g_recovery_index_diff_fn` (`kf_marker_apply_recovery_diff`); it never re-enters the caller's `apply_window` hooks and needs no process-local state. Only the coordinator's *in-window* replay (`bulk_replay_window_forward_locked` → `bulk_apply_and_sync_indexes_locked`) re-invokes the hooks, and that happens strictly before the unresolved exit. So releasing the caller's staging on the unresolved exit is safe: the marker owns recovery from that point.

Also verified: the single-record path (`v2_insert_prepare_commit`/`apply_commit` in storage.c) frees everything inline on apply — it needs only `abort_window` wiring, which the coordinator fix covers for free; `SlotcaskUpsertOpts` needs no new fields. The delete path's hooks stage nothing persistent (PLAN_NOTES ACID audit), so `SlotcaskBulkDeleteOpts` gets no new fields.

### F2 — macOS cannot compile: `reallocarray` in dead code

**Root cause.** slotcask.c's `idx_touch_record` grows `IdxTouchSet.v` with `reallocarray`. It is the only `reallocarray` in `src/`. macOS declares it only under `_DARWIN_C_SOURCE`, which the build's strict feature macros disable → implicit-declaration error. The function is marked `__attribute__((unused))` and has **zero callers**: `tls_idx_touch` is installed around `apply_window`, and `plan->touch` is consumed (sort/dedupe/flush) in `bulk_apply_and_sync_indexes_locked`, but nothing ever *records* into it — the per-(field, shard) dedupe sync this machinery was meant to enable never happens; the hooks sync durably themselves (`v2_bulk_ins_apply_window`'s `btree_sync_path` loop, trigram/bitmap `sync_after`). It compiles, wastes nothing at runtime, and is harmless — but it must not break the macOS build.

### F3 — Linux x86 CI watchdog: 2×2000 individually-durable test inserts

**Root cause.** `test-planner-sort-vs-walk` and `test-cursor-fetch-sort` (same file) each seed 2000 rows with individual `insert` requests. Since the durability-window merge, every indexed insert runs the full M→A→I→K→T→C fsync sequence. On the shared GitHub x86 runner this exceeded the runner's default 180 s per-case watchdog (`SHARD_TEST_WATCHDOG_SEC`, test_runner.c) before the first assertion; locally it passes in <1 s. Storage-latency sensitivity of the *fixture*, not an engine bug (the analysis's ARM-green/local-green evidence supports this).

**Fix shape: shrink the fixture, keep the planner boundary.** `prefer_fetch_sort` (query_plan.c) decides `sort` iff `candidates² < (offset+limit) × N` (`want = 0 + 25 = 25`). With N = 205 (5 rare, 200 common): rare `5² = 25 < 25 × 205 = 5125` → sort; common `200² = 40000 ≥ 5125` → walk. Both assertions hold with ≥8× margin. The cursor test only asserts on the rare set (3 + 2 rows) — unaffected by the common count. One `bulk-insert` request per case (wire shape `{"mode":"bulk-insert",...,"records":[{"key":...,"value":...},...]}`, response `{"inserted":N,...}`) replaces 2000 fsync sequences with ~1 window per kf shard (~8).

### F4 — Sanitizer-gate blind spot: the log-scan backstop never ran

**Root cause.** In `sanitizers.yml` the "Fail if any sanitizer findings escaped" step has no `if: always()`, so when the suite step fails (F-id: test-rebuild-recovery assertions), the grep over daemon logs — the only thing that would have caught the F1 leak — is skipped. `tsan.yml` has the identical defect. Second gap: both steps grep only `/tmp/shard-db_*/logs/` (daemon logs); embedded-harness findings print to the runner log (documented in PLAN_NOTES), which is not captured anywhere.

### F5 — test-rebuild-recovery's 5 red assertions (not a rebuild bug)

**Root cause.** The case's client uses `io_timeout_ms = 30000`; the `vacuum ... "splits":16` rebuild request returned NULL (client timeout) on the loaded 2-job ASan runner, leaving `.rebuild_txn.active` behind and cascading through the artifact assertions. Not reproducible locally (isolated + full `--jobs 2` both green — 12,591 assertions). Same shape as the already-precedented `test-auto-reshard` fix (90 s → 600 s). Fix the client budget; do not chase a phantom rebuild bug.

---

## Design decisions

1. **Three release routes, exactly one fires per staged window.** `commit_done` (success, durable), `abort_window` (pre-M failure, nothing durable published, staged mutations must never run), and a **new** `release_window` (post-M unresolved/EINPROGRESS: marker owns recovery, gate/startup replay re-derives from disk, so process-local staging must go *now* even though durability never confirmed). Keeping a distinct name matters: firing `commit_done` on an unresolved window would lie about durability, and firing `abort_window` would violate its "marker never became durable" contract. All three share the identical release body in query_bulk.c today — the distinction is contractual, and this repo's hook contracts are load-bearing documentation.
2. **Exactly-once by construction, not by bookkeeping.** `bulk_commit_one_kf_window` has a single exit; two local flags (`plan.hooks_staged`, `published`) are enough to route. No tri-state enum on the plan struct.
3. **Do not** raise the 180 s watchdog, exclude the two planner cases, weaken durability, or add a fast-durability test mode.
4. **Do not** wire or remove the `IdxTouchSet` mechanism in this plan. It is dead but harmless; removal touches the I-step for zero behavioral gain. Flagged as a candidate follow-up pre-plan.
5. **Route observation is a direct in-process test, not cross-process counters.** The `test-win-*` durability cases already run the coordinator in-process and arm the `shard_test_ctl.h` knobs directly (verified: `test_durability_ordering.c` sets `g_shard_test_fail_phase` inline). Task 1f uses the same pattern with recording hooks — no new instrumentation surface, and every routing outcome is asserted deterministically, base-red.

---

## Task 1 — Coordinator invokes exactly one window-release hook (fixes F1)

**Test-first (base-red proof), with a hard ordering constraint.** The Task 1f test wires `.release_window`, which does not exist until 1a lands — so the order inside Task 1 is:

1. **Land 1a only** (header field + contract comment — non-behavioral: the new field is NULL in every existing initializer, and nothing reads it yet). Build must stay green.
2. Write the Task 1f test file + build.sh registration, and run `./build/bin/shard-db-test run test-window-release-routes` on this tree — it now **compiles** (field exists) but every route assertion **fails** (the coordinator still routes nothing). Paste the red output.
3. Land 1b–1g (coordinator routing, query_bulk wiring, adapter enforcement). Re-run: green. Paste.

Second leg (leak repro): build asan (`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`), run `./build/bin/shard-db-test run test-durability-bulk-window-boundary` bare (any case doing a successful indexed bulk window; the previous agent reproduced the exact CI leak in 3.3 s with one of these). Confirm the daemon log under `/tmp/shard-db_*/logs/` contains a LeakSanitizer report of 12,504 bytes in 2 allocations and the command exits nonzero. Paste. Re-run after 1b–1g and paste the clean output.

### 1a. `src/db/slotcask.h` — add `release_window` to `SlotcaskBulkOpts`

Anchor: the field pair at the tail of `SlotcaskBulkOpts`:

```c
    slotcask_bulk_commit_done_fn    commit_done;
    slotcask_bulk_abort_window_fn   abort_window;
```

Replace with:

```c
    slotcask_bulk_commit_done_fn    commit_done;
    /* Fires at most once, on the post-M UNRESOLVED exit: the window's marker
       is (or may be) durable but the coordinator's in-process forward replay
       failed to converge, so the outcome is pending (EINPROGRESS). Durable
       recovery is owned by the on-disk marker from here on —
       kf_shard_marker_gate and startup replay re-derive every index mutation
       from marker + segment bytes and never re-enter this process's hooks —
       so the caller's process-local staging must be released now, exactly as
       commit_done would, without claiming the window reached durability.
       Distinct from abort_window: that name is contractually reserved for
       the pre-M case where no durable evidence exists. Optional; NULL is
       fine for hooks that stage nothing. */
    slotcask_bulk_commit_done_fn    release_window;
    slotcask_bulk_abort_window_fn   abort_window;
```

Also extend the hook-contract comment block above the typedefs. Anchor is the `abort_window —` bullet ending in `Optional; NULL is fine for hooks that stage nothing durable-adjacent in prepare_window. */` — insert before that final sentence a new bullet:

```c
 * Exactly one of {commit_done, abort_window, release_window} fires per
 * window whose prepare_window ran, after the coordinator's last possible
 * re-entry into apply_window for that window: commit_done on success
 * (direct or via forward replay), abort_window whenever no durable marker
 * evidence exists (a pre-M publication failure, or a window that planned
 * zero active records so M was skipped entirely — such a window was never
 * committed, even though the batch call may report success), release_window
 * on a post-M unresolved (EINPROGRESS) exit. A window that planned zero
 * active records routes to abort_window, never commit_done.
 *
 * A non-zero return from prepare_window is self-cleaning: the hook must
 * have fully released everything it staged before returning non-zero (and
 * after rejecting every active record). The coordinator fires no release
 * route after a failed prepare_window.
 *
```

### 1b. `src/db/slotcask.c` — `BulkWindowPlan` gains `hooks_staged`

Anchor, tail of `BulkWindowPlan`:

```c
    KfInsertPlan      *abandoned;   /* reserved-but-rejected NEW-insert slots */
    size_t             nabandoned;
} BulkWindowPlan;
```

Replace with:

```c
    KfInsertPlan      *abandoned;   /* reserved-but-rejected NEW-insert slots */
    size_t             nabandoned;
    /* prepare_window ran OK: the caller's bulk_hook_ctx now owns staged
       state that exactly one of {commit_done, abort_window, release_window}
       must release at this window's exit (see bulk_commit_one_kf_window). */
    int                hooks_staged;
} BulkWindowPlan;
```

(`bulk_window_plan_destroy` memsets the struct, so this needs no destroy change.)

### 1c. `src/db/slotcask.c` — set the flag after each successful `prepare_window`

Anchor 1 (upsert), inside `bulk_plan_window_locked`:

```c
        if (uo->prepare_window(recs, plan->active, n,
                               uo->bulk_hook_ctx) != 0) goto hard_fail;
        /* hook may have set status=-1/-2 (policy); rebuild below */
```

Replace with:

```c
        if (uo->prepare_window(recs, plan->active, n,
                               uo->bulk_hook_ctx) != 0) goto hard_fail;
        plan->hooks_staged = 1;
        /* hook may have set status=-1/-2 (policy); rebuild below */
```

Anchor 2 (delete), immediately below:

```c
        if (txn->delete_opts->prepare_window(recs, plan->active, n,
                               txn->delete_opts->bulk_hook_ctx) != 0) goto hard_fail;
    }
```

Replace with:

```c
        if (txn->delete_opts->prepare_window(recs, plan->active, n,
                               txn->delete_opts->bulk_hook_ctx) != 0) goto hard_fail;
        plan->hooks_staged = 1;
    }
```

### 1d. `src/db/slotcask.c` — route exactly one hook at the window exit

In `bulk_commit_one_kf_window`. Anchor the declarations:

```c
    SlotcaskKfHandle kh;
    BulkWindowPlan plan = {0};
    int prc, rc = -1;
```

Replace with:

```c
    SlotcaskKfHandle kh;
    BulkWindowPlan plan = {0};
    int prc, rc = -1;
    int published = 0;   /* a marker file was actually created (M reached
                            with nactive > 0): durable evidence may exist */
```

Anchor the publish step:

```c
    prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
    if (prc < 0) goto out;        /* pre-M: nothing was published */
    if (prc > 0) goto replay;     /* published, durability unconfirmed */
```

Replace with:

```c
    prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
    /* publish returns 0 WITHOUT creating any marker when the window
       planned zero active records (all records policy-rejected) — that is
       not a commit point. Only nactive > 0 with prc >= 0 means durable
       marker evidence exists (prc > 0: created but durability
       unconfirmed). */
    if (prc >= 0 && plan.nactive > 0) published = 1;
    if (prc < 0) goto out;        /* pre-M: nothing was published */
    if (prc > 0) goto replay;     /* published, durability unconfirmed */
```

Anchor the exit block:

```c
out:
    bulk_window_plan_destroy(&plan);
    kfcache_release(&kh);
```

Replace with:

```c
out:
    /* Exactly one release route per staged window. No marker evidence →
       abort_window (publish failed pre-M, or the window planned zero
       active records and M was skipped: nothing was ever committed, even
       when the batch rc is 0; v2's prepare already self-released the
       all-rejected case, so this is an idempotent no-op there). Marker
       evidence + rc==0 → commit_done (durable; no further hook re-entry).
       Marker evidence + failure → release_window (the marker owns
       recovery; gate and startup replay re-derive from disk and never
       re-enter these hooks). Pointers stay const: BulkMutationShard
       members are const-qualified. All routes NULL-guarded. */
    if (plan.hooks_staged) {
        const SlotcaskBulkOpts *uo = shard->upsert_opts;
        const SlotcaskBulkDeleteOpts *dopt = shard->delete_opts;
        if (!published) {
            if (uo && uo->abort_window)
                uo->abort_window(uo->bulk_hook_ctx);
            else if (dopt && dopt->abort_window)
                dopt->abort_window(dopt->bulk_hook_ctx);
        } else if (rc == 0) {
            if (uo && uo->commit_done)
                uo->commit_done(uo->bulk_hook_ctx);
        } else {
            if (uo && uo->release_window)
                uo->release_window(uo->bulk_hook_ctx);
        }
    }
    bulk_window_plan_destroy(&plan);
    kfcache_release(&kh);
```

Executor note: `SlotcaskBulkDeleteOpts` has no `commit_done`/`release_window` members (deliberate — its hooks stage nothing persistent); only `abort_window` is routed for delete. Verify the delete struct's ctx member is `bulk_hook_ctx` at its definition before compiling.

### 1e. `src/db/query_bulk.c` — implement + wire `v2_bulk_ins_release_window`

Anchor, after `v2_bulk_ins_commit_done`:

```c
static void v2_bulk_ins_commit_done(void *ctx) {
    v2_bulk_ins_window_release((BulkInsShardWork *)ctx);
}
```

Append immediately after:

```c
/* Post-M unresolved exit (EINPROGRESS): identical release body to
   commit_done/abort_window. The retained marker now owns durable recovery
   and gate/startup replay re-derives every mutation from disk, so the
   staged state must be released now even though durability never
   confirmed. Kept as a distinct name so the SlotcaskBulkOpts.release_window
   wiring reads honestly. */
static void v2_bulk_ins_release_window(void *ctx) {
    v2_bulk_ins_window_release((BulkInsShardWork *)ctx);
}
```

Anchor, inside the `SlotcaskBulkOpts opts = {` literal:

```c
        .commit_done          = sw->nidx > 0 ? v2_bulk_ins_commit_done  : NULL,
```

Replace with:

```c
        .commit_done          = sw->nidx > 0 ? v2_bulk_ins_commit_done  : NULL,
        .release_window       = sw->nidx > 0 ? v2_bulk_ins_release_window : NULL,
```

### Call-site inventory for the changed struct (per CORE-PROCESS)

`SlotcaskBulkOpts` is value-initialized at: slotcask.c `SlotcaskBulkOpts bopts = {0};` (×3 — single upsert adapters) and query_bulk.c `SlotcaskBulkOpts opts = {` (×4 — bulk insert, and the three bulk-update variants). All use `= {0}` or designated initializers, so `release_window` defaults to NULL everywhere except 1e; the single routing site NULL-guards. `SlotcaskBulkDeleteOpts` is untouched. No wire/protocol/disk-format change.

### Edge cases and invariants

- Success via the in-window replay label lands at `out:` with `rc == 0` and `published == 1` → `commit_done` fires exactly once even though `apply_window` ran twice.
- `prc > 0` (published-but-unconfirmed) then failed replay → `published == 1`, `rc == -2` → `release_window`. The marker stays on disk; a later write's `kf_shard_marker_gate` converges from marker bytes alone (verified self-contained), so releasing the caller's staging is safe.
- **Zero-active windows** (`plan.nactive == 0`) — two ways to arrive, one routing: (a) prepare_window early-returned without staging (`sw->nidx == 0 || nactive == 0`), or (b) it staged, then rejected every record (per-record unwinding; `v2_bulk_ins_prepare_window` self-releases when `nsurvive == 0`). Both reach `bulk_publish_window_marker_locked`, which returns 0 *without creating a marker* (verified: its `nactive == 0` early return precedes any serialization) → `published == 0` → the **abort_window** route fires even though the batch rc is 0. A window that published nothing was never committed. The release is an idempotent no-op in both cases: never-staged state frees only NULL pointers/zero counts (`bitmap_prepare_window_free` on a zeroed struct included), and case (b) already self-released.
- **Failed prepare_window** (`goto hard_fail` from inside `bulk_plan_window_locked`): the hook self-cleaned per the new contract rule, `hooks_staged == 0` → no route hook fires. Verified: `v2_bulk_ins_prepare_window` calls `v2_bulk_ins_window_release` on every internal failure path and before returning -1. For the single-record adapter path, 1g makes the adapter itself enforce the contract via `abort_commit`.
- Gate-refused entry (`kf_shard_marker_gate` fails before planning) → `hooks_staged == 0` → no hook fires, nothing staged yet.
- `plan_window` hard-fails *after* `prepare_window` succeeded but before publish (e.g. marker composition) → `hooks_staged == 1`, `published == 0`, `rc == -1` → `abort_window`. (This path leaked before this fix too.)
- A window whose `prepare_window` early-returned without staging (`sw->nidx == 0 || nactive == 0`) still sets `hooks_staged`; the release must be a harmless no-op on never-staged state. Verify before compiling: `v2_bulk_ins_window_release` only frees NULL pointers / zero counts in that state, including `bitmap_prepare_window_free` on a zeroed struct.
- Release-then-reuse: `v2_bulk_ins_window_release` zeroes every pointer/count it frees, so the worker's post-join accounting and any later touch see empty state — no double free, no UAF.
- Single-record path: `slotcask_upsert_with_hooks` wires `abort_window` only; on pre-M failure it now actually fires (frees `v2_insert_*` staged bitmap/diff state) — behavior fix, no new fields needed. On success nothing needs releasing (apply frees inline).

### 1f. New regression test: `src/test/cases/test_window_release_routes.c`

**Prerequisite: 1a must already be in the tree** — the test wires `.release_window`, which only exists after the header field lands. See the ordering in Task 1's test-first paragraph.

Direct, in-process, no cross-process counters: the test drives `slotcask_bulk_upsert_in_kfshard` with its own recording hooks and arms the `shard_test_ctl.h` knobs directly (same process — the exact pattern the `test-win-*` cases in `test_durability_ordering.c` use). It pins all six routing outcomes:

- success → `commits == 1`, `aborts == releases == 0`, staged freed;
- pre-M publish failure → `aborts == 1`, no marker on disk;
- post-M unresolved (sticky K failure defeats inline replay) → `releases == 1`, marker retained, and a later clean write gate-replays and converges with `commits == 1`;
- failed prepare → self-cleanup, **no** route hook fires;
- all-records-rejected (M skipped) → abort route despite rc 0;
- single-record adapter (1g): failing `prepare_commit` → exactly one `abort_commit`, staged freed, rc -1.

**Base-red:** after 1a (compile-able) but before 1b–1g, no route ever fires — every route assertion fails and `staged` leaks (LSan fails the case under `BUILD_MODE=asan` too). Run `./build/bin/shard-db-test run test-window-release-routes` at that point (expect failures on the route assertions; paste) and again after 1b–1g (green; paste).

Create the file with this complete content:

```c
/* Regression: the window coordinator must fire exactly one of
   {commit_done, abort_window, release_window} per staged window, routing
   by on-disk truth (marker evidence), not by batch rc alone. Base-red:
   without the routing no hook ever fires, so every route assertion below
   fails and the staged allocation leaks (LSan fails the case under
   BUILD_MODE=asan as well). */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"
#include "fixtures.h"
#include "shard_test_ctl.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void compute_hash_raw(const char *key, size_t key_len,
                             uint8_t hash_out[16]);

static void rt_cleanup_dir(const char *dir) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", dir);
    system(cmd);
}

typedef struct {
    SlotcaskDb db;
    char base[PATH_MAX];
} RouteDb;

static int route_db_open(RouteDb *w) {
    slotcask_init(64, 64);
    char b[] = "/tmp/shard-db-route-test-XXXXXX";
    if (!mkdtemp(b)) return -1;
    snprintf(w->base, sizeof(w->base), "%s", b);
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", w->base);
    snprintf(k, sizeof(k), "%s/data/kf", w->base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return -1;
    memset(&w->db, 0, sizeof(w->db));
    if (slotcask_open(&w->db, w->base, 8, 1, 64) != 0) return -1;
    w->db.bulk_commit_window = 0;
    shard_test_ctl_reset();
    return 0;
}

static void route_db_close(RouteDb *w) {
    slotcask_close(&w->db);
    rt_cleanup_dir(w->base);
    slotcask_shutdown();
    shard_test_ctl_reset();
}

static int rt_marker_scan(const char *base) {
    char kdir[PATH_MAX];
    snprintf(kdir, sizeof(kdir), "%s/data/kf", base);
    DIR *d = opendir(kdir);
    if (!d) return -1;
    int n = 0;
    struct dirent *e;
    while ((e = readdir(d)) != NULL) {
        size_t L = strlen(e->d_name);
        if (L > 11 && strcmp(e->d_name + L - 11, "_marker.dat") == 0) n++;
    }
    closedir(d);
    return n;
}

typedef struct {
    int    prepares, prepare_fails;
    int    applies;
    int    commits, aborts, releases;
    int    fail_next;   /* arm: next prepare stages then fails (self-clean) */
    int    reject_all;  /* arm: prepare succeeds but rejects every record */
    void  *staged;      /* non-NULL between prepare and its route hook */
} RouteCounts;

static int rt_prepare(SlotcaskBulkRec *recs, const size_t *active,
                      size_t nactive, void *ctx) {
    RouteCounts *c = ctx;
    (void)recs; (void)active;
    if (c->fail_next) {
        /* Stage, then self-clean — the contract obliges a failed prepare
           to release what it staged before returning non-zero, and Route 4
           asserts staged==NULL afterward so the test actually proves it. */
        c->staged = malloc(32);
        free(c->staged);
        c->staged = NULL;
        c->prepare_fails++;
        return -1;
    }
    c->staged = malloc(32);           /* only a route hook may free this */
    if (!c->staged) return -1;
    c->prepares++;
    if (c->reject_all && nactive > 0)
        recs[active[0]].status = -2;  /* policy rejection by the hook */
    return 0;
}

static int rt_apply(SlotcaskBulkRec *recs, const size_t *active,
                    size_t nactive, void *ctx) {
    RouteCounts *c = ctx;
    (void)recs; (void)active; (void)nactive;
    c->applies++;
    return 0;
}

static void rt_commit(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->commits++;
}
static void rt_abort(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->aborts++;
}
static void rt_release(void *ctx) {
    RouteCounts *c = ctx;
    free(c->staged); c->staged = NULL;
    c->releases++;
}

/* Single-record (SlotcaskUpsertOpts) shapes for Route 6. */
static int rt_single_prepare_fail(const uint8_t *new_value, size_t new_vlen,
                                  uint32_t planned_kf_slot, void *ctx) {
    RouteCounts *c = ctx;
    (void)new_value; (void)new_vlen; (void)planned_kf_slot;
    c->staged = malloc(32);        /* staged, deliberately left behind:
                                      the adapter's abort_commit (Task 1g)
                                      is the cleanup route for this */
    if (!c->staged) return -1;
    c->prepares++;
    return -1;
}
static int rt_single_apply_noop(const uint8_t *new_value, size_t new_vlen,
                                uint32_t planned_kf_slot, void *ctx) {
    RouteCounts *c = ctx;
    (void)new_value; (void)new_vlen; (void)planned_kf_slot;
    c->applies++;
    return 0;
}

/* Drive one 1-record window through the bulk primitive with the recording
   hooks. Returns the public rc (bulk_finish_status folds shard rc to
   0 / -1, with errno==EINPROGRESS when any shard was pending). */
static int rt_drive_one(RouteDb *w, uint64_t key, RouteCounts *c, int shard) {
    char v[8] = "route";
    SlotcaskBulkRec rec;
    memset(&rec, 0, sizeof(rec));
    rec.key = &key; rec.klen = sizeof(key);
    rec.value = v;  rec.vlen = 5;
    SlotcaskBulkOpts o;
    memset(&o, 0, sizeof(o));
    o.has_indexed_fields = 1;
    o.prepare_window = rt_prepare;
    o.apply_window   = rt_apply;
    o.commit_done    = rt_commit;
    o.release_window = rt_release;
    o.abort_window   = rt_abort;
    o.bulk_hook_ctx  = c;
    return slotcask_bulk_upsert_in_kfshard(&w->db, shard, &rec, 1, &o);
}

static int test_window_release_routes(void) {
    RouteDb w;
    ASSERT_EQ_INT(route_db_open(&w), 0, "open route-test db");
    if (t_ctx->failed) return 1;

    uint64_t key = 4242;
    uint8_t h[16];
    compute_hash_raw((const char *)&key, sizeof(key), h);
    int shard = compute_record_shard(h, 8);

    /* Route 1 — success: exactly one commit_done. */
    RouteCounts c; memset(&c, 0, sizeof(c));
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0, "success window rc");
    ASSERT_EQ_INT(c.prepares, 1, "success: one prepare");
    ASSERT_EQ_INT(c.commits, 1, "success: commit_done exactly once");
    ASSERT_EQ_INT(c.aborts, 0, "success: no abort");
    ASSERT_EQ_INT(c.releases, 0, "success: no release");
    ASSERT_TRUE(c.staged == NULL, "success: staged state released");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "success: no marker left");

    /* Route 2 — pre-M publish failure: abort route, no marker on disk.
       shard_test_ctl_reset() first: the sync counters are process-global
       and cumulative — Route 1's window already counted an M barrier, so
       without a reset the armed occurrence would never match. */
    memset(&c, 0, sizeof(c));
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_M;
    g_shard_test_fail_occurrence = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "pre-M failure rc");
    ASSERT_TRUE(errno != EINPROGRESS, "pre-M failure is not pending");
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    ASSERT_EQ_INT(c.aborts, 1, "pre-M: abort_window fired");
    ASSERT_EQ_INT(c.commits, 0, "pre-M: no commit_done");
    ASSERT_EQ_INT(c.releases, 0, "pre-M: no release");
    ASSERT_TRUE(c.staged == NULL, "pre-M: staged released by abort");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "pre-M: no marker file");

    /* Route 3 — post-M unresolved: a sticky K failure defeats the
       coordinator's own inline replay, so the outcome is genuinely
       pending. release_window fires, the marker stays, and a later clean
       write must gate-replay it and route commit_done normally. Reset
       first — same cumulative-counter reason as Route 2. */
    memset(&c, 0, sizeof(c));
    shard_test_ctl_reset();
    g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
    g_shard_test_fail_occurrence = 1;
    g_shard_test_fail_sticky = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "unresolved rc (-1 folded)");
    ASSERT_EQ_INT(errno, EINPROGRESS, "unresolved errno is EINPROGRESS");
    g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
    g_shard_test_fail_sticky = 0;
    ASSERT_EQ_INT(c.releases, 1, "unresolved: release_window fired");
    ASSERT_EQ_INT(c.commits, 0, "unresolved: no commit_done");
    ASSERT_EQ_INT(c.aborts, 0, "unresolved: no abort");
    ASSERT_TRUE(c.staged == NULL, "unresolved: staged released");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 1, "unresolved: marker retained");
    memset(&c, 0, sizeof(c));
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0,
                  "post-unresolved retry converges");
    ASSERT_EQ_INT(c.commits, 1, "retry: commit_done exactly once");
    ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "retry: marker cleared");

    /* Route 4 — failed prepare self-cleans; NO route hook fires. */
    memset(&c, 0, sizeof(c));
    c.fail_next = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), -1, "failed prepare rc");
    ASSERT_EQ_INT(c.aborts + c.commits + c.releases, 0,
                  "failed prepare: no route hook");
    ASSERT_EQ_INT(c.prepare_fails, 1, "failed prepare: hook ran");
    ASSERT_TRUE(c.staged == NULL, "failed prepare: self-cleaned");

    /* Route 5 — all records policy-rejected: nactive==0, M skipped, no
       marker exists — abort route even though the batch rc is 0. */
    memset(&c, 0, sizeof(c));
    c.reject_all = 1;
    ASSERT_EQ_INT(rt_drive_one(&w, key, &c, shard), 0, "all-rejected rc");
    ASSERT_EQ_INT(c.aborts, 1, "all-rejected: abort route (never committed)");
    ASSERT_EQ_INT(c.commits, 0, "all-rejected: no commit_done");
    ASSERT_EQ_INT(c.releases, 0, "all-rejected: no release");
    ASSERT_TRUE(c.staged == NULL, "all-rejected: staged released");

    /* Route 6 — single-record adapter: a failing prepare_commit is cleaned
       by the ADAPTER (1g runs abort_commit on the failure path), so the
       self-clean contract holds even for hooks that stage and give up.
       rc -1, exactly one abort_commit, nothing leaked. */
    memset(&c, 0, sizeof(c));
    {
        uint64_t skey = 4242;
        char sv[8] = "route";
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        so.has_indexed_fields = 1;
        so.prepare_commit = rt_single_prepare_fail;
        so.apply_commit   = rt_single_apply_noop;
        so.abort_commit   = rt_abort;       /* recorder: increments aborts */
        so.pre_commit_ctx = &c;
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, &skey, sizeof(skey),
                                                 sv, 5, &so, NULL), -1,
                      "single failed prepare rc");
    }
    ASSERT_EQ_INT(c.aborts, 1, "single: adapter ran abort_commit once");
    ASSERT_EQ_INT(c.commits + c.releases, 0, "single: no other route");
    ASSERT_TRUE(c.staged == NULL, "single: staged released");

    route_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-window-release-routes", test_window_release_routes)
```

Register it in `build.sh` — anchor:

```
    src/test/cases/test_durability_ordering.c \
```

Replace with:

```
    src/test/cases/test_durability_ordering.c \
    src/test/cases/test_window_release_routes.c \
```

Executor notes: (a) the harness mirrors `test_durability_ordering.c`'s `WinDb` fixture (`slotcask_init` → `mkdtemp` → `slotcask_open(root, 8 splits, 1 stream, 64 slot)`); if any call signature drifted, halt per the anchor rule. (b) Exact rc values follow `bulk_finish_status` (verified: any shard failure folds to -1; `errno=EINPROGRESS` is set only when a shard rc was -2). (c) The knobs are process-global but this case is single-threaded and disarms every knob (and `shard_test_ctl_reset()` on close) before returning — same discipline as the existing win-* cases.

### 1g. Enforce the failed-prepare contract in the single-record adapter

The self-clean contract (1a) binds every `prepare_window` implementation. For the single-record path that implementation is `upsert_adapter_prepare_window`, which today just returns the inner `prepare_commit` result — it does not ensure cleanup, so the contract holds only for inner hooks that happen to self-clean. Fix: the adapter runs `abort_commit` — the release path designed for exactly "prepare staged, apply never ran" — on the failure path, making the outer hook self-cleaning for *every* `SlotcaskUpsertOpts` consumer.

**Safety, verified:** if the inner hook already cleaned itself up (as `v2_insert_prepare_commit`'s failure branches do today), the adapter's `abort_commit` re-runs the same releases. Both primitives are free+zero idempotent — `bitmap_prepare_set_free` sets `entries = NULL; cap = 0` after freeing, and `v2_insert_bm_owned_free` zeroes `n_bm_owned` after its loop — so the second pass is a no-op. The delete adapter is left untouched: its hooks stage nothing persistent (PLAN_NOTES ACID audit), so there is nothing to enforce.

Anchor in slotcask.c:

```c
static int upsert_adapter_prepare_window(SlotcaskBulkRec *recs,
                                         const size_t *active, size_t nactive,
                                         void *ctx) {
    if (nactive == 0) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (!uo->prepare_commit) return 0;
    return uo->prepare_commit(rec->value, rec->vlen, rec->kf_slot,
                              uo->pre_commit_ctx) != 0 ? -1 : 0;
}
```

Replace with:

```c
static int upsert_adapter_prepare_window(SlotcaskBulkRec *recs,
                                         const size_t *active, size_t nactive,
                                         void *ctx) {
    if (nactive == 0) return 0;
    SlotcaskBulkRec *rec = &recs[active[0]];
    UpsertAdapterCtx *actx = (UpsertAdapterCtx *)ctx;
    const SlotcaskUpsertOpts *uo = actx->uo;
    if (!uo->prepare_commit) return 0;
    if (uo->prepare_commit(rec->value, rec->vlen, rec->kf_slot,
                           uo->pre_commit_ctx) != 0) {
        /* Self-clean contract (slotcask.h): a failed prepare_window must
           release its staging before returning non-zero. Inner two-phase
           hooks express that release as abort_commit, and the primitive
           frees are idempotent (free+zero), so running it here is safe
           even when prepare_commit already cleaned itself up. The
           coordinator fires no route for this window — hooks_staged is
           only set after a successful prepare. */
        if (uo->abort_commit) uo->abort_commit(uo->pre_commit_ctx);
        return -1;
    }
    return 0;
}
```

Route 6 of the 1f test pins this: a `prepare_commit` that stages and fails yields rc -1 with exactly one `abort_commit` and `staged == NULL`.

---

## Task 2 — Portable allocation in `idx_touch_record` (fixes F2)

**Test-first.** No local macOS target: the local proof is `grep -rn reallocarray src/` going from the single F2 hit to zero; the authoritative confirmation is the next `ci.yml` macOS leg going green.

Anchor in slotcask.c:

```c
    if (s->n == s->cap) {
        size_t ncap = s->cap ? s->cap * 2 : 16;
        IdxTouch *nv = reallocarray(s->v, ncap, sizeof(*nv));
        if (!nv) return;
        s->v = nv; s->cap = ncap;
    }
```

Replace with:

```c
    if (s->n == s->cap) {
        IdxTouch *nv;
        /* Guard BEFORE doubling: s->cap * 2 itself must not overflow. */
        if (s->cap > SIZE_MAX / (2 * sizeof(IdxTouch))) return;
        size_t ncap = s->cap ? s->cap * 2 : 16;
        nv = realloc(s->v, ncap * sizeof(IdxTouch));
        if (!nv) return;
        s->v = nv; s->cap = ncap;
    }
```

Overflow-equivalent to `reallocarray` (checked multiply): after the guard, `ncap = cap*2 ≤ SIZE_MAX/sizeof(IdxTouch)`, so `ncap * sizeof(IdxTouch)` cannot wrap, and the guard runs before the doubling so `cap * 2` cannot overflow either. Do not wire up or delete the IdxTouchSet mechanism in this task (decision 4 above).

---

## Task 3 — Planner test seeding: 205 rows via one bulk-insert (fixes F3)

**Test-first.** The 180 s watchdog is CI-runner-specific (locally the case passes in <1 s, so it cannot be reproduced base-red on this workstation — state this honestly in the execution notes rather than faking a repro). The functional contract that must keep passing: all four planner/cursor assertions in the file, plus the new bulk-inserted count assertion. Run `./build/bin/shard-db-test run test-planner-sort-vs-walk` and `run test-cursor-fetch-sort` before (green, slow-ish) and after (green, fast) — paste both timings. Then verify locally that the seeding request count collapsed: `SHARD_TEST_WATCHDOG_SEC=5 ./build/bin/shard-db-test run test-planner-sort-vs-walk` must pass after the change (it exercises the watchdog ceiling the CI runner effectively hits).

In `src/test/cases/test_planner_sort_vs_walk.c`, add a shared seeder above `test_planner_sort_vs_walk` (anchor: `static int test_planner_sort_vs_walk(void) {`). Both cases keep their own schema field names, so the seeder takes them as parameters:

```c
/* Seed N records (5 rare, N-5 common) in ONE bulk-insert request. Each
   individual insert is now a fully durable M→A→I→K→T→C window, so 2000
   single-row seeds blew the CI runner's 180 s watchdog before the first
   assertion. One bulk request produces ~one window per kf shard and keeps
   the planner boundary: with N=205 and limit 25, prefer_fetch_sort picks
   sort for the rare set (5² < 25×205) and walk for the common set
   (200² ≥ 25×205). num_field is the integer ordering field's name in the
   case's schema (t / score). */
static int seed_sort_walk_fixture(TestClient *tc, const char *dir,
                                  const char *obj, const char *str_field,
                                  const char *num_field, int n,
                                  const char *what) {
    char *req = malloc(65536); char *resp = NULL;
    ASSERT_NOT_NULL(req, "seed alloc");
    size_t off = (size_t)snprintf(req, 65536,
        "{\"mode\":\"bulk-insert\",\"dir\":\"%s\",\"object\":\"%s\",\"records\":[",
        dir, obj);
    for (int i = 0; i < n; i++) {
        const char *cat = (i < 5) ? "rare" : "common";
        off += (size_t)snprintf(req + off, 65536 - off,
            "%s{\"key\":\"k%04d\",\"value\":{\"%s\":\"%s\",\"%s\":%d}}",
            i ? "," : "", i, str_field, cat, num_field, i);
    }
    snprintf(req + off, 65536 - off, "]}");
    tc_request(tc, req, &resp);
    free(req);
    char expect[64];
    snprintf(expect, sizeof(expect), "\"inserted\":%d", n);
    ASSERT_CONTAINS(resp, expect, what);
    free(resp);
    return 0;
}
```

In `test_planner_sort_vs_walk`, anchor the seeding loop:

```c
    for (int i = 0; i < 2000; i++) {
        char req[256];
        /* 5 rare rows (k≈5), 1995 common rows */
        const char *cat = (i < 5) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"sw\",\"object\":\"swobj\",\"key\":\"k%04d\","
            "\"value\":{\"cat\":\"%s\",\"t\":%d}}", i, cat, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
```

Replace with:

```c
    if (seed_sort_walk_fixture(tc, "sw", "swobj", "cat", "t", 205,
                               "bulk seed 205 (5 rare)")) return 1;
```

In `test_cursor_fetch_sort`, anchor:

```c
    /* 5 "rare" rows (k≈5), 1995 "common" rows — sparse in score. */
    for (int i = 0; i < 2000; i++) {
        char req[256];
        const char *kind = (i < 5) ? "rare" : "common";
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"cf\",\"object\":\"cfobj\",\"key\":\"k%04d\","
            "\"value\":{\"kind\":\"%s\",\"score\":%d}}", i, kind, i);
        tc_request(tc, req, &resp); free(resp); resp=NULL;
    }
```

Replace with:

```c
    /* 5 "rare" rows (k≈5), 200 "common" rows — sparse in score. One bulk
       request; see seed_sort_walk_fixture for the watchdog rationale. */
    if (seed_sort_walk_fixture(tc, "cf", "cfobj", "kind", "score", 205,
                               "bulk seed 205 (5 rare)")) return 1;
```

Do not touch any assertion below the seeding loops; page-2 expectations (3 rare rows then 2) are unaffected. The 64 KB request buffer comfortably holds 205 records (~90 bytes each).

---

## Task 4 — Sanitizer workflow backstop actually runs (fixes F4)

**Test-first.** No local runner equivalent; verification is (a) the mechanism argument below, (b) the next CI run: the scan step must appear in the run's step list even when the suite step is red. Locally, validate the grep set against a synthetic report file (create `/tmp/fake-report` containing `SUMMARY: AddressSanitizer: 12504 byte(s) leaked`, run the same `grep -Erq ... /tmp/fake-report`, expect exit 0/match).

### `.github/workflows/sanitizers.yml`

Anchor:

```yaml
      - name: Run full C test suite under ASan + UBSan
        # --jobs pinned low: ASan/UBSan instrumentation is CPU/memory
        # heavy per test case, and this runner is shared/weak — full-nproc
        # default parallelism caused contention-driven request timeouts.
        run: ./build/bin/shard-db-test run-all --exclude "$SHARD_TEST_EXCLUDE" --jobs 2
```

Replace with:

```yaml
      - name: Run full C test suite under ASan + UBSan
        # --jobs pinned low: ASan/UBSan instrumentation is CPU/memory
        # heavy per test case, and this runner is shared/weak — full-nproc
        # default parallelism caused contention-driven request timeouts.
        # tee: embedded-harness findings print to the runner log, not the
        # daemon log dir — capture them for the scan step below.
        run: |
          set -eo pipefail
          ./build/bin/shard-db-test run-all --exclude "$SHARD_TEST_EXCLUDE" --jobs 2 2>&1 | tee /tmp/suite-output.log
```

Anchor the scan step:

```yaml
      - name: Fail if any sanitizer findings escaped
        run: |
          if grep -Erq "AddressSanitizer|UndefinedBehaviorSanitizer|runtime error|SUMMARY: " /tmp/shard-db_*/logs/ 2>/dev/null; then
            echo "::error::Sanitizer findings detected in daemon logs"
            grep -Er "AddressSanitizer|UndefinedBehaviorSanitizer|runtime error|SUMMARY: " /tmp/shard-db_*/logs/ | head -50
            exit 1
          fi
          echo "No sanitizer findings."
```

Replace with:

```yaml
      - name: Fail if any sanitizer findings escaped
        # always(): the backstop must run even when the suite step above
        # failed — 2026-08-28: a LeakSanitizer report escaped exactly this
        # way (suite red on an unrelated assertion → scan skipped).
        if: always()
        run: |
          if grep -Erq "AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer|runtime error|SUMMARY: " /tmp/shard-db_*/logs/ /tmp/suite-output.log 2>/dev/null; then
            echo "::error::Sanitizer findings detected in daemon logs or runner output"
            grep -Er "AddressSanitizer|LeakSanitizer|UndefinedBehaviorSanitizer|runtime error|SUMMARY: " /tmp/shard-db_*/logs/ /tmp/suite-output.log | head -50
            exit 1
          fi
          echo "No sanitizer findings."
```

(`set -eo pipefail` keeps the suite's exit code authoritative through `tee`.)

### `.github/workflows/tsan.yml`

Same two changes: tee the suite run to `/tmp/suite-output.log` (with `set -eo pipefail`), and add `if: always()` plus `/tmp/suite-output.log` to both greps of the "Fail if any TSan findings escaped" step. Patterns stay TSan-specific (`ThreadSanitizer|WARNING:|data race|deadlock`).

---

## Task 5 — test-rebuild-recovery client budget (fixes F5)

**Root cause:** 30 s client `io_timeout_ms` on the rebuild request under 2-job ASan contention on the shared runner; not reproducible locally (isolated + full `--jobs 2` green). **Not** an engine/rebuild defect — do not modify rebuild code.

In `src/test/cases/test_rebuild_recovery.c`, anchor:

```c
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
```

Replace with:

```c
    /* 120s: the vacuum/rebuild round-trip exceeded 30s once on the shared
       CI ASan runner under --jobs 2 contention (2026-08-28 run), which
       left .rebuild_txn.active behind and cascaded through every later
       assert. Same treatment as test-auto-reshard's 90s→600s. */
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
```

Verify: `./build/bin/shard-db-test run test-rebuild-recovery` green; base-red cannot be produced locally (state that honestly).

---

## Task 6 — Docs (anchored, complete text)

### 6a. `docs/concepts/storage-model.md`

The window lifecycle is described in the paragraph beginning "Indexed writes add a durable KFM2 batch commit-intent marker". Anchor — the end of that paragraph and the start of the next:

```markdown
Marker cleanup (unlink + directory sync) is the sole cleanup step; corrupt or
unparseable marker evidence fails closed rather than being guessed at.

A prior release (2026.08.1) persisted a matching abort sidecar on post-marker
```

Replace with:

```markdown
Marker cleanup (unlink + directory sync) is the sole cleanup step; corrupt or
unparseable marker evidence fails closed rather than being guessed at.

Each window's caller-facing hooks release exactly once, chosen by on-disk
truth rather than the batch's return code: if no marker evidence exists
(pre-marker publication failure, or a window whose every record was
policy-rejected, so publication was skipped entirely) the coordinator fires
`abort_window` — the window was never committed. If the marker is durable and
the window converges, `commit_done` fires. If the marker is durable but the
in-process replay fails to converge (`EINPROGRESS`), the new `release_window`
fires: durable recovery belongs to the marker from that point, and both
`kf_shard_marker_gate` and the startup sweep re-derive every index mutation
from marker + segment bytes without ever re-entering the caller's hooks. A
failed `prepare_window` is self-cleaning and fires no release route.

A prior release (2026.08.1) persisted a matching abort sidecar on post-marker
```

(`AGENTS.md` needs no change: this plan does not touch the documented local gate invocations.)

### 6b. `docs/reference/changelog.md`

`commit_done` appears nowhere under `docs/` today (verified by grep), so this entry is the docs' first statement of the release contract. Anchor — the head of the unreleased section:

```markdown
## Unreleased

**Fixed: btree↔kfcache lock-order inversion (production deadlock).** The
```

Replace with:

```markdown
## Unreleased

**Fixed: window release hooks never fired (per-call staging leak).** The
window coordinator (`bulk_commit_one_kf_window`) never invoked the
`commit_done`/`abort_window` hooks that indexed bulk inserts wired in, so
every successful indexed bulk window leaked its staged index state (bitmap
ops, queued trigram/btree ops, idx-pair keys). The coordinator now routes
exactly one release per staged window, chosen by on-disk truth: no marker
evidence → `abort_window` (including windows whose every record was
policy-rejected — those were never committed even though the batch reports
success); marker durable + success → `commit_done`; marker durable + failed
replay → the new `release_window` (durable recovery is owned by the marker;
gate/startup replay re-derives from disk). A failed `prepare_window` remains
self-cleaning and fires no route; the single-record two-phase adapter now
runs `abort_commit` when `prepare_commit` fails, so that contract holds for
every consumer. Pinned by `test-window-release-routes`.

**Fixed: macOS build.** The (dead) `IdxTouchSet` growth path used
`reallocarray`, which macOS doesn't declare under the build's strict feature
macros; replaced with an overflow-checked `realloc`.

**Changed: CI.** The two planner/cursor test fixtures now seed 205 rows via
one bulk-insert request instead of 2,000 individually-durable inserts
(post-durability-merge, those blew the 180 s per-case watchdog on shared CI
runners); `test-rebuild-recovery`'s client budget is 120 s; the sanitizer
workflows' log-scan backstop now runs with `if: always()` and also scans the
captured runner output, so a finding can no longer escape behind an
unrelated failing step.

**Fixed: btree↔kfcache lock-order inversion (production deadlock).** The
```

---

## Verification (whole plan, per AGENTS.md)

1. `SKIP_TESTS=1 ./build.sh` — clean build, no new warnings.
2. `./build/bin/shard-db-test run-all` (native) — full suite green.
3. `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then **three** fresh `./build/bin/shard-db-test run-all` — zero findings, zero leaks (bare invocation; strictness is baked in).
4. `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then **three** fresh `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all`.
5. Base-red evidence already required by Task 1: `test-window-release-routes` red-before/green-after (paste both), plus the LeakSanitizer report before and clean run after on the focused asan case.
6. Push is the human's; CI then confirms the macOS leg (Task 2) and the hardened scan steps (Task 4).

## Explicit non-goals

- Removing/wiring the dead `IdxTouchSet` mechanism (candidate follow-up pre-plan).
- Raising `SHARD_TEST_WATCHDOG_SEC` defaults anywhere.
- Any durability weakening, test exclusion, or fast-durability mode.
- Changes to `SlotcaskBulkDeleteOpts` or the single-record two-phase hook API.
