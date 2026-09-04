# Plan: bulk-commit throughput + durability closure (2026-09-04)

## Goal

The 2026-08-28 durability-windows change (`334b78d`) removed the
`DURABILITY_SYNC_MS` background flusher and made durability synchronous per
window. Correct contract — but the implementation syncs far more often than
the design requires: per-(record, field) index fdatasyncs in every indexed
bulk update/delete, per-slot `MS_SYNC` in the K phase, trigram per-record
syncs, an all-shards btree sync loop, and 5 namespace syscalls + 3 fsyncs per
window for marker publication. The per-window dedup collector (`IdxTouch`)
was built for exactly this and left with zero call sites.

This plan converges every indexed bulk path on one rule — **mutate without
per-record syncing, collect unique touched files, sync each exactly once
before the marker is cleared** — closes two real durability gaps (bitmap
bulk-insert sync, sequence/file-op durability), makes the expensive phases
measurable, and raises `BULK_COMMIT_WINDOW` to 4096 once the per-record
costs are gone.

## Findings verified (2026-09-04 audit)

| # | Finding | Verdict |
|---|---------|---------|
| 1 | Bulk insert never syncs bitmap pages before marker clear (`bm_close` doesn't sync; disk only on cache eviction) | TRUE — durability gap |
| 2 | `kfcache_sync_slots_locked_impl` issues one `MS_SYNC` per 24-byte slot (up to ~1024/window under the shard wrlock) | TRUE |
| 3 | `make_index_diff_arg` hard-sets `sync_after=1`; all 5 call sites are bulk apply_windows → one fdatasync per changed (record, field); `IdxTouch` flush seam is dead code | TRUE |
| 4 | Trigram `sync_after=1` per record; btree loop syncs all shards of touched fields (O_CREAT materializes empty `.idx`); merge-published shards re-synced | TRUE |
| 5 | Sequence: `fopen("w")`+`fclose`, no fsync, value returned on failed write, reset unlocked. put-file: `O_TRUNC` in place, write() return ignored, no fsync. b64 put-file: fsync return ignored, no dir fsync after rename. delete-file: no dir fsync | TRUE, all sub-claims |
| 6 | Marker publish = create+write+fsync+link+unlink+dir-fsync; clear = unlink+dir-fsync. Payload spans are checksummed on replay (not dead bytes) | TRUE; payload trim deferred (see Out of scope) |
| 7 | `commit_sync_us_total` covers only the K phase + a test-only clear wrapper; M/A/I/C invisible; `commit.count` wraps whole shard calls | TRUE |

## Design decisions (surfaced, not silent)

1. **K phase = one whole-mapping `msync(MS_SYNC)`.** The window holds the
   shard's exclusive wrlock, so every dirty page of that kf mapping belongs
   to the caller (readers never dirty pages). `msync` over the full range is
   the POSIX-defined contract the code already standardizes on
   (`durability_msync_range`), keeps semantics identical, and waits once for
   batched writeback instead of once per slot. Precedent already exists in
   this file: the kfcache shutdown path does exactly
   `msync(e->map, e->map_size, MS_SYNC)`. The alternative (one
   `fdatasync(fd)`; fewer syscalls, batched writeback of mmap dirt on
   Linux) is noted as a follow-up once the new metrics can quantify it —
   POSIX leaves mmap-durability-via-fdatasync formally unspecified, so we
   do not switch the contract silently.
2. **Bitmap windows join the IdxTouch flush** instead of syncing inside
   `bitmap_prepare_window_apply`. `bm_close` keeps fd+mmap alive in the bm
   cache, so a flush-seam `bm_open`+`bm_sync`+`bm_close` per touched
   (field, kf_shard) is correct and cheap, satisfies invariant I1 (flush
   runs in phase I, before K and C), and makes bitmap windows visible to
   the existing `SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_I)` seam.
3. **Marker publish switches link-dance → fsync+rename+dir-fsync.** Safe:
   `kf_shard_marker_gate` replays and clears any retained marker before a
   new window plans, so a pre-existing final name at publish time can never
   be another window's live intent (link's create-once EEXIST semantics are
   not load-bearing). Saves link + tmp-unlink per window; the tri-state
   return contract is preserved exactly.
4. **`BULK_COMMIT_WINDOW` bump lands LAST**, after the per-record syncs are
   gone, so its effect (amortizing fixed per-window costs 4×) is measurable
   against the new phase metrics rather than masking them.
5. **Single-record paths do not change.** `make_index_diff_arg` has zero
   non-bulk call sites; storage.c CRUD keeps `sync_after =
   (type == IT_BITMAP)` semantics; `idx_touch_record` no-ops when the TLS
   set is not installed (everywhere outside `bulk_apply_and_sync_indexes_locked`).

## Invariants (unchanged by this plan)

- **I1**: index mutations are durable before the commit-intent marker is
  cleared. The flush seam already runs inside phase I (slotcask.c, before
  `bulk_apply_and_sync_kf_locked` and `bulk_clear_window_marker_locked`);
  bitmap joins it there.
- **Fail-closed**: any flush failure returns -1 from phase I → the marker is
  retained → forward replay re-runs A→I→K→T→C idempotently. No new failure
  modes are introduced; existing replay machinery is reused untouched.
- **No O_CREAT of never-written index files**: only genuinely touched
  (field, shard) pairs are synced. This fixes today's empty-`.idx`
  materialization; `idx_touch_record` calls must therefore only fire after a
  mutation was actually attempted.
- **Marker tri-state contract** (0 published+durable / 1 published,
  durability unconfirmed / -1 never published) is preserved byte-for-byte in
  semantics.

## Out of scope (deliberate deferrals)

- **Marker payload trim** (key/old/new spans): the spans are read back to
  checksum the marker (fail-closed integrity), so removal requires a
  `KF_BATCH_MARKER_VERSION` bump and a replay-parser change. Separate plan
  after this one lands and the metrics show publish cost matters.
- **`fdatasync` instead of `msync` for K** — revisit with real phase data.
- **Crash-injection test for `bm_sync` itself** — the note-sync(I) seam now
  covers bitmap windows; wiring a daemon-reachable arming mechanism is
  follow-up test-harness work.

## Execution rules (this repo)

- Fresh branch off `main`. Do tasks in order. **Leave all work uncommitted**
  (standing AGENTS.md exception — reviewer reads the raw diff).
- Build: `SKIP_TESTS=1 ./build.sh`. Test: `./build/bin/shard-db-test run <name>`
  / `run-all`.
- If a quoted anchor is not found exactly, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire run immediately.
- The human runs benches (`./build/bin/shard-db-bench bench-invoice`); the
  executor never runs benches.
- Final gate (Task 8): full suite fresh, then `BUILD_MODE=asan SKIP_TESTS=1
  ./build.sh` + three `./build/bin/shard-db-test run-all`, then
  `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` + three
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1"
  ./build/bin/shard-db-test run-all`. No `halt_on_error` anywhere.
- Baseline for the perf claim: the human has `bench-invoice` numbers on the
  pre-change `main` HEAD. Record `git rev-parse main` in the final summary
  so the comparison pair is unambiguous.

---

## Task 1 — Per-phase durability metrics (finding 7)

**Root cause.** `commit_sync_us_record` is called from only two wrappers:
`kfcache_sync_slots_locked` (K phase) and `kf_batch_marker_clear` (whose only
caller is a test). Marker publish (M), segment barrier (A), index syncs (I)
and marker clear (C) are uninstrumented, and the struct comment at
`shard_db_internal.h` claims coverage the code does not have. Every
subsequent task is verified through these counters, so this lands first.

### Test first — `src/test/cases/test_commit_phase_metrics.c` (new file)

Red on base: the counters do not exist, so the file fails to compile; after
Task 1 it compiles and asserts a bulk window actually moved them. (New-API
tests are compile-red on base by nature; the behavioral reds for Tasks 2/5/7
below are runtime-red.)

```c
/* Red on base: g_commit_windows_total & friends do not exist yet. After
   Task 1: a plain (unindexed) bulk upsert must run >= 1 commit window and
   publish >= 1 marker, proving the M/C instrumentation wraps real work. */
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "slotcask.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void expect(int cond, const char *what) {
    if (!cond) { fprintf(stderr, "FAIL: %s\n", what); t_ctx->failed++; }
}

static int test_commit_phase_metrics_run(void) {
    slotcask_init(64, 64);
    char base[] = "/tmp/shard-db-phase-metrics-XXXXXX";
    if (!mkdtemp(base)) return 1;
    char d[PATH_MAX], k[PATH_MAX];
    snprintf(d, sizeof(d), "%s/data", base);
    snprintf(k, sizeof(d), "%s/data/kf", base);
    if (mkdir(d, 0755) != 0 || mkdir(k, 0755) != 0) return 1;

    SlotcaskDb db;
    memset(&db, 0, sizeof(db));
    if (slotcask_open(&db, base, 8, 1, 64) != 0) return 1;
    db.bulk_commit_window = 16;

    uint64_t w0  = __atomic_load_n(&g_commit_windows_total, __ATOMIC_RELAXED);
    uint64_t m0  = __atomic_load_n(&g_commit_marker_publish_count, __ATOMIC_RELAXED);
    uint64_t mu0 = __atomic_load_n(&g_commit_marker_publish_us_total, __ATOMIC_RELAXED);
    uint64_t cu0 = __atomic_load_n(&g_commit_marker_clear_us_total, __ATOMIC_RELAXED);

    SlotcaskBulkRec batch[8];
    memset(batch, 0, sizeof(batch));
    for (int i = 0; i < 8; i++) {
        batch[i].key   = batch[i].user_ctx = NULL; /* filled below */
        batch[i].value = NULL;
    }
    static char keys[8][16], vals[8][16];
    for (int i = 0; i < 8; i++) {
        snprintf(keys[i], sizeof(keys[i]), "pm-key-%02d", i);
        snprintf(vals[i], sizeof(vals[i]), "pm-val-%02d", i);
        batch[i].key   = keys[i];
        batch[i].klen  = strlen(keys[i]);
        batch[i].value = vals[i];
        batch[i].vlen  = strlen(vals[i]);
        batch[i].status = 0;
        batch[i].was_update = 0;
    }
    SlotcaskBulkOpts opts = {0};   /* no indexed fields: hooks not required */
    int rc = slotcask_bulk_upsert_in_kfshard(&db, 0, batch, 8, &opts);
    expect(rc == 0, "bulk upsert succeeds");
    for (int i = 0; i < 8; i++)
        expect(batch[i].status == 0, "record committed");

    uint64_t w1  = __atomic_load_n(&g_commit_windows_total, __ATOMIC_RELAXED);
    uint64_t m1  = __atomic_load_n(&g_commit_marker_publish_count, __ATOMIC_RELAXED);
    uint64_t mu1 = __atomic_load_n(&g_commit_marker_publish_us_total, __ATOMIC_RELAXED);
    uint64_t cu1 = __atomic_load_n(&g_commit_marker_clear_us_total, __ATOMIC_RELAXED);
    expect(w1 > w0,  "commit_windows_total advanced");
    expect(m1 > m0,  "commit_marker_publish_count advanced");
    expect(mu1 >= mu0, "marker_publish_us_total present (grows or stays)");
    expect(cu1 >= cu0, "marker_clear_us_total present (grows or stays)");

    slotcask_close(&db);
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    slotcask_shutdown();
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-commit-phase-metrics", test_commit_phase_metrics_run)
```

### Changes

**1a. Counters — `src/db/shard_db_internal.h`.** Replace the stale comment
block and add fields. Anchor (comment begins):

```c
    /* Durability commit-window instrumentation. commit_count
```

through

```c
    uint64_t commit_count;
    uint64_t commit_lock_hold_us_total;
    uint64_t commit_sync_us_total;
```

replace with:

```c
    /* Durability commit-window instrumentation. commit_count and
       commit_lock_hold_us_total cover single-record upsert/insert commits
       and each bulk shard-call (one shard-call may contain many windows —
       use windows_total for per-window math). Per-phase totals below split
       the M/A/I/K/C barrier costs so sync_us_avg is attributable:
       marker_publish/clear = MFM2 publication + clear fsyncs,
       segment_sync = the P/A segment msync+fdatasync barrier,
       index_sync = the per-(field, idx shard) flush in phase I,
       commit_sync_us_total = the K-phase kf mapping sync only. */
    uint64_t commit_count;
    uint64_t commit_lock_hold_us_total;
    uint64_t commit_sync_us_total;
    uint64_t commit_windows_total;
    uint64_t commit_marker_publish_us_total;
    uint64_t commit_marker_publish_count;
    uint64_t commit_segment_sync_us_total;
    uint64_t commit_index_sync_us_total;
    uint64_t commit_index_sync_ops_total;
    uint64_t commit_marker_clear_us_total;
```

**1b. Macros — same file.** After the anchor line

```c
#define g_commit_sync_us_total      (g_db->commit_sync_us_total)
```

add:

```c
#define g_commit_windows_total      (g_db->commit_windows_total)
#define g_commit_marker_publish_us_total (g_db->commit_marker_publish_us_total)
#define g_commit_marker_publish_count    (g_db->commit_marker_publish_count)
#define g_commit_segment_sync_us_total   (g_db->commit_segment_sync_us_total)
#define g_commit_index_sync_us_total     (g_db->commit_index_sync_us_total)
#define g_commit_index_sync_ops_total    (g_db->commit_index_sync_ops_total)
#define g_commit_marker_clear_us_total   (g_db->commit_marker_clear_us_total)
```

**1c. Generic recorder — `src/db/slotcask.c`.** After the existing helper

```c
/* Accumulate time spent in marker and targeted kf durability barriers. */
static void commit_sync_us_record(uint64_t t0) {
    if (g_db) __atomic_add_fetch(&g_commit_sync_us_total, now_us() - t0, __ATOMIC_RELAXED);
}
```

add:

```c
/* Accumulate time into one named durability-phase counter (see
   shard_db_internal.h). counter is one of the g_commit_*_us_total lvalues. */
static void commit_phase_us_record(uint64_t *counter, uint64_t t0) {
    if (g_db) __atomic_add_fetch(counter, now_us() - t0, __ATOMIC_RELAXED);
}
```

**1d. Wrap M — `bulk_commit_one_kf_window`.** Anchor:

```c
    prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
```

replace with:

```c
    {
        uint64_t t0m = now_us();
        prc = bulk_publish_window_marker_locked(txn, &kh, &plan);
        commit_phase_us_record(&g_commit_marker_publish_us_total, t0m);
        if (plan.nactive > 0)
            __atomic_add_fetch(&g_commit_marker_publish_count, 1,
                               __ATOMIC_RELAXED);
    }
```

**1e. Wrap A — `bulk_activate_new_payloads_locked`.** Anchor:

```c
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    rc = bulk_seg_apply_and_sync(txn->db, locs, n, 1, 1);
```

replace with:

```c
    qsort(locs, n, sizeof(*locs), segloc_cmp);
    {
        uint64_t t0a = now_us();
        rc = bulk_seg_apply_and_sync(txn->db, locs, n, 1, 1);
        commit_phase_us_record(&g_commit_segment_sync_us_total, t0a);
    }
```

**1f. Wrap I + ops count — `bulk_apply_and_sync_indexes_locked`.** Anchor
(the flush loop):

```c
    for (size_t i = 0; i < plan->touch.n; i++) {
        const IdxTouch *t = &plan->touch.v[i];
        const char *field = t->field;
        if (index_sync_record_fields(eff_root, object, txn->db->num_shards,
                                     t->hash16, &field,
                                     (const enum IndexType *)&t->type,
                                     1) != 0)
            return -1;
    }
```

replace with (keep the bitmap branch a simple passthrough until Task 2 adds
it — do not add it here):

```c
    {
        uint64_t t0i = now_us();
        for (size_t i = 0; i < plan->touch.n; i++) {
            const IdxTouch *t = &plan->touch.v[i];
            const char *field = t->field;
            if (index_sync_record_fields(eff_root, object, txn->db->num_shards,
                                         t->hash16, &field,
                                         (const enum IndexType *)&t->type,
                                         1) != 0)
                return -1;
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }
```

**1g. Wrap C — `bulk_clear_window_marker_locked`.** Anchor:

```c
    if (unlink(path) != 0) return -1;
    rc = fsync_dir(kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) rc = -1;
    return rc;
```

replace with:

```c
    if (unlink(path) != 0) return -1;
    {
        uint64_t t0c = now_us();
        rc = fsync_dir(kf_dir);
        commit_phase_us_record(&g_commit_marker_clear_us_total, t0c);
    }
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) rc = -1;
    return rc;
```

**1h. Stats output — `src/db/server.c`.** In the `stats` JSON branch,
anchor:

```c
        uint64_t commit_n         = __atomic_load_n(&g_commit_count, __ATOMIC_RELAXED);
        uint64_t commit_hold_us   = __atomic_load_n(&g_commit_lock_hold_us_total, __ATOMIC_RELAXED);
        uint64_t commit_sync_us   = __atomic_load_n(&g_commit_sync_us_total, __ATOMIC_RELAXED);
```

add after it:

```c
        uint64_t commit_windows   = __atomic_load_n(&g_commit_windows_total, __ATOMIC_RELAXED);
        uint64_t commit_mpub_us   = __atomic_load_n(&g_commit_marker_publish_us_total, __ATOMIC_RELAXED);
        uint64_t commit_mpub_n    = __atomic_load_n(&g_commit_marker_publish_count, __ATOMIC_RELAXED);
        uint64_t commit_segsync_us = __atomic_load_n(&g_commit_segment_sync_us_total, __ATOMIC_RELAXED);
        uint64_t commit_idxsync_us = __atomic_load_n(&g_commit_index_sync_us_total, __ATOMIC_RELAXED);
        uint64_t commit_idxsync_n  = __atomic_load_n(&g_commit_index_sync_ops_total, __ATOMIC_RELAXED);
        uint64_t commit_mclear_us = __atomic_load_n(&g_commit_marker_clear_us_total, __ATOMIC_RELAXED);
```

In the table-format `OUT` (anchor `"commit          count=%lu`), extend the
line with ` windows=%lu marker_publish_us=%lu marker_publish_n=%lu
segment_sync_us=%lu index_sync_us=%lu index_sync_ops=%lu
marker_clear_us=%lu` and the matching arguments. In the JSON `OUT` (anchor
`"commit\":{\"count\":%lu,`), extend the commit object with
`\"windows_total\":%lu,\"marker_publish_us_total\":%lu,\"marker_publish_count\":%lu,\"segment_sync_us_total\":%lu,\"index_sync_us_total\":%lu,\"index_sync_ops_total\":%lu,\"marker_clear_us_total\":%lu` and matching arguments. Then read the second snapshot site
(`g_commit_count` appears again around server.c:1156): if that site also
emits commit metrics, extend it the same way; if it is a different format
that carries no commit fields, note that in the PR description instead.

### Verify

```bash
SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-commit-phase-metrics
./build/bin/shard-db-test run-all
```

---

## Task 2 — Converge indexed bulk syncing on the touched-file flush (findings 1, 3, 4)

**Root cause.** `make_index_diff_arg` sets `sync_after=1` for every index
type, so each of the five bulk apply_window callbacks pays one
fdatasync per changed (record, field) — the same file re-synced thousands of
times per window — while the dedup flush seam built for this (`tls_idx_touch`
installed around apply_window, qsort dedupe, flush before K/C) has zero
recording call sites (`idx_touch_record` is `__attribute__((unused))`).
Bulk insert additionally syncs trigram per record (`ta.sync_after = 1`) and
never syncs bitmap windows at all (finding 1: marker cleared while bitmap
pages are cache-only), and its btree loop syncs every shard of every touched
field via `btree_sync_path`, whose `bt_acquire(writer)` O_CREATs empty
`.idx` files for never-touched shards.

### Test first — `src/test/cases/test_bulk_idx_sync_batching.c` (new file)

Daemon-based (needs real schema + indexes). Red on base: after Task 1 the
`index_sync_ops_total` stat exists but stays `0` (the flush is dead code),
so the `expect(idx_ops > 0, ...)` fails. Green after this task. Correctness
finds also guard against batching breaking index contents.

```c
/* Red on base: bulk insert/update/delete on an indexed object leave
   commit.index_sync_ops_total == 0 (per-record syncs bypass the flush
   seam). After Task 2 the same workload records unique (field, shard)
   touches: ops > 0 and bounded by windows x fields x idx-shards. */
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect(int cond, const char *what) {
    if (!cond) { fprintf(stderr, "FAIL: %s\n", what); t_ctx->failed++; }
}

/* Extract a JSON integer that follows `"key":` in resp. Returns 0 on miss. */
static long json_int_after(const char *resp, const char *key) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(resp, pat);
    if (!p) return 0;
    return strtol(p + strlen(pat), NULL, 10);
}

static int test_bulk_idx_sync_batching_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bat\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":["
        "{\"name\":\"status\",\"type\":\"varchar\",\"size\":16},"
        "{\"name\":\"note\",\"type\":\"varchar\",\"size\":64}"
        "],\"indexes\":[]}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"default\",\"object\":\"bat\","
        "\"field\":\"status\"}", &resp);
    free(resp); resp = NULL;

    /* 32 fresh inserts through the indexed bulk-insert window. */
    {
        char *req = malloc(8192); size_t p = 0;
        p += (size_t)snprintf(req + p, 8192 - p,
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bat\","
            "\"records\":[");
        for (int i = 0; i < 32; i++)
            p += (size_t)snprintf(req + p, 8192 - p,
                "%s{\"key\":\"K-%02d\",\"value\":{\"status\":\"S%d\",\"note\":\"n%d\"}}",
                i ? "," : "", i, i % 4, i);
        snprintf(req + p, 8192 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk insert ok");
        free(resp); resp = NULL; free(req);
    }

    /* 32 updates that change the indexed field (per-record syncs today). */
    {
        char *req = malloc(8192); size_t p = 0;
        p += (size_t)snprintf(req + p, 8192 - p,
            "{\"mode\":\"bulk-update\",\"dir\":\"default\",\"object\":\"bat\","
            "\"records\":[");
        for (int i = 0; i < 32; i++)
            p += (size_t)snprintf(req + p, 8192 - p,
                "%s{\"key\":\"K-%02d\",\"value\":{\"status\":\"T%d\"}}",
                i ? "," : "", i, i % 3);
        snprintf(req + p, 8192 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk update ok");
        free(resp); resp = NULL; free(req);
    }

    /* 16 deletes through the indexed bulk-delete window. */
    {
        char *req = malloc(2048); size_t p = 0;
        p += (size_t)snprintf(req + p, 2048 - p,
            "{\"mode\":\"bulk-delete\",\"dir\":\"default\",\"object\":\"bat\","
            "\"keys\":[");
        for (int i = 0; i < 16; i++)
            p += (size_t)snprintf(req + p, 2048 - p, "%s\"K-%02d\"",
                                  i ? "," : "", i);
        snprintf(req + p, 2048 - p, "]}");
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "indexed bulk delete ok");
        free(resp); resp = NULL; free(req);
    }

    /* Index contents converged: old values gone, new values found. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bat\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"S0\"}],"
        "\"limit\":100}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "find old value ok");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bat\","
        "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"T1\"}],"
        "\"limit\":100}", &resp);
    expect(resp && strstr(resp, "K-"), "find new value hits");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"stats\"}", &resp);
    long windows = json_int_after(resp, "windows_total");
    long ops     = json_int_after(resp, "index_sync_ops_total");
    free(resp); resp = NULL;

    expect(windows > 0, "windows_total > 0 (Task 1 present)");
    expect(ops > 0, "index_sync_ops_total > 0 (RED on base)");
    /* Bound: <= windows x 1 field x index_splits_for(8)=2 shards per window,
       plus slack for the second find-triggered nothing (stats only reads).
       Anything near the old per-record count (>= 48 syncs) fails this. */
    expect(ops <= windows * 2 + 8, "ops bounded by touched files, not records");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-bulk-idx-sync-batching", test_bulk_idx_sync_batching_run)
```

Second test — bitmap/trigram windows. **The executor must first confirm the
declared-index syntax for index types** in
`docs/query-protocol/schema-mutations.md` (create-object `indexes` with a
bitmap and a trigram index). If the syntax matches the form below, use this
test; if it differs, patch the two `create-object`/`add-index` request
strings to the documented form and note it in PLAN_NOTES only if no
declared-type syntax exists (then bitmap/trigram coverage rides on the
sanitizer suite and this test stays btree-only).

```c
/* src/test/cases/test_bulk_idx_types_batching.c — red on base for the same
   reason as test-bulk-idx-sync-batching, but exercises IT_BITMAP and
   IT_TRIGRAM windows (bitmap sync existence is the finding-1 fix). */
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect(int cond, const char *what) {
    if (!cond) { fprintf(stderr, "FAIL: %s\n", what); t_ctx->failed++; }
}
static long json_int_after(const char *resp, const char *key) {
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(resp, pat);
    if (!p) return 0;
    return strtol(p + strlen(pat), NULL, 10);
}

static int test_bulk_idx_types_batching_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;
    /* NOTE(executor): confirm the index-type declaration syntax in
       docs/query-protocol/schema-mutations.md; the form below assumes a
       "type" qualifier on the index entry. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bix\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":["
        "{\"name\":\"flag\",\"type\":\"varchar\",\"size\":8},"
        "{\"name\":\"desc\",\"type\":\"varchar\",\"size\":128}"
        "],"
        "\"indexes\":[{\"field\":\"flag\",\"type\":\"bitmap\"},"
        "{\"field\":\"desc\",\"type\":\"trigram\"}]}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "create typed indexes ok");
    free(resp); resp = NULL;

    char *req = malloc(16384); size_t p = 0;
    p += (size_t)snprintf(req + p, 16384 - p,
        "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"bix\","
        "\"records\":[");
    for (int i = 0; i < 32; i++)
        p += (size_t)snprintf(req + p, 16384 - p,
            "%s{\"key\":\"B-%02d\",\"value\":{\"flag\":\"f%d\","
            "\"desc\":\"alpha beta gamma %d\"}}", i ? "," : "", i, i % 2, i);
    snprintf(req + p, 16384 - p, "]}");
    tc_request(tc, req, &resp);
    expect(resp && !strstr(resp, "\"error\""), "bitmap+trigram bulk insert ok");
    free(resp); resp = NULL; free(req);

    /* Both index types resolve their rows after the batched window. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bix\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"f1\"}],"
        "\"limit\":100}", &resp);
    expect(resp && strstr(resp, "B-"), "bitmap find hits");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"bix\","
        "\"criteria\":[{\"field\":\"desc\",\"op\":\"contains\","
        "\"value\":\"beta\"}],\"limit\":100}", &resp);
    expect(resp && strstr(resp, "B-"), "trigram contains hits");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"stats\"}", &resp);
    long ops = json_int_after(resp, "index_sync_ops_total");
    free(resp); resp = NULL;
    expect(ops > 0, "bitmap/trigram touches flushed (RED on base)");

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-bulk-idx-types-batching", test_bulk_idx_types_batching_run)
```

### Changes

**2a. Promote the recorder — `src/db/slotcask.c`.** Anchor:

```c
static void __attribute__((unused)) idx_touch_record(const char *field, int idx_shard, int type,
                             const uint8_t hash16[16]) {
    IdxTouchSet *s = tls_idx_touch;
    if (!s) return;
```

replace with (drop `static` + unused attribute; make hash16 optional for
bitmap, which routes by kf shard, not hash):

```c
void idx_touch_record(const char *field, int idx_shard, int type,
                      const uint8_t hash16[16]) {
    IdxTouchSet *s = tls_idx_touch;
    if (!s) return;
```

and inside, replace

```c
    memcpy(s->v[s->n].hash16, hash16, 16);
    s->n++;
```

with

```c
    if (hash16) memcpy(s->v[s->n].hash16, hash16, 16);
    else memset(s->v[s->n].hash16, 0, 16);
    s->n++;
```

**2b. Declare it — `src/db/slotcask.h`.** Near the bulk-window declarations
(anchor: the `slotcask_bulk_upsert_in_kfshard` declaration block), add:

```c
/* Record one (field, idx shard) index file the current bulk window mutated,
   so bulk_apply_and_sync_indexes_locked can fdatasync each unique file once
   before the marker is cleared instead of every mutation syncing inline.
   No-op when no bulk apply_window is in flight (single-record paths keep
   their own sync contract). For IT_BITMAP, idx_shard is the kf shard and
   hash16 may be NULL. */
void idx_touch_record(const char *field, int idx_shard, int type,
                      const uint8_t hash16[16]);
```

**2c. Record instead of sync in `update_idx_fn` — `src/db/index.c`.**
Btree case — replace

```c
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char idx_path[PATH_MAX];
                build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(idx_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            }
```

with

```c
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char idx_path[PATH_MAX];
                build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(idx_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            } else if (!a->out_error && (a->old_key || a->new_key)) {
                /* Bulk window: record the touched (field, shard) so the
                   post-apply flush syncs each unique file exactly once
                   (invariant I1) instead of per record. */
                idx_touch_record(a->field, idx_shard_for_hash(a->hash, a->splits),
                                 IT_BTREE, a->hash);
            }
```

Trigram case — replace

```c
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char tg_path[PATH_MAX];
                tg_build_path(tg_path, sizeof(tg_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(tg_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            }
```

with

```c
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char tg_path[PATH_MAX];
                tg_build_path(tg_path, sizeof(tg_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(tg_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            } else if (!a->out_error && (n_old_deleted || n_new_inserted)) {
                /* Bulk window: record only when a trigram leaf write really
                   happened — key churn with an identical trigram set must
                   not O_CREAT an otherwise-untouched .tg via the flush. */
                idx_touch_record(a->field, idx_shard_for_hash(a->hash, a->splits),
                                 IT_TRIGRAM, a->hash);
            }
```

For this, the trigram case needs the two counters it already implies. In
the delete loop, replace

```c
                if (!in_new) {
                    if (tg_idx_delete(a->db_root, a->object, a->field, a->splits,
                                      old_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                }
```

with

```c
                if (!in_new) {
                    if (tg_idx_delete(a->db_root, a->object, a->field, a->splits,
                                      old_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                    n_old_deleted++;
                }
```

in the insert loop, replace

```c
                if (!in_old) {
                    if (tg_idx_insert(a->db_root, a->object, a->field, a->splits,
                                      new_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                }
```

with

```c
                if (!in_old) {
                    if (tg_idx_insert(a->db_root, a->object, a->field, a->splits,
                                      new_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                    n_new_inserted++;
                }
```

and at the top of the `IT_TRIGRAM` case, next to the existing declarations

```c
            uint8_t old_tg[TG_MAX_DISTINCT][3];
            uint8_t new_tg[TG_MAX_DISTINCT][3];
```

add:

```c
            size_t n_old_deleted = 0, n_new_inserted = 0;
```

Bitmap case — replace

```c
            a->out_error = bitmap_update(a->db_root, a->object, a->field,
                                         a->kf_shard, a->splits,
                                         a->kf_slot, a->bm_max_values,
                                         a->new_key, a->new_len,
                                         a->old_key, a->old_len,
                                         a->sync_after);
```

with

```c
            a->out_error = bitmap_update(a->db_root, a->object, a->field,
                                         a->kf_shard, a->splits,
                                         a->kf_slot, a->bm_max_values,
                                         a->new_key, a->new_len,
                                         a->old_key, a->old_len,
                                         a->sync_after);
            if (!a->out_error && !a->sync_after && (a->old_key || a->new_key)) {
                /* Bulk window: the flush seam syncs this (field, kf shard)
                   bitmap file once via the bm cache (hash16 unused — bitmap
                   files route by kf shard). */
                idx_touch_record(a->field, a->kf_shard, IT_BITMAP, NULL);
            }
```

**2d. Flush seam learns bitmap — `src/db/slotcask.c`,
`bulk_apply_and_sync_indexes_locked`.** Replace the loop anchored at

```c
    for (size_t i = 0; i < plan->touch.n; i++) {
        const IdxTouch *t = &plan->touch.v[i];
        const char *field = t->field;
        if (index_sync_record_fields(eff_root, object, txn->db->num_shards,
```

(that is, the loop as rewritten in Task 1f) with:

```c
    {
        uint64_t t0i = now_us();
        for (size_t i = 0; i < plan->touch.n; i++) {
            const IdxTouch *t = &plan->touch.v[i];
            const char *field = t->field;
            if (t->type == IT_BITMAP) {
                if (bitmap_sync_shard_path(eff_root, object, t->field,
                                           t->idx_shard,
                                           txn->db->num_shards) != 0)
                    return -1;
            } else if (index_sync_record_fields(eff_root, object,
                                                txn->db->num_shards,
                                                t->hash16, &field,
                                                (const enum IndexType *)&t->type,
                                                1) != 0)
                return -1;
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }
```

Also fix the stale dedupe comment — replace

```c
    if (plan->touch.n > 0)     /* delete windows carry no touches: v is NULL */
```

with

```c
    if (plan->touch.n > 0)     /* empty until Task 2026-09-04 wired recorders */
```

**2e. Bitmap sync-by-path helper — `src/db/index.c`** (place next to
`index_sync_record_fields`), plus its declaration in `src/db/types.h` next
to `int index_sync_record_fields(...)`:

```c
/* Flush one (field, kf shard) bitmap file through the bm cache. Used by the
   bulk window flush seam (invariant I1: before marker clear). bm handles
   stay alive in the cache after bm_close, so this is one fdatasync per
   unique file per window. A missing file means nothing was ever written —
   not an error. Returns 0 or -1. */
int bitmap_sync_shard_path(const char *db_root, const char *object,
                           const char *field, int kf_shard, int splits) {
    char path[1024];
    bitmap_shard_path(path, sizeof(path), db_root, object, field, kf_shard);
    int slots = (int)slotcask_default_slots_for_splits(splits);
    BitmapShard *bm = bm_open(path, slots, 0, 0, 0, 1 /* writer */);
    if (!bm) return errno == ENOENT ? 0 : -1;
    int rc = bm_sync(bm);
    bm_close(bm);
    return rc;
}
```

types.h declaration (anchor: the `index_sync_record_fields` declaration):

```c
int bitmap_sync_shard_path(const char *db_root, const char *object,
                           const char *field, int kf_shard, int splits);
```

NOTE(executor): if `bm_open`'s 6-arg signature differs from
`(path, slots, create, bool_fastpath, bm_max_values, writer)` in
`bitmap.h`, match the `bitmap_prepare_open` call in index.c and adapt; halt
via PLAN_NOTES only if `bm_open` cannot open an existing file read-write
without creating.

**2f. Bulk insert: trigram stops syncing per record — `src/db/query_bulk.c`.**
Anchor:

```c
                    ta.hash = r->hash; ta.type = IT_TRIGRAM;
                    ta.sync_after = 1;
```

replace with:

```c
                    ta.hash = r->hash; ta.type = IT_TRIGRAM;
                    ta.sync_after = 0;  /* flush seam syncs once per touched
                                           (field, idx shard) after apply */
```

**2g. Bulk insert: bitmap windows join the flush — same file,
`v2_bulk_ins_prepare_window`, IT_BITMAP branch.** After the successful-add
branch anchor:

```c
                    int rc = bitmap_prepare_window_add(&sw->bw_window, &ba,
                                                        errf, sizeof(errf));
                    if (rc == 0) {
```

insert the touch immediately inside the `if (rc == 0) {` block, before the
existing `bw_track_buf` line:

```c
                    int rc = bitmap_prepare_window_add(&sw->bw_window, &ba,
                                                        errf, sizeof(errf));
                    if (rc == 0) {
                        idx_touch_record(sw->idx_fields[fi], rec->kf_shard,
                                         IT_BITMAP, NULL);
```

(Recording happens in prepare, syncing still happens only in phase I via
the flush seam; a window that later aborts destroys the plan's touch set
with it.)

**2h. Bulk insert: btree syncs become touched-shard flushes — same file,
`v2_bulk_ins_apply_window`.** In the btree-delete loop, replace

```c
    for (size_t i = 0; i < sw->bt_del_nops; i++) {
        BtDeleteOp *op = &sw->bt_del_ops[i];
        if (delete_index_entry(sw->db_root, sw->object, sw->idx_fields[op->fi],
                               sw->sch->splits, op->key, op->klen, op->hash) != 0)
            rc = -1;
        bt_field_touched[op->fi] = 1;
    }
```

with

```c
    for (size_t i = 0; i < sw->bt_del_nops; i++) {
        BtDeleteOp *op = &sw->bt_del_ops[i];
        if (delete_index_entry(sw->db_root, sw->object, sw->idx_fields[op->fi],
                               sw->sch->splits, op->key, op->klen, op->hash) != 0)
            rc = -1;
        idx_touch_record(sw->idx_fields[op->fi],
                         idx_shard_for_hash(op->hash, sw->sch->splits),
                         IT_BTREE, op->hash);
        bt_field_touched[op->fi] = 1;
    }
```

In the merge loop, replace

```c
        idx_build_field_worker(&fa);
        if (fa.out_error) rc = -1;
        bt_field_touched[fi] = 1;
    }
```

with

```c
        idx_build_field_worker(&fa);
        if (fa.out_error) rc = -1;
        for (size_t pi = 0; pi < count; pi++)
            idx_touch_record(sw->idx_fields[fi],
                             idx_shard_for_hash(sw->idx_pairs[fi][pi].hash,
                                                sw->sch->splits),
                             IT_BTREE, sw->idx_pairs[fi][pi].hash);
        bt_field_touched[fi] = 1;
    }
```

Then delete the blanket all-shards sync loop entirely — replace

```c
    /* btree_bulk_merge/delete_index_entry only dirty mmap'd pages — they do
       not fsync. Force every touched field's shards durable now, before the
       window's marker gets cleared and Kf is published, so a crash right
       after "apply succeeded" can't leave on-disk B-tree state lagging
       behind the now-durable Kf state. */
    for (int fi = 0; fi < sw->nidx; fi++) {
        if (!bt_field_touched[fi]) continue;
        int idx_n = index_splits_for(sw->sch->splits);
        for (int s = 0; s < idx_n; s++) {
            char idx_path[PATH_MAX];
            build_idx_path(idx_path, sizeof(idx_path), sw->db_root, sw->object,
                           sw->idx_fields[fi], s);
            if (btree_sync_path(idx_path) != 0) rc = -1;
        }
    }

    if (bitmap_prepare_window_apply(&sw->bw_window) != 0) rc = -1;
    return rc;
```

with

```c
    /* btree_bulk_merge/delete_index_entry only dirty mmap'd pages — they do
       not fsync. Durability for this window's btree/trigram/bitmap files is
       now owned by the post-apply flush seam (unique (field, shard) files
       synced exactly once, before the marker is cleared — invariant I1).
       btree_bulk_merge's rename-published shards are already durable; their
       recorded touch re-fdatasyncs an already-clean file, which is cheap
       and keeps one uniform rule. Never-touched shards are never synced, so
       bt_acquire(writer)'s O_CREAT can no longer materialize empty .idx
       files (index.c warns about exactly this). */
    if (bitmap_prepare_window_apply(&sw->bw_window) != 0) rc = -1;
    return rc;
```

(`bt_field_touched` remains — it still gates the merge loop's diagnostics
and keeps `nidx` bookkeeping honest; if the compiler flags it unused after
this change, keep the array and its assignments — do not delete them, they
document which fields saw work and cost nothing.)

**2i. Bulk update/delete stop syncing per record — same file.** Replace

```c
    a.bm_max_values = 0;  /* default cap — header wins on existing */
    a.sync_after = 1;
    return a;
```

with

```c
    a.bm_max_values = 0;  /* default cap — header wins on existing */
    /* Bulk windows record touched (field, shard) files instead of syncing
       per mutation; bulk_apply_and_sync_indexes_locked flushes each unique
       file once before the marker is cleared (invariant I1). All five call
       sites are bulk apply_window callbacks (key-list delete, structured /
       delimited / JSON update, criteria delete) — no single-record caller. */
    a.sync_after = 0;
    return a;
```

Also update the function's doc comment above it: replace

```c
   — required whenever type==IT_BITMAP (index.c's update_idx_fn dispatches
   IT_BITMAP straight into bitmap_update using these raw). Callers either
   dispatch immediately via update_idx_fn() or batch several into a
   parallel_for() array; either way out_error must be checked afterward. */
```

with

```c
   — required whenever type==IT_BITMAP (index.c's update_idx_fn dispatches
   IT_BITMAP straight into bitmap_update using these raw). Callers either
   dispatch immediately via update_idx_fn() or batch several into a
   parallel_for() array; either way out_error must be checked afterward.
   sync_after is 0: during a bulk window, update_idx_fn records an
   idx_touch_record instead of syncing, and the window's flush seam does
   one fdatasync per unique (field, idx shard) before marker clear. */
```

### Call-site inventory (signature/behavior changes)

- `idx_touch_record`: previously file-static + unused, zero callers. New:
  declared in slotcask.h; called from index.c (update_idx_fn ×3) and
  query_bulk.c (insert prepare ×1, insert apply ×2). TLS NULL-guard keeps it
  a no-op outside bulk apply windows.
- `make_index_diff_arg`: 5 call sites, all bulk apply_windows
  (query_bulk.c key-list delete, structured update, delimited update, JSON
  update, criteria delete). Behavior change is intentional and covered by
  the invariant I1 argument above.
- `bitmap_sync_shard_path`: new, one caller (flush seam).
- Single-record paths (storage.c CRUD, `bitmap_prepare_set_apply`,
  `bitmap_update` with `sync_after=1`) are untouched.

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-bulk-idx-sync-batching
./build/bin/shard-db-test run test-bulk-idx-types-batching
./build/bin/shard-db-test run test-commit-phase-metrics
./build/bin/shard-db-test run-all
```

Also run the pre-existing durability suites explicitly — their note-sync /
msync-injection scenarios now see non-empty touch sets in phase I:

```bash
./build/bin/shard-db-test run test-durability-sync-failures
./build/bin/shard-db-test run test-durability-ordering
./build/bin/shard-db-test run test-window-release-routes
```

---

## Task 3 — K phase: one msync per window (finding 2)

**Root cause.** `kfcache_sync_slots_locked_impl` issues one blocking
`MS_SYNC` per touched 24-byte slot (~170 slots share a page; hash-scattered
slots mean ~1 syscall per slot regardless), up to `BULK_COMMIT_WINDOW`
sequential device waits per window while holding the shard wrlock. The
window holds the exclusive wrlock, so every dirty page of the mapping
belongs to this window; the codebase already flushes whole mappings this
way at shutdown.

No new test: this is a behavior-preserving refactor of *how* the same
durability barrier is reached. The existing K-failure injection
(`SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)` after the sync in
`bulk_apply_and_sync_kf_locked`, plus `durability_test_msync_fail_on_call`
in `test-durability-sync-failures`) is the guard. Expected observable:
`commit.sync_us_total` per window drops sharply — the human's bench confirms.

### Change — `src/db/slotcask.c`

Replace

```c
static int kfcache_sync_slots_locked_impl(SlotcaskKfHandle *h,
                              const size_t *slots, size_t nslots,
                              int header_changed) {
    if (!h || !h->writer || !h->hdr || (!slots && nslots)) {
        errno = EINVAL;
        return -1;
    }
    for (size_t i = 0; i < nslots; i++) {
        if (slots[i] >= h->capacity) { errno = EINVAL; return -1; }
        size_t off = SLOTCASK_KF_HDR_SIZE + slots[i] * sizeof(*h->map);
        if (durability_msync_range(h->hdr, off, sizeof(*h->map)) < 0)
            return -1;
    }
    return !header_changed ||
           durability_msync_range(h->hdr, 0, SLOTCASK_KF_HDR_SIZE) == 0
               ? 0 : -1;
}
```

with

```c
static int kfcache_sync_slots_locked_impl(SlotcaskKfHandle *h,
                              const size_t *slots, size_t nslots,
                              int header_changed) {
    (void)header_changed;  /* the 24-byte header lives at byte 0 of the same
                              mapping and is covered by the full-range sync */
    if (!h || !h->writer || !h->hdr || (!slots && nslots)) {
        errno = EINVAL;
        return -1;
    }
    for (size_t i = 0; i < nslots; i++)
        if (slots[i] >= h->capacity) { errno = EINVAL; return -1; }
    /* One whole-mapping MS_SYNC instead of one msync per touched slot:
       the caller holds this shard's exclusive wrlock, so every dirty page
       of the mapping belongs to it (readers never dirty pages), and a
       full-range MS_SYNC waits once for the batched writeback instead of
       once per 24-byte slot (~170 slots share a page; hash-scattered slots
       otherwise force ~1 blocking flush per record). Same contract as the
       kfcache shutdown path's msync(map, map_size, MS_SYNC). */
    return msync((void *)h->hdr, h->map_size, MS_SYNC) == 0 ? 0 : -1;
}
```

(`<sys/mman.h>` is already included at slotcask.c:34; keep the
`kfcache_sync_slots_locked` timing wrapper and its
`commit_sync_us_record` unchanged.)

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-durability-sync-failures
./build/bin/shard-db-test run-all
```

---

## Task 4 — Marker publish via rename (finding 6, mechanics only)

**Root cause.** Every window's marker publish costs create + write + fsync +
link + unlink(tmp) + dir-fsync, and the clear costs unlink + dir-fsync —
five namespace operations where three suffice. `link()`'s create-once
semantics are not load-bearing: `kf_shard_marker_gate` replays and clears
any retained marker before a new window plans, so a pre-existing final name
at publish time can never be a different window's live intent.

No new test: the existing marker lifecycle suites
(`test-durability-ordering`, `test-durability-sync-failures`,
`test-window-release-routes`) exercise publish/clear/replay/gate on every
run and are the guard.

### Change — `src/db/slotcask.c`

Replace the tri-state contract comment and body,

```c
/* Tri-state contract:
 *   0  = marker published (linked) and its publication is durable
 *   1  = marker IS published but publication durability is unconfirmed
 *        (post-link unlink/fsync_dir failure) — caller MUST treat this as a
 *        post-M failure: forward replay, else EINPROGRESS
 *   -1 = marker was never linked — safe plain pre-M failure */
static int marker_publish_atomic(const char *kf_dir, const char *final_name,
                                 const void *bytes, size_t len) {
    char tmp_path[PATH_MAX], final_path[PATH_MAX];
    const char *p = bytes;
    size_t left = len;
    int fd = -1;

    if (marker_make_unique_paths(kf_dir, final_name, tmp_path,
                                 sizeof(tmp_path), final_path,
                                 sizeof(final_path)) != 0)
        return -1;
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return -1;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            goto fail_open;
        }
        p += n;
        left -= (size_t)n;
    }
    if (fsync(fd) != 0) goto fail_open;
    if (close(fd) != 0) { fd = -1; goto fail_open; }
    fd = -1;
    if (link(tmp_path, final_path) != 0) goto fail_open;

    /* Past this point the final marker exists: every remaining failure is
     * a post-M outcome and is reported as published-but-pending. */
    if (unlink(tmp_path) == 0 && fsync_dir(kf_dir) == 0)
        return 0;
    return 1;

fail_open:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    return -1;
}
```

with

```c
/* Tri-state contract:
 *   0  = marker published (renamed into place) and its publication durable
 *   1  = marker IS published but publication durability is unconfirmed
 *        (post-rename fsync_dir failure) — caller MUST treat this as a
 *        post-M failure: forward replay, else EINPROGRESS
 *   -1 = marker was never published — safe plain pre-M failure
 * rename (not link) is safe here: kf_shard_marker_gate replays and clears
 * any retained marker before a new window plans, so a pre-existing final
 * name can never belong to another window's live intent. */
static int marker_publish_atomic(const char *kf_dir, const char *final_name,
                                 const void *bytes, size_t len) {
    char tmp_path[PATH_MAX], final_path[PATH_MAX];
    const char *p = bytes;
    size_t left = len;
    int fd = -1;

    if (marker_make_unique_paths(kf_dir, final_name, tmp_path,
                                 sizeof(tmp_path), final_path,
                                 sizeof(final_path)) != 0)
        return -1;
    fd = open(tmp_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (fd < 0) return -1;
    while (left > 0) {
        ssize_t n = write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            goto fail_open;
        }
        p += n;
        left -= (size_t)n;
    }
    if (fsync(fd) != 0) goto fail_open;
    if (close(fd) != 0) { fd = -1; goto fail_open; }
    fd = -1;
    if (rename(tmp_path, final_path) != 0) goto fail_open;

    /* Past this point the final marker exists: every remaining failure is
     * a post-M outcome and is reported as published-but-pending. */
    if (fsync_dir(kf_dir) == 0)
        return 0;
    return 1;

fail_open:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    return -1;
}
```

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-durability-ordering
./build/bin/shard-db-test run test-durability-sync-failures
./build/bin/shard-db-test run test-window-release-routes
./build/bin/shard-db-test run-all
```

---

## Task 5 — Sequence durability (finding 5a)

**Root cause.** `seq_next_val` / `seq_next_val_batch` persist the counter
with `fopen("w") + fclose` — no fsync, no atomic replace, so a crash can
reissue allocated values or truncate the file mid-write — and both return
the allocated value even when the write failed. `cmd_sequence reset` writes
`0` with no flock at all, racing concurrent allocations.

### Test first — `src/test/cases/test_sequence_durability.c` (new file)

Two reds: (1) behavioral — with the sequence state path occupied by a
directory, `seq_next_val` must fail closed (`-1`); on base it silently
returns the value (write failure swallowed). (2) link-red — `seq_state_reset`
does not exist on base.

```c
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static void expect(int cond, const char *what) {
    if (!cond) { fprintf(stderr, "FAIL: %s\n", what); t_ctx->failed++; }
}

static int test_sequence_durability_run(void) {
    char base[] = "/tmp/shard-db-seq-dur-XXXXXX";
    if (!mkdtemp(base)) return 1;

    /* 1. Normal allocation persists and increments. */
    long long v1 = seq_next_val(base, "obj", "s1");
    expect(v1 == 1, "first allocation returns 1");
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/obj/metadata/sequences/s1", base);
    FILE *f = fopen(path, "r");
    expect(f != NULL, "state file exists");
    if (f) {
        long long stored = -1;
        expect(fscanf(f, "%lld", &stored) == 1 && stored == 1,
               "state file holds 1");
        fclose(f);
    }

    /* 2. Write failure must fail closed (RED on base: returns the value). */
    long long v2 = seq_next_val(base, "obj", "s2dir");
    expect(v2 == 1, "fresh sequence starts at 1");
    char dpath[PATH_MAX];
    snprintf(dpath, sizeof(dpath), "%s/obj/metadata/sequences/s2dir", base);
    if (remove(dpath) != 0) { /* file from step's write; clear it */ }
    if (mkdir(dpath, 0755) != 0) return 1;   /* state path is now a dir */
    errno = 0;
    long long v3 = seq_next_val(base, "obj", "s2dir");
    expect(v3 == -1, "state write failure returns -1 (RED on base)");

    /* 3. Durable reset helper (link-red on base: symbol does not exist). */
    expect(seq_state_reset(base, "obj", "s1") == 0, "reset succeeds");
    f = fopen(path, "r");
    expect(f != NULL, "state file exists after reset");
    if (f) {
        long long stored = -1;
        expect(fscanf(f, "%lld", &stored) == 1 && stored == 0,
               "reset wrote 0 durably");
        fclose(f);
    }
    expect(seq_next_val(base, "obj", "s1") == 1, "post-reset allocation is 1");
    /* Reset against a wedged state path must error, not silently succeed. */
    expect(seq_state_reset(base, "obj", "s2dir") == -1,
           "reset on unwritable state fails (RED via seq_state_reset)");

    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", base);
    system(cmd);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-sequence-durability", test_sequence_durability_run)
```

### Changes — `src/db/config.c`

**5a. Durable state-write helper.** After the includes/`mkdirp`-adjacent
helpers, before `seq_next_val`, add:

```c
/* Durable sequence-state write: temp file in the same directory, full
   write, fdatasync, rename, parent-dir fsync. The flock caller holds the
   per-sequence lock across read-modify-write, so rename's replace
   semantics are safe. Returns 0, or -1 with the state file unchanged. */
static int seq_state_write(const char *seq_dir, const char *seq_path,
                           long long val) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", seq_path, (int)getpid());
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    char line[32];
    int n = snprintf(line, sizeof(line), "%lld\n", val);
    const char *p = line;
    size_t left = (size_t)n;
    while (left > 0) {
        ssize_t w = write(fd, p, left);
        if (w < 0) {
            if (errno == EINTR) continue;
            close(fd); unlink(tmp); return -1;
        }
        p += w; left -= (size_t)w;
    }
    if (fdatasync(fd) != 0 || close(fd) != 0) {
        close(fd); unlink(tmp); return -1;
    }
    if (rename(tmp, seq_path) != 0) { unlink(tmp); return -1; }
    if (fsync_parent_dir(seq_path) != 0) return -1;
    return 0;
}

/* Durable reset of one sequence to 0. Takes the same per-sequence flock the
   allocation paths hold, so reset can no longer race a concurrent
   seq_next_val / seq_next_val_batch. Returns 0 or -1. */
int seq_state_reset(const char *db_root, const char *object,
                    const char *seq_name) {
    char seq_dir[PATH_MAX], seq_path[PATH_MAX], lock_path[PATH_MAX];
    snprintf(seq_dir, sizeof(seq_dir), "%s/%s/metadata/sequences", db_root, object);
    mkdirp(seq_dir);
    snprintf(seq_path, sizeof(seq_path), "%s/%s", seq_dir, seq_name);
    snprintf(lock_path, sizeof(lock_path), "%s/%s.lock", seq_dir, seq_name);
    int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return -1;
    flock(lockfd, LOCK_EX);
    int rc = seq_state_write(seq_dir, seq_path, 0);
    flock(lockfd, LOCK_UN);
    close(lockfd);
    return rc;
}
```

**5b. `seq_next_val` fails closed.** Replace

```c
    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    val++;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return val;
}
```

with

```c
    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    val++;
    if (seq_state_write(seq_dir, seq_path, val) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val: durable write failed for [%s]", seq_name);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return val;
}
```

**5c. `seq_next_val_batch` fails closed.** Replace

```c
    long long start = val + 1;
    val += n;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
}
```

with

```c
    long long start = val + 1;
    val += n;
    if (seq_state_write(seq_dir, seq_path, val) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "seq_next_val_batch: durable write failed for [%s]", seq_name);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
}
```

### Changes — `src/db/query_maint.c`

**5d. `cmd_sequence` reset/current lock up.** (`cmd_sequence` lives in
`src/db/query_maint.c`, not config.c — only 5a–5c above are config.c edits.
It already uses the `OUT` + `strerror` style shown, and sees the
`seq_state_reset` declaration via `types.h`.) Replace

```c
    if (strcmp(action, "reset") == 0) {
        FILE *f = fopen(seq_path, "w");
        if (f) { fprintf(f, "0\n"); fclose(f); }
        OUT("{\"sequence\":\"%s\",\"value\":0}\n", seq_name);
        return 0;
    }
```

with

```c
    if (strcmp(action, "reset") == 0) {
        if (seq_state_reset(db_root, object, seq_name) != 0) {
            OUT("{\"error\":\"sequence reset failed: %s\"}\n", strerror(errno));
            return 1;
        }
        OUT("{\"sequence\":\"%s\",\"value\":0}\n", seq_name);
        return 0;
    }
```

(The unlocked `current` read stays as-is — display-only; note it in the PR
description rather than widening scope.)

### Call-site inventory (error-contract change)

`seq_next_val` returning -1 on write failure: `server.c` `auto_key_generate`
(already handles -1 with an error log + response), `config.c`
`generate_default` DK_SEQ branch (already returns NULL on -1).
`seq_next_val_batch` returning -1: `query_bulk.c:1464` and `query_bulk.c:2220`
— both already branch on a negative start (verify during execution; both
paths were written against the -1 lock-failure contract that already
existed). `seq_state_reset`: new symbol, called from `cmd_sequence` and the
new test. Declaration for `seq_state_reset` goes in `src/db/types.h` next
to the existing `seq_next_val` declarations (anchor: `long long seq_next_val(const char *db_root, const char *object,`).

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-sequence-durability
./build/bin/shard-db-test run-all
```

---

## Task 6 — put-file / delete-file durability (finding 5b/5c)

**Root cause.** `cmd_put_file` truncates the live destination, ignores
short writes and read errors, and never fsyncs; `cmd_put_file_b64` ignores
its fsync result and skips the parent-dir fsync after rename; `cmd_delete_file`
unlinks without a dir fsync. A crash after a success response can lose the
stored bytes, revert an overwrite, or resurrect a deleted file.

Test-first note, stated honestly: the fsync/dir-fsync additions have no
portable in-suite failure observable without a crash or fault injector, so
the regression test below guards the *atomicity* fix (tmp+rename in
`cmd_put_file`, checked writes) and round-trip behavior; the durability
property itself rides on code review + the same `fsync_parent_dir` pattern
the marker paths already use. This is a documented deviation from
test-first for the fsync-only hunks.

### Test first — `src/test/cases/test_file_ops_roundtrip.c` (new file)

```c
#include "test_runner.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void expect(int cond, const char *what) {
    if (!cond) { fprintf(stderr, "FAIL: %s\n", what); t_ctx->failed++; }
}

static int test_file_ops_roundtrip_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { test_env_stop(&env); return 1; }
    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* Seed a source file larger than one write() chunk. */
    char src[PATH_MAX];
    snprintf(src, sizeof(src), "%s/upload-src.bin", env.db_root);
    FILE *f = fopen(src, "wb");
    expect(f != NULL, "open upload source");
    if (f) {
        for (int i = 0; i < 200000; i++) fputc('A' + (i % 26), f);
        fclose(f);
    }

    tc_request(tc, "{\"mode\":\"create-object\",\"dir\":\"default\","
                   "\"object\":\"fops\",\"splits\":8,\"max_key\":16,"
                   "\"fields\":[{\"name\":\"v\",\"type\":\"varchar\",\"size\":8}]}",
               &resp);
    free(resp); resp = NULL;

    {
        char req[PATH_MAX + 128];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"put-file\",\"dir\":\"default\",\"object\":\"fops\","
                 "\"path\":\"%s\",\"filename\":\"blob.bin\"}", src);
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "put-file ok");
        free(resp); resp = NULL;
    }

    /* Overwrite with shorter content via b64; the stored file must shrink
       exactly (guards the atomic-replace + truncation semantics). */
    {
        char req[256];
        snprintf(req, sizeof(req),
                 "{\"mode\":\"put-file-b64\",\"dir\":\"default\","
                 "\"object\":\"fops\",\"filename\":\"blob.bin\","
                 "\"data\":\"aGVsbG8=\"}");   /* "hello" */
        tc_request(tc, req, &resp);
        expect(resp && !strstr(resp, "\"error\""), "b64 overwrite ok");
        free(resp); resp = NULL;
    }
    tc_request(tc,
        "{\"mode\":\"get-file-b64\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"blob.bin\"}", &resp);
    expect(resp && strstr(resp, "\"data\":\"aGVsbG8=\""),
           "get-file returns the overwritten content");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"delete-file\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"blob.bin\"}", &resp);
    expect(resp && !strstr(resp, "\"error\""), "delete-file ok");
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"get-file-b64\",\"dir\":\"default\",\"object\":\"fops\","
        "\"filename\":\"blob.bin\"}", &resp);
    expect(resp && strstr(resp, "\"error\""), "get after delete errors");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-file-ops-roundtrip", test_file_ops_roundtrip_run)
```

NOTE(executor): confirm the exact JSON mode names/keys for put-file
(path+filename variant) and put-file-b64 against
`docs/query-protocol/files.md` and the dispatch in `server.c`; patch the
request strings to the documented shapes if they differ.

### Changes — `src/db/query_find.c`

**6a. `cmd_put_file` — atomic replace + checked I/O + fsync.** Replace

```c
    int dfd = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) { close(sfd); fprintf(stderr, "Error: Cannot create %s\n", dest); return 1; }
    char buf[65536]; ssize_t n;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) write(dfd, buf, n);
    close(sfd); close(dfd);
    OUT("{\"status\":\"stored\",\"path\":\"%s\"}\n", dest);
    return 0;
}
```

with

```c
    /* Write to a temp sibling, fsync, rename, fsync the parent dir — an
       interrupted copy leaves the previous file intact instead of a
       truncated live destination. Short writes and read errors are
       failures, not silent truncation. */
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", dest, (int)getpid());
    int dfd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) {
        close(sfd);
        fprintf(stderr, "Error: Cannot create %s\n", tmp);
        return 1;
    }
    char buf[65536]; ssize_t n;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) {
        size_t off = 0;
        while (off < (size_t)n) {
            ssize_t w = write(dfd, buf + off, (size_t)n - off);
            if (w < 0) {
                if (errno == EINTR) continue;
                close(sfd); close(dfd); unlink(tmp);
                fprintf(stderr, "Error: write to %s failed: %s\n",
                        tmp, strerror(errno));
                return 1;
            }
            off += (size_t)w;
        }
    }
    if (n < 0) {
        close(sfd); close(dfd); unlink(tmp);
        fprintf(stderr, "Error: read from %s failed\n", src);
        return 1;
    }
    close(sfd);
    if (fsync(dfd) != 0) {
        close(dfd); unlink(tmp);
        fprintf(stderr, "Error: fsync(%s) failed: %s\n", tmp, strerror(errno));
        return 1;
    }
    if (close(dfd) != 0) {
        unlink(tmp);
        fprintf(stderr, "Error: close(%s) failed\n", tmp);
        return 1;
    }
    if (rename(tmp, dest) != 0) {
        unlink(tmp);
        fprintf(stderr, "Error: rename %s -> %s failed: %s\n",
                tmp, dest, strerror(errno));
        return 1;
    }
    if (fsync_parent_dir(dest) != 0)
        fprintf(stderr, "Warning: fsync of %s failed (stored, durability unconfirmed)\n",
                dest_dir);
    OUT("{\"status\":\"stored\",\"path\":\"%s\"}\n", dest);
    return 0;
}
```

**6b. `cmd_put_file_b64` — fsync checked + dir fsync.** Replace

```c
    fsync(fd);
    close(fd);
    free(raw);

    if (rename(tmp, dest) != 0) {
        unlink(tmp);
        OUT("{\"error\":\"rename failed\"}\n");
        return 1;
    }

    OUT("{\"status\":\"stored\",\"filename\":\"%s\",\"bytes\":%zu}\n", filename, raw_len);
    return 0;
}
```

with

```c
    if (fsync(fd) != 0) {
        close(fd); unlink(tmp); free(raw);
        OUT("{\"error\":\"fsync failed\"}\n");
        return 1;
    }
    close(fd);
    free(raw);

    if (rename(tmp, dest) != 0) {
        unlink(tmp);
        OUT("{\"error\":\"rename failed\"}\n");
        return 1;
    }
    if (fsync_parent_dir(dest) != 0) {
        OUT("{\"error\":\"stored but durability unconfirmed\"}\n");
        return 1;
    }

    OUT("{\"status\":\"stored\",\"filename\":\"%s\",\"bytes\":%zu}\n", filename, raw_len);
    return 0;
}
```

**6c. `cmd_delete_file` — dir fsync.** Replace

```c
    if (unlink(dest) != 0) {
        if (errno == ENOENT)
            OUT("{\"error\":\"file not found\",\"filename\":\"%s\"}\n", filename);
        else
            OUT("{\"error\":\"unlink failed: %s\",\"filename\":\"%s\"}\n",
                strerror(errno), filename);
        return 1;
    }

    OUT("{\"status\":\"deleted\",\"filename\":\"%s\"}\n", filename);
    return 0;
}
```

with

```c
    if (unlink(dest) != 0) {
        if (errno == ENOENT)
            OUT("{\"error\":\"file not found\",\"filename\":\"%s\"}\n", filename);
        else
            OUT("{\"error\":\"unlink failed: %s\",\"filename\":\"%s\"}\n",
                strerror(errno), filename);
        return 1;
    }
    /* Same fail-closed rule as the marker clear: a failed directory sync
       means the deletion's durability is unconfirmed — report it. */
    if (fsync_parent_dir(dest) != 0) {
        OUT("{\"error\":\"deleted but durability unconfirmed\",\"filename\":\"%s\"}\n",
            filename);
        return 1;
    }

    OUT("{\"status\":\"deleted\",\"filename\":\"%s\"}\n", filename);
    return 0;
}
```

(`fsync_parent_dir` is declared in `types.h`; query_find.c already sees
types.h.)

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-file-ops-roundtrip
./build/bin/shard-db-test run-all
```

---

## Task 7 — BULK_COMMIT_WINDOW default 1024 → 4096

**Root cause.** With per-record and per-slot syncing gone (Tasks 2–3), the
remaining per-window fixed costs (marker publish/clear, per-touched-file
flushes, plan overhead) dominate at small windows; 4096 amortizes them 4×.
Legal range is already `[16, 16384]` (`parse_bulk_commit_window`), so no
parser change. Lands last so its effect is measurable against Task 1's
phase metrics instead of masking the earlier fixes.

Trade-off accepted (and to be noted in the changelog): each window holds
the kf shard wrlock for its M..C span, so a 4096-record window holds it
~4× longer than a 1024-record window — total lock time across a batch is
unchanged, but concurrent single-key ops on the same shard see longer
individual stalls; window staging memory (marker buffer with key+old+new
per record, slot vectors, index staging) grows ~4× per bulk worker.

### Test first — `src/test/cases/test_bulk_commit_window_default.c` (new file)

Red on base: default resolves to 1024. Modeled on the existing
`test-bulk-commit-window-config` fixture pattern.

```c
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static int write_plain_env(const char *dir) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/db.env", dir);
    FILE *f = fopen(path, "w");
    if (!f) return -1;
    fprintf(f, "DB_ROOT=/tmp/ignored\n");   /* no BULK_COMMIT_WINDOW */
    return fclose(f);
}

static int test_bulk_commit_window_default_run(void) {
    char fixture[] = "/tmp/shard-db-win-default-XXXXXX";
    char cwd[PATH_MAX], parsed_root[PATH_MAX];
    char *cwd_ok = getcwd(cwd, sizeof(cwd));
    ASSERT_NOT_NULL(cwd_ok, "capture cwd");
    if (!cwd_ok) return 1;
    char *fixture_ok = mkdtemp(fixture);
    ASSERT_NOT_NULL(fixture_ok, "create fixture");
    if (!fixture_ok) return 1;
    int saved = g_db->bulk_commit_window;
    chdir(fixture);
    ASSERT_EQ_INT(write_plain_env(fixture), 0, "write env without the knob");
    ASSERT_EQ_INT(load_db_root(parsed_root, sizeof(parsed_root)), 0,
                  "env parses");
    ASSERT_EQ_INT(g_db->bulk_commit_window, 4096,
                  "default commit window is 4096 (RED on base: 1024)");
    g_db->bulk_commit_window = saved;
    chdir(cwd);
    rmrf(fixture);
    return t_ctx->failed ? 1 : 0;
}
TEST_REGISTER("test-bulk-commit-window-default", test_bulk_commit_window_default_run)
```

### Changes

1. `src/db/embedded.c` — replace
   ```c
       db->bulk_commit_window        = 1024;
   ```
   with
   ```c
       db->bulk_commit_window        = 4096;
   ```
2. `src/db/slotcask.c` — replace both window_cap fallbacks (they are
   textually identical; use replace-all):
   ```c
       txn.window_cap = db->bulk_commit_window > 0
                       ? (size_t)db->bulk_commit_window : 1024;
   ```
   →
   ```c
       txn.window_cap = db->bulk_commit_window > 0
                       ? (size_t)db->bulk_commit_window : 4096;
   ```
3. `src/db/slotcask.h` — replace
   ```c
       int     bulk_commit_window; /* records per commit window; 0 = default
                                      (1024). db.env BULK_COMMIT_WINDOW. */
   ```
   with
   ```c
       int     bulk_commit_window; /* records per commit window; 0 = default
                                      (4096). db.env BULK_COMMIT_WINDOW. */
   ```
4. `db.env` — replace
   ```c
   export BULK_COMMIT_WINDOW=1024
   ```
   with
   ```c
   export BULK_COMMIT_WINDOW=4096
   ```
5. `docs/getting-started/configuration.md` — in the row beginning
   `| \`BULK_COMMIT_WINDOW\` | \`1024\` |`, change the default cell to
   `4096` and append to the description: "Default 4096 since 2026.09.x
   (was 1024); with per-window batched index/kf syncing, larger windows
   mainly amortize marker publication costs. Shard wrlock hold per window
   grows proportionally."

### Verify

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-bulk-commit-window-default
./build/bin/shard-db-test run test-bulk-commit-window-config
./build/bin/shard-db-test run-all
```

---

## Task 8 — Docs, changelog, full gates, handoff

1. `docs/reference/changelog.md` — under the `## Unreleased` heading, add
   one entry covering: batched index flushing for indexed bulk
   insert/update/delete (per-window, per-unique-file), bitmap windows now
   synced before marker clear (durability fix), single whole-mapping kf
   sync per window, marker publish via rename, durable sequences (incl.
   locked reset) with fail-closed allocation, durable put-file/delete-file,
   per-phase durability metrics in `stats`, `BULK_COMMIT_WINDOW` default
   4096.
2. `docs/concepts/concurrency.md` — if it quotes the per-slot kf sync or
   per-record index sync behavior, update to the batched description;
   otherwise leave.
3. `AGENTS.md` — no changes required (it pins no window value or sync
   granularity).
4. Full local gate, in order, all fresh:
   ```bash
   SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all
   BUILD_MODE=asan SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all   # ×3
   BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh && TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all   # ×3
   ```
5. Leave everything uncommitted. Final summary must include: `git rev-parse
   main` at branch point, `git diff --stat`, the three fresh sanitizer run
   outcomes, and a reminder for the human to run
   `./build/bin/shard-db-bench bench-invoice` against the recorded main
   baseline (expected movement: "BULK INSERT 14 indexes" wall and
   "BULK UPDATE/DELETE x10000" walls drop sharply; `stats` →
   `commit.index_sync_us_total` / `commit.segment_sync_us_total` /
   `commit.sync_us_total` now attribute where the remaining time goes).

---

## Risk register

- **Touch under-recording** is the one way this plan could break durability:
  every mutation site must record a touch (or keep its own sync). Mitigated
  by: recordings co-located with the mutation calls they describe (2c/2g/2h),
  the note-sync(I) seam firing on any non-empty touch set, and the
  existing msync/failure-injection suites.
- **Touch over-recording** costs one extra fdatasync and — via
  `bt_acquire(writer)` O_CREAT — an empty index file at worst; the trigram
  `mutated` counters and the "only after old_key||new_key" guards keep this
  rare.
- **Longer shard wrlock holds** (Task 7) are called out in the changelog;
  `BULK_COMMIT_WINDOW` stays operator-tunable for anyone who needs the old
  stall profile (`BULK_COMMIT_WINDOW=1024` restores today's behavior
  exactly).
- **Existing tests that assert note-sync behavior** may newly fire phase-I
  injections now that touch sets are non-empty; any such failure is a test
  observing the *intended* new behavior — fix the test's expectation, not
  the batching.
