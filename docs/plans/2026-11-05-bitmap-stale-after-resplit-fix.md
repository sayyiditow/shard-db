# Plan: keep bitmap indexes correct across keyfile rebuilds (resplit + vacuum compaction)

Date: 2026-11-05. Planner: Claude Sonnet 5.5. Executor: whichever model the human designates (do not assume).
Write-up of the bug: `docs/plans/2026-11-04-bitmap-stale-after-resplit.md`.
Decision (human, 2026-11-05): **remap in the resplit + durable marker + startup rebuild** (option 1 of the write-up).

## Execution rules (embedded — follow literally)

- Branch: work directly on `fix/type-correctness-recovery` (the single long-lived branch for all of this work until it is merged; do **not**
  create a sub-branch; confirm with `git branch --show-current` and that HEAD is `faa8340` or later). Never commit to `main`. **Leave all work uncommitted** (AGENTS.md); the reviewing agent and
  human review the raw `git diff` first. Git write operations only on an explicit human request.
- Do the tasks in order. Paste real command output for every build/test step; never claim a step passed without it.
- If a quoted anchor is not found **exactly once**, write `PLAN_NOTES-bmfix.md` describing the mismatch and **halt the entire run immediately** —
  do not guess, reinterpret, or continue to any later task. Resuming requires the human (or the planning model) to read the note and hand back a
  patched or fresh plan.
- If you hit a decision this plan does not cover, stop and ask. Do not improvise. Never weaken or skip a test to make a failure disappear.
  A test that fails then passes on rerun is a bug until root-caused.
- Build: `SKIP_TESTS=1 ./build.sh`. Tests: `./build/bin/shard-db-test run <name>` (single) and
  `./build/bin/shard-db-test run-all --jobs 4` (full). `rm -rf /tmp/shard-db-*` after **every** test run (tmpfs). **Do not run benches.**
- Sanitizers (ASan+UBSan, TSan) run **once**, in Task 9, after all fixes and the full normal suite are done. Not between rounds.

## Root cause (verified in source, not inferred)

A bitmap bit `i` of shard `s` means "kf slot `i` of kf shard `s` holds a record with this value" (`bitmap.h` header comment; readers do
`bm_walk` → `kf_map[slot].hash` in `bm_emit_cb`, `query.c`). `kfcache_resplit_locked` (`src/db/slotcask.c`) builds a fresh keyfile and, in
"Phase B: probe-rebuild loop", re-inserts every live entry at `kf_slot_for(hash, new_cap)` — **entries move to different slot numbers** whenever
`new_cap` differs, and also on a same-capacity rebuild because tombstones are dropped and probe chains shift. Nothing renumbers the existing
bitmap bits: the live-write path only calls `bm_grow()` for a *new* record's slot (`index.c`: "Auto-grow on slot overflow",
`bitmap_apply_grow_for_slot`), which only zero-extends. Rebuild paths (`reindex`, recompose) recompute bits from current slots, which is why they
repair it. Result: after any keyfile rebuild, bits name wrong slots → silent under-count (762 of 1714 measured).

`kfcache_resplit_locked` is the single place every keyfile rebuild goes through. Fixing it fixes growth **and** vacuum.

### Consumers / call sites (complete list from `grep`)

`kfcache_resplit_locked` is called from: `slotcask_pregrow_worker` ("while (kh.capacity < kf_max_slots()" loop near "Pre-grow worker"),
`kf_plan_insert_slot`, `kf_plan_window_insert_slot`, the bulk stage pre-grow ("Pre-grow is folded into this shard's pipeline stage"), and
`slotcask_compact_kf` (same-capacity rebuild; caller `cmd_vacuum` in `query_maint.c`, under the exclusive objlock). **No signature of any of
these changes** (the new code is internal to `kfcache_resplit_locked`). Callers already treat a non-zero return as "did not grow" (`if (... == 0)`
or `break`), which is the contract the new abort path relies on.

Bitmap file consumers (unchanged, but they define the lock contract): `bm_open` readers in `query.c`, `query_aggregate.c`, `query_plan.c`;
writers in `index.c` (`bitmap_update`, `bitmap_prepare_*`), `query_schema.c` (build), `slotcask.c` (request flush syncs by path).
`docs/concepts/concurrency.md` "Cross-cache rule: kfcache before bitmap cache" already mandates kf entry lock first, bitmap entry lock second;
the new code (kf wrlock held → bm wrlock) follows that rule and adds no new edge. No path may take the bitmap lock first.

New on-disk artifacts: `<obj>/indexes/.bmfix-NNN` (empty marker, `NNN` = `%03x` shard) and transient `<obj>/indexes/<field>/NNN.bm.remap`.
No existing file format changes (kf, bm, markers, schema untouched) → **no version bump, no migration**. A binary without this fix simply
ignores the extra files.

## Design

Inside `kfcache_resplit_locked`, between "Phase B" and the msync/rename:

1. **Prepare (abortable).** For each `<obj>/indexes/<field>/<NNN>.bm` of this shard: take its bitmap writer handle (`bm_open(...,writer=1)`, lock
   order kf→bm), write a permuted copy `NNN.bm.remap` (slot space = `new_cap`), fsync it. For each old set bit `i` the new slot is found by probing
   the *new* keyfile for the entry's hash (no extra allocation; the new table is still mapped). Any failure → discard temps, release handles,
   unlink `kf.new`, return `-1`: the old keyfile and bitmaps are untouched and consistent.
2. **Marker.** After all temps exist, write+fsync `<obj>/indexes/.bmfix-NNN` and fsync the directory. From here a crash is recoverable.
3. Existing msync + rename of the keyfile (unchanged).
4. **Commit.** Immediately after the keyfile rename+dir-fsync: rename each `.remap` over its `.bm` in place (`bm_publish`, keeps the cache entry
   coherent), fsync the field dir, unlink the marker, fsync `indexes/`, release handles. All still under the kf wrlock, so no reader observes the
   mixed state (readers take kf rdlock before bm rdlock; count-only readers block on the bm wrlock).
5. **Startup recovery.** If a crash (or a commit I/O error that was logged) leaves a `.bmfix-*` marker, `shard_db_recover_before_stamp` — after KFM2
   replay — rebuilds all indexes of that object with the existing strict `reindex_object_durable_checked_ex`, then deletes markers and `.remap`
   leftovers. Correct regardless of which half of the sequence was durable (rebuild is a pure function of kf + segments).

Residual risk (stated, accepted): if a bitmap `rename` fails *after* the keyfile rename (I/O error, ENOSPC on rename is not possible; EIO/EROFS),
the running daemon serves stale bitmaps for that shard until restart. This is logged at ERROR (`KF_BMFIX_FAILED`), the marker is retained, and
the next start repairs it. It requires a failing filesystem and is strictly better than today.

Performance note (human runs benches): the remap adds one random probe into the new keyfile per set bit per bitmap field per resplit, only for
objects that have `.bm` files. Objects without bitmap indexes pay one failed `opendir`. The existing "streaming resplit, zero malloc" property is
kept (only a per-field bookkeeping array of `MAX_FIELDS` entries is allocated, lazily at the first `.bm` file found, so btree-only objects never allocate).

## Invariants and edge cases (executor must preserve / test)

1. After any keyfile rebuild, for every live record `r` at kf slot `s` and every bitmap field `f` with a non-NULL value `v`: bit `s` of value `v`'s
   bitmap is set, and no other bit is set for `r`. Bits of dead slots are dropped.
2. New bitmap `slots` header == `new_cap` (the bitmap never lags the keyfile after a rebuild).
3. Same-capacity rebuild (vacuum, `new_cap == old_cap`) remaps too.
4. Bits at positions ≥ old `hdr.slots` (padding bits in the last byte) are ignored (matches `bm_walk`).
5. A `.bm` that does not exist for this shard (btree field dirs, composite dirs, shard never written) is skipped. Names starting with `.` skipped.
6. `new_cap > UINT32_MAX / 2` with at least one bitmap → fail the prepare (errno `EOVERFLOW`) → resplit aborts, nothing changes.
7. Bool fast-path files (`BM_FLAG_BOOL_FASTPATH`, dict `[01 00 00][01 00 01]`) and dictionary padding between `dict_off` and `bitmaps_off`
   are copied byte-for-byte; `bitmaps_off`, `dict_off`, `n_values`, `max_values`, `flags` unchanged.
8. Corrupt bitmap (stride mismatch, file shorter than header says) → prepare fails with `EUCLEAN`, resplit aborts, nothing changes.
9. Lock order kf wrlock → bm wrlock only. The calling thread must not already hold a bitmap writer handle for the same shard when a resplit can
   run (verified in Task 0; pre-grow in the bulk stage runs before any prepare, single insert plans the slot before `bitmap_prepare_set_add`).
10. `cmd_vacuum` and `slotcask_compact_kf` run under the exclusive objlock → no concurrent bitmap holders.
11. Recovery runs **after** KFM2 marker replay, for clean and non-clean starts (a marker can survive a clean stop after a logged commit failure).

---

## Task 0 — Preconditions and lock-order audit (read-only; paste output)

- [ ] 0.1 `git status --short` and `git log --oneline -3`: expect HEAD `faa8340`, only the four known untracked files (`HANDOFF-bitmap-fix.md`,
  `docs/plans/2026-11-04-r6-evidence.md`, this plan `docs/plans/2026-11-05-bitmap-stale-after-resplit-fix.md`, and the sibling plan `docs/plans/2026-11-06-bulk-chain-bitmap-sync-path.md`). Anything else → stop and ask. Confirm the branch per the execution rules (no new branch).
- [ ] 0.2 Confirm each quoted anchor used below occurs exactly once (any mismatch → `PLAN_NOTES-bmfix.md`, halt):
  ```bash
  grep -c 'static int kfcache_resplit_locked(SlotcaskKfHandle \*kh, size_t new_cap) {' src/db/slotcask.c        # 1
  grep -c 'new_hdr->total = live_copied;' src/db/slotcask.c                                                       # 1
  grep -c 'Phase D: rename + parent-dir fsync' src/db/slotcask.c                                                   # 1
  grep -c 'uint64_t t_after_rename = kf_now_us();' src/db/slotcask.c                                               # 1
  grep -c '^/\* Rebuild every kf shard in place to drop tombstones' src/db/slotcask.c                              # 1
  grep -c '^static int fsync_dir(const char \*dir_path) {' src/db/slotcask.c                                       # 1
  grep -c '^int  bm_grow(BitmapShard \*bm, uint32_t new_slots);' src/db/bitmap.h                                   # 1
  grep -c '^/\* ─────────────────────── observability' src/db/bitmap.c                                             # 1
  grep -c '^int  slotcask_compact_kf(SlotcaskDb \*db);' src/db/slotcask.h                                          # 1
  grep -c '^int shard_db_recover_before_stamp' src/db/embedded.c                                                   # 1
  grep -c 'Replay completed against .BTRI. — only now is the migration stamp' src/db/embedded.c                    # 1
  grep -c 'src/test/cases/test_predicate_equivalence.c' build.sh                                                   # >=1
  ```
- [ ] 0.3 Lock-order / ownership audit. Run and paste:
  ```bash
  grep -n 'kfcache_resplit_locked' src/db/*.c
  grep -n 'bm_open(' src/db/*.c | grep -v 'bm_open(bp, 0, 0, 0, 0, 0'
  ```
  Confirm and write one line each in your hand-back report:
  (a) every `kfcache_resplit_locked` caller holds the kf **writer** handle (`kh.writer`; the function returns -1 otherwise);
  (b) in `kf_plan_insert_slot`/`kf_plan_window_insert_slot` the resplit precedes `bitmap_prepare_set_add` for the same record
  (`storage.c`: the prepare call sits after `c->kf_slot` is fixed) and the bulk stage pre-grow (`while (kh.capacity < kf_max_slots()`) runs
  before any window is prepared;
  (c) no code path calls `kfcache_acquire(... writer ...)` or `kfcache_resplit_locked` while holding a bitmap handle it obtained **before**
  acquiring the kf entry. The bitmap-reader sites listed by `grep` without a nearby `kfcache_acquire` are popcount/count-only paths that never
  touch kf, so they add no bm→kf edge.
  If (a)–(c) cannot be confirmed → `PLAN_NOTES-bmfix.md` and halt (a lock-order change needs the human).

## Task 1 — Failing tests first (red on the unmodified base)

- [ ] 1.1 Create `src/test/cases/test_bitmap_resplit.c` with exactly this content:

```c
/* Bitmap bits are kf slot numbers. Every keyfile rebuild -- in-place growth
   resplit (single insert, bulk window) and vacuum's tombstone compaction --
   renumbers slots, so it must renumber the bits with it. Each case compares
   count and find against a C-side oracle for every bitmap value, with the kf
   tier shrunk to 64 slots/shard so growth happens while earlier rows' bits
   already exist. Each case runs in its own daemon and tmpdir. */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BR_N 3000

typedef struct { int flag; int kind; int live; } BrRow;
static const char *const br_kind[] = { "a", "b", "c" };

typedef struct {
    TestEnv env;
    TestClient *tc;
    BrRow rows[BR_N];
} Br;

static char *br_q(Br *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));
static char *br_q(Br *b, const char *fmt, ...) {
    char req[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(req, sizeof(req), fmt, ap);
    va_end(ap);
    char *resp = NULL;
    if (tc_request(b->tc, req, &resp) != 0) { free(resp); return NULL; }
    return resp;
}

static int br_connect(Br *b) {
    TestClientCfg cfg = { .port = b->env.port, .io_timeout_ms = 120000 };
    b->tc = tc_connect(&cfg);
    return b->tc ? 0 : -1;
}

static int br_open(Br *b, int legacy) {
    memset(b, 0, sizeof(*b));
    for (int i = 0; i < BR_N; i++) {
        b->rows[i].flag = (i % 3 == 0) || (i % 7 == 0);
        b->rows[i].kind = i % 3;
    }
    if (test_env_start(&b->env) != 0) return -1;
    if (test_env_ctl_set_kf_seams(&b->env, 64, 0) != 0 || br_connect(b) != 0) {
        test_env_stop(&b->env);
        return -1;
    }
    char *r = br_q(b, "{\"mode\":\"add-dir\",\"dir\":\"br\"}");
    free(r);
    r = br_q(b,
        "{\"mode\":\"create-object\",%s\"dir\":\"br\",\"object\":\"t\",\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"flag:bool\",\"kind:enum(a,b,c)\",\"n:int\"],"
        "\"indexes\":[\"flag:bitmap\",\"kind:bitmap\",\"n\"]}",
        legacy ? "\"declaration_layout\":\"legacy\"," : "");
    ASSERT_CONTAINS(r, "\"status\":\"created\"", "create object");
    free(r);
    return 0;
}

static void br_close(Br *b) {
    if (b->tc) tc_close(b->tc);
    b->tc = NULL;
    test_env_stop(&b->env);
}

static int br_bulk(Br *b, int from, int to) {
    size_t cap = (size_t)(to - from) * 128 + 256;
    char *req = malloc(cap);
    if (!req) return -1;
    size_t n = (size_t)snprintf(req, cap,
        "{\"mode\":\"bulk-insert\",\"dir\":\"br\",\"object\":\"t\",\"records\":[");
    for (int i = from; i < to; i++) {
        n += (size_t)snprintf(req + n, cap - n,
            "%s{\"key\":\"k%05d\",\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
            i > from ? "," : "", i, b->rows[i].flag ? "true" : "false",
            br_kind[b->rows[i].kind], i);
        b->rows[i].live = 1;
    }
    snprintf(req + n, cap - n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    int ok = rc == 0 && resp && !strstr(resp, "\"error\"");
    free(resp);
    return ok ? 0 : -1;
}

static int br_single(Br *b, int i) {
    char *r = br_q(b,
        "{\"mode\":\"insert\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k%05d\","
        "\"value\":{\"flag\":%s,\"kind\":\"%s\",\"n\":%d}}",
        i, b->rows[i].flag ? "true" : "false", br_kind[b->rows[i].kind], i);
    int ok = r && strstr(r, "\"status\":\"inserted\"");
    free(r);
    if (ok) b->rows[i].live = 1;
    return ok ? 0 : -1;
}

static int br_bulk_delete(Br *b, int step) {
    char *req = malloc((size_t)BR_N * 16 + 256);
    if (!req) return -1;
    size_t n = (size_t)sprintf(req,
        "{\"mode\":\"bulk-delete\",\"dir\":\"br\",\"object\":\"t\",\"keys\":[");
    int first = 1;
    for (int i = 0; i < BR_N; i += step) {
        if (!b->rows[i].live) continue;
        n += (size_t)sprintf(req + n, "%s\"k%05d\"", first ? "" : ",", i);
        first = 0;
        b->rows[i].live = 0;
    }
    sprintf(req + n, "]}");
    char *resp = NULL;
    int rc = tc_request(b->tc, req, &resp);
    free(req);
    int ok = rc == 0 && resp && !strstr(resp, "\"error\"");
    free(resp);
    return ok ? 0 : -1;
}

static int br_count(TestClient *tc, const char *crit) {
    char req[1024];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"br\",\"object\":\"t\",\"criteria\":%s}", crit);
    char *r = NULL;
    int rc = tc_request(tc, req, &r);
    int n = rc == 0 && r ? tu_parse_count(r) : -1;
    free(r);
    return n;
}

static int br_find_total(TestClient *tc, const char *crit) {
    int total = 0;
    for (int off = 0;; off += 1000) {
        char req[1024];
        snprintf(req, sizeof(req),
            "{\"mode\":\"find\",\"dir\":\"br\",\"object\":\"t\",\"criteria\":%s,"
            "\"offset\":%d,\"limit\":1000,\"fields\":[\"n\"]}", crit, off);
        char *r = NULL;
        if (tc_request(tc, req, &r) != 0 || !r || strstr(r, "\"error\"")) { free(r); return -1; }
        int got = 0;
        for (const char *p = r; (p = strstr(p, "\"key\":\"")) != NULL; p += 7) got++;
        free(r);
        total += got;
        if (got < 1000) break;
    }
    return total;
}

static void br_expect(TestClient *tc, const char *when, const char *label, const char *crit, int want) {
    char desc[200];
    snprintf(desc, sizeof(desc), "%s: count %s == oracle (%d)", when, label, want);
    ASSERT_EQ_INT(br_count(tc, crit), want, desc);
    snprintf(desc, sizeof(desc), "%s: find %s == oracle (%d)", when, label, want);
    ASSERT_EQ_INT(br_find_total(tc, crit), want, desc);
}

/* Every bitmap predicate against the C-side oracle: bool, enum, and a
   bitmap AND bitmap intersect. */
static void br_check_tc(Br *b, TestClient *tc, const char *when) {
    int ft = 0, ff = 0, k[3] = { 0, 0, 0 }, both = 0;
    for (int i = 0; i < BR_N; i++) {
        if (!b->rows[i].live) continue;
        if (b->rows[i].flag) ft++; else ff++;
        k[b->rows[i].kind]++;
        if (b->rows[i].flag && b->rows[i].kind == 1) both++;
    }
    br_expect(tc, when, "flag=true",  "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]", ft);
    br_expect(tc, when, "flag=false", "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"false\"}]", ff);
    br_expect(tc, when, "kind=a", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"a\"}]", k[0]);
    br_expect(tc, when, "kind=b", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]", k[1]);
    br_expect(tc, when, "kind=c", "[{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"c\"}]", k[2]);
    br_expect(tc, when, "flag=true AND kind=b",
        "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"},{\"field\":\"kind\",\"op\":\"eq\",\"value\":\"b\"}]",
        both);
}
static void br_check(Br *b, const char *when) { br_check_tc(b, b->tc, when); }

/* Bulk windows: growth happens inside later chunks while earlier chunks' bits exist. */
static int br_run_bulk(int legacy) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, legacy) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    br_check(b, "after growth");
    ASSERT_EQ_INT(br_bulk_delete(b, 5), 0, "bulk-delete every 5th");
    br_check(b, "after delete");
    for (int i = 0; i < 200; i++) {            /* flip kind on live rows via update */
        if (!b->rows[i].live) continue;
        b->rows[i].kind = (b->rows[i].kind + 1) % 3;
        char *r = br_q(b,
            "{\"mode\":\"update\",\"dir\":\"br\",\"object\":\"t\",\"key\":\"k%05d\","
            "\"value\":{\"kind\":\"%s\"}}", i, br_kind[b->rows[i].kind]);
        ASSERT_CONTAINS(r, "\"status\":\"updated\"", "update kind");
        free(r);
    }
    br_check(b, "after update");
    for (int i = 0; i < BR_N; i += 5) b->rows[i].live = 0;     /* re-insert the deleted rows */
    ASSERT_EQ_INT(br_bulk(b, 0, 1), 0, "re-insert k00000");
    for (int i = 5; i < BR_N; i += 5) ASSERT_EQ_INT(br_single(b, i), 0, "re-insert single");
    br_check(b, "after re-insert");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}
static int test_bitmap_resplit_bulk(void)   { return br_run_bulk(0); }
static int test_bitmap_resplit_legacy(void) { return br_run_bulk(1); }

/* Single-record inserts: kf_plan_insert_slot resplits with older rows' bits present. */
static int test_bitmap_resplit_single(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int i = 0; i < 1200; i++)
        ASSERT_EQ_INT(br_single(b, i), 0, "single insert");
    br_check(b, "after single-insert growth");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* vacuum rebuilds every kf shard with deleted > 0 at the same capacity. */
static int test_bitmap_resplit_vacuum(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_bulk_delete(b, 2), 0, "bulk-delete every 2nd");
    br_check(b, "before vacuum");
    char *r = br_q(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}");
    ASSERT_CONTAINS(r, "\"status\":\"vacuumed\"", "light vacuum (compacts kf)");
    free(r);
    br_check(b, "after vacuum");
    ASSERT_EQ_INT(br_bulk(b, 0, 300), 0, "insert after vacuum");
    br_check(b, "after insert following vacuum");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int br_restart(Br *b) {
    int port = b->env.port;
    char root[256];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    if (test_env_start_at(&b->env, root, port) != 0) return -1;
    return br_connect(b);
}

/* Remapped bitmaps are durable: same answers after a clean restart. */
static int test_bitmap_resplit_restart(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_restart(b), 0, "restart daemon on the same root");
    br_check(b, "after restart");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Crash window: marker present + shard-0 bitmap body zeroed (stands for "keyfile
   renamed, bitmap not yet published"). Startup must rebuild and clear the marker. */
static int test_bitmap_resplit_marker_recovery(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    for (int from = 0; from < BR_N; from += 500)
        ASSERT_EQ_INT(br_bulk(b, from, from + 500), 0, "bulk-insert chunk");
    int port = b->env.port;
    char root[256], bm[600], marker[600];
    snprintf(root, sizeof(root), "%s", b->env.db_root);
    tc_close(b->tc);
    b->tc = NULL;
    test_env_stop_keep(&b->env);
    snprintf(bm, sizeof(bm), "%s/br/t/indexes/flag/000.bm", root);
    snprintf(marker, sizeof(marker), "%s/br/t/indexes/.bmfix-000", root);
    ASSERT_TRUE(tu_file_exists(bm), "flag shard-0 bitmap exists");
    /* bool bitmap: 32-byte header + 6-byte dict, bitmaps_off aligned to 40 */
    ASSERT_EQ_INT(tu_run_cmd("sz=$(stat -c%%s '%s'); dd if=/dev/zero of='%s' bs=1 seek=40 "
                             "count=$((sz-40)) conv=notrunc 2>/dev/null", bm, bm), 0, "zero the bitmap body");
    ASSERT_EQ_INT(tu_run_cmd(": > '%s'", marker), 0, "plant the marker");
    ASSERT_EQ_INT(test_env_start_at(&b->env, root, port), 0, "restart with a pending marker");
    ASSERT_EQ_INT(br_connect(b), 0, "reconnect");
    br_check(b, "after marker recovery");
    ASSERT_TRUE(!tu_file_exists(marker), "marker removed after recovery");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* Readers race resplits: no deadlock (test timeout), no error responses, exact
   answers at the end. Also the TSan-relevant case in Task 9. */
typedef struct { int port; volatile int stop; int errors; int reads; } BrReader;
static void *br_reader_main(void *arg) {
    BrReader *rd = arg;
    TestClientCfg cfg = { .port = rd->port, .io_timeout_ms = 120000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) { rd->errors++; return NULL; }
    while (!rd->stop) {
        int c = br_count(tc, "[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]");
        if (c < 0) rd->errors++;
        rd->reads++;
    }
    tc_close(tc);
    return NULL;
}
static int test_bitmap_resplit_concurrent_readers(void) {
    Br *b = calloc(1, sizeof(*b));
    ASSERT_NOT_NULL(b, "alloc");
    if (!b) return 1;
    if (br_open(b, 0) != 0) { free(b); return 1; }
    BrReader rd = { .port = b->env.port };
    pthread_t th;
    ASSERT_EQ_INT(pthread_create(&th, NULL, br_reader_main, &rd), 0, "start reader");
    for (int from = 0; from < BR_N; from += 250)
        ASSERT_EQ_INT(br_bulk(b, from, from + 250), 0, "bulk-insert chunk");
    ASSERT_EQ_INT(br_bulk_delete(b, 3), 0, "bulk-delete every 3rd");
    char *r = br_q(b, "{\"mode\":\"vacuum\",\"dir\":\"br\",\"object\":\"t\"}");
    free(r);
    rd.stop = 1;
    pthread_join(th, NULL);
    ASSERT_EQ_INT(rd.errors, 0, "concurrent readers saw no error response");
    ASSERT_TRUE(rd.reads > 0, "reader made progress");
    br_check(b, "after concurrent load");
    br_close(b);
    free(b);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bitmap-resplit-bulk", test_bitmap_resplit_bulk)
TEST_REGISTER("test-bitmap-resplit-legacy", test_bitmap_resplit_legacy)
TEST_REGISTER("test-bitmap-resplit-single", test_bitmap_resplit_single)
TEST_REGISTER("test-bitmap-resplit-vacuum", test_bitmap_resplit_vacuum)
TEST_REGISTER("test-bitmap-resplit-restart", test_bitmap_resplit_restart)
TEST_REGISTER("test-bitmap-resplit-marker-recovery", test_bitmap_resplit_marker_recovery)
TEST_REGISTER("test-bitmap-resplit-concurrent-readers", test_bitmap_resplit_concurrent_readers)
```

  (`rd.stop`/`rd.errors` are plain ints touched by two threads in a test; make `stop` `volatile` as written and `errors/reads` are read only
  after `pthread_join`, so this is race-free under TSan. If TSan flags `rd.stop` in Task 9, change it to `_Atomic int` — a test-side fix.)

- [ ] 1.2 Register in `build.sh`: find the line `    src/test/cases/test_predicate_equivalence.c \` (exactly once inside the `shard-db-test` gcc list; confirm with
  `grep -n 'test_predicate_equivalence.c' build.sh`) and add on the next line `    src/test/cases/test_bitmap_resplit.c \`. If `build.sh`
  keeps a second list (e.g. a sanitizer/TSan variant) that names test files, add it there too.
- [ ] 1.3 Restore the repro in the existing R6 guard. In `src/test/cases/test_predicate_equivalence.c` replace the single line
  `    /* NOTE: no kf-tier seam here -- bitmaps are keyed by kf slot and go stale after a kf resplit (known issue, tracked separately) */`
  with
  `    ASSERT_EQ_INT(test_env_ctl_set_kf_seams(&env, 64, 0), 0, "shrink the kf tier to force growth");`
  (it sits between `test_env_start` and `TestClientCfg cfg`). This is the only edit to that file; do not touch its opening comment.
- [ ] 1.4 Build and run **red** on the unmodified engine (paste output):
  ```bash
  SKIP_TESTS=1 ./build.sh 2>&1 | tail -5
  for t in bulk legacy single vacuum restart marker-recovery concurrent-readers; do ./build/bin/shard-db-test run test-bitmap-resplit-$t 2>&1 | grep -E '^(not ok|# bitmap|# Failed|1\.\.)' | head -8; done
  ./build/bin/shard-db-test run test-predicate-equivalence 2>&1 | grep -c '^not ok'
  rm -rf /tmp/shard-db-*
  ```
  Expected: every `test-bitmap-resplit-*` case reports `not ok` assertions (under-counts), and `test-predicate-equivalence` reports ≈16
  failing assertions (the write-up's number). **If any new case is green on the base, it does not prove the fix: stop, write `PLAN_NOTES-bmfix.md`
  (case name + why), and halt.** Paste the failing counts (they become the "before" half of the Task 6 proof).

## Task 2 — Bitmap API: write, publish, discard a slot-remapped copy

- [ ] 2.1 `src/db/bitmap.h`: replace the line
  `int  bm_grow(BitmapShard *bm, uint32_t new_slots);`
  with
```c
int  bm_grow(BitmapShard *bm, uint32_t new_slots);

/* Keyfile-rebuild support. The keyfile resplit / compaction renumbers kf slots;
   bit i of a bitmap means "kf slot i", so the bits must move with the entries.
   bm_slotmap_prepare writes a copy of `bm`'s file to a sibling temporary
   (`<path>.remap`, name returned in tmp_path) in which every set bit `s` is
   moved to `map_fn(s, ctx)` and the slot space is resized to `new_slots`.
   map_fn returns UINT32_MAX to drop a bit (dead slot); a return >= new_slots is
   corruption (errno EUCLEAN). The live file and `bm` are not modified; the
   temporary is fsynced. `bm` must be a writer handle. Returns 0 / -1 (errno set,
   temporary already removed on failure). */
int  bm_slotmap_prepare(BitmapShard *bm, uint32_t new_slots,
                        uint32_t (*map_fn)(uint32_t old_slot, void *ctx), void *ctx,
                        char *tmp_path, size_t tmp_len);
/* Atomically replace the live file with the prepared temporary and re-map `bm`
   in place (cache entry stays coherent, like bm_grow), then fsync the parent
   directory. `bm` must still be the writer handle used for prepare. */
int  bm_slotmap_publish(BitmapShard *bm, const char *tmp_path);
/* Remove an unpublished temporary (safe on "" / NULL). */
void bm_slotmap_discard(const char *tmp_path);
```
- [ ] 2.2 `src/db/bitmap.c`: insert the following block immediately **before** the line
  `/* ─────────────────────── observability ─────────────────────── */`:

```c
/* ─────────────────────── slot remap (kf rebuild) ─────────────────────── */

int bm_slotmap_prepare(BitmapShard *bm, uint32_t new_slots,
                       uint32_t (*map_fn)(uint32_t old_slot, void *ctx), void *ctx,
                       char *tmp_path, size_t tmp_len) {
    if (!bm || !map_fn || !tmp_path || tmp_len == 0 || new_slots == 0 || !bm->writer) {
        errno = EINVAL;
        return -1;
    }
    int n = snprintf(tmp_path, tmp_len, "%s.remap", bm->path);
    if (n < 0 || (size_t)n >= tmp_len) {
        tmp_path[0] = '\0';
        errno = ENAMETOOLONG;
        return -1;
    }

    const struct BmHeader *oh = &bm->hdr;
    size_t old_end = (size_t)oh->bitmaps_off + (size_t)oh->n_values * (size_t)oh->stride;
    if (oh->stride != bm_stride_for(oh->slots) || old_end > bm->mmap_size ||
        oh->bitmaps_off < oh->dict_off) {
        tmp_path[0] = '\0';
        errno = EUCLEAN;
        return -1;
    }

    struct BmHeader nhdr = *oh;
    nhdr.slots = new_slots;
    nhdr.stride = bm_stride_for(new_slots);
    size_t total = bm_file_size(&nhdr);

    int saved = 0;
    int fd = -1;
    uint8_t *obuf = malloc(nhdr.stride);
    if (!obuf) { saved = ENOMEM; goto fail; }
    fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { saved = errno; goto fail; }
    if (ftruncate(fd, (off_t)total) != 0) { saved = errno; goto fail; }
    if (pwrite(fd, &nhdr, sizeof(nhdr), 0) != (ssize_t)sizeof(nhdr)) {
        saved = errno ? errno : EIO;
        goto fail;
    }
    /* Dictionary and its alignment padding pass through unchanged. */
    size_t dict_bytes = (size_t)oh->bitmaps_off - (size_t)oh->dict_off;
    if (dict_bytes &&
        pwrite(fd, (const uint8_t *)bm->mmap_ptr + oh->dict_off, dict_bytes,
               (off_t)nhdr.dict_off) != (ssize_t)dict_bytes) {
        saved = errno ? errno : EIO;
        goto fail;
    }
    for (uint32_t v = 0; v < oh->n_values; v++) {
        const uint8_t *src = (const uint8_t *)bm->mmap_ptr + oh->bitmaps_off +
                             (size_t)v * (size_t)oh->stride;
        memset(obuf, 0, nhdr.stride);
        for (uint32_t bi = 0; bi < oh->stride; bi++) {
            uint8_t byte = src[bi];
            while (byte) {
                uint32_t slot = bi * 8u + (uint32_t)__builtin_ctz(byte);
                byte &= (uint8_t)(byte - 1u);
                if (slot >= oh->slots) break;   /* padding bits, as bm_walk */
                uint32_t ns = map_fn(slot, ctx);
                if (ns == UINT32_MAX) continue;
                if (ns >= new_slots) { saved = EUCLEAN; goto fail; }
                obuf[ns >> 3] |= (uint8_t)(1u << (ns & 7u));
            }
        }
        if (pwrite(fd, obuf, nhdr.stride,
                   (off_t)nhdr.bitmaps_off + (off_t)v * (off_t)nhdr.stride)
                != (ssize_t)nhdr.stride) {
            saved = errno ? errno : EIO;
            goto fail;
        }
    }
    if (fsync(fd) != 0) { saved = errno; goto fail; }
    {
        int cfd = fd;
        fd = -1;
        if (close(cfd) != 0) { saved = errno; goto fail; }
    }
    free(obuf);
    return 0;

fail:
    if (fd >= 0) close(fd);
    unlink(tmp_path);
    free(obuf);
    tmp_path[0] = '\0';
    errno = saved ? saved : EIO;
    return -1;
}

int bm_slotmap_publish(BitmapShard *bm, const char *tmp_path) {
    if (!bm || !bm->writer || !tmp_path || !tmp_path[0]) {
        errno = EINVAL;
        return -1;
    }
    if (bm_publish(bm, tmp_path) != 0) return -1;
    /* Any other cached view of this path must re-validate its inode. */
    atomic_fetch_add_explicit(&g_bm_publish_generation, 1, memory_order_acq_rel);
    char parent[PATH_MAX];
    if (parent_dir_copy(bm->path, parent, sizeof(parent)) != 0) return -1;
    int dfd = open(parent, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) return -1;
    int rc = fsync(dfd);
    int saved = errno;
    close(dfd);
    if (rc != 0) { errno = saved; return -1; }
    return 0;
}

void bm_slotmap_discard(const char *tmp_path) {
    if (tmp_path && tmp_path[0]) unlink(tmp_path);
}

```
- [ ] 2.3 `SKIP_TESTS=1 ./build.sh 2>&1 | tail -15` — must compile with **no new warnings** (paste). `parent_dir_copy`, `PATH_MAX`, `O_DIRECTORY`,
  `atomic_fetch_add_explicit` are already used in `bitmap.c`; if one is undeclared the build says so — add only the missing `#include`.

## Task 3 — Keyfile rebuild: prepare → marker → commit inside `kfcache_resplit_locked`

- [ ] 3.1 `src/db/slotcask.c`: insert the following block immediately **before** the line
  `static int kfcache_resplit_locked(SlotcaskKfHandle *kh, size_t new_cap) {`
  (that line is the function definition, directly below `kf_now_us`; the block goes between them):

```c
/* ── Bitmap remap across a keyfile rebuild ──
   Bitmap bit i == "kf slot i" (bitmap.h). A keyfile rebuild moves entries to
   new slot numbers, so every <obj>/indexes/<field>/<shard>.bm of the rebuilt
   shard is rewritten with its bits renumbered. Sequence (all under the kf
   wrlock the caller holds; lock order kf -> bitmap, docs/concepts/concurrency.md):
     prepare  - per .bm: writer handle + fsynced "<shard>.bm.remap"; any failure
                aborts the whole rebuild with the old kf and bitmaps untouched.
     marker   - durable <obj>/indexes/.bmfix-<shard>; from here a crash is
                repaired at startup by a full reindex of the object.
     (kf msync + rename happen in kfcache_resplit_locked)
     commit   - rename each .remap over its .bm, fsync, drop the marker. */
typedef struct {
    BitmapShard *bm;
    char         tmp[PATH_MAX];
} KfBmFixEntry;

typedef struct {
    int           n;
    KfBmFixEntry *e;                    /* MAX_FIELDS entries, NULL when no bitmaps */
    char          indexes_dir[PATH_MAX];
    char          marker[PATH_MAX];     /* "" until the marker is durable */
} KfBmFix;

typedef struct {
    const SlotcaskKfEntry *old_map; size_t old_cap;
    const SlotcaskKfEntry *new_map; size_t new_cap;
} KfRemapCtx;

/* New slot of the live entry that sat at `old_slot`, found by probing the
   rebuilt table for its hash (Phase B placed every live entry). UINT32_MAX
   drops the bit: the old slot was not live. */
static uint32_t kf_remap_slot(uint32_t old_slot, void *vctx) {
    const KfRemapCtx *c = (const KfRemapCtx *)vctx;
    if ((size_t)old_slot >= c->old_cap) return UINT32_MAX;
    const SlotcaskKfEntry *src = &c->old_map[old_slot];
    if (src->flag != 1) return UINT32_MAX;
    size_t start = kf_slot_for(src->hash, c->new_cap);
    for (size_t j = 0; j < c->new_cap; j++) {
        size_t s = (start + j) % c->new_cap;
        if (c->new_map[s].flag == 0) break;
        if (memcmp(c->new_map[s].hash, src->hash, 16) == 0) return (uint32_t)s;
    }
    return UINT32_MAX;
}

/* "<obj>/data/kf/NNN.kf" -> "<obj>" and the decimal shard number. */
static int kf_bmfix_split_path(const char *kf_path, char *objdir, size_t objlen, int *shard) {
    static const char suffix[] = "/data/kf";
    const char *slash = strrchr(kf_path, '/');
    if (!slash) return -1;
    size_t dlen = (size_t)(slash - kf_path);
    size_t slen = sizeof(suffix) - 1;
    if (dlen <= slen || memcmp(kf_path + dlen - slen, suffix, slen) != 0) return -1;
    char tail = 0;
    if (sscanf(slash + 1, "%d.k%c", shard, &tail) != 2 || tail != 'f' || *shard < 0) return -1;
    size_t olen = dlen - slen;
    if (olen + 1 > objlen) return -1;
    memcpy(objdir, kf_path, olen);
    objdir[olen] = '\0';
    return 0;
}

static void kf_bmfix_abort(KfBmFix *fix) {
    for (int i = 0; i < fix->n; i++) {
        KfBmFixEntry *en = &fix->e[i];
        bm_slotmap_discard(en->tmp);
        if (en->bm) bm_close(en->bm);
        en->bm = NULL;
        en->tmp[0] = '\0';
    }
    if (fix->marker[0]) {
        (void)unlink(fix->marker);
        (void)fsync_dir(fix->indexes_dir);
        fix->marker[0] = '\0';
    }
    free(fix->e);
    fix->e = NULL;
    fix->n = 0;
}

/* 0 = ready (fix may be empty: no bitmaps), -1 = failed with everything
   released/removed (errno set) -- the caller must abandon the rebuild. */
static int kf_bmfix_prepare(KfBmFix *fix, const char *kf_path, const KfRemapCtx *ctx) {
    memset(fix, 0, sizeof(*fix));
    char objdir[PATH_MAX];
    int shard = 0;
    if (kf_bmfix_split_path(kf_path, objdir, sizeof(objdir), &shard) != 0) return 0;
    int n = snprintf(fix->indexes_dir, sizeof(fix->indexes_dir), "%s/indexes", objdir);
    if (n < 0 || (size_t)n >= sizeof(fix->indexes_dir)) return 0;
    DIR *d = opendir(fix->indexes_dir);
    if (!d) return errno == ENOENT ? 0 : -1;

    int rc = 0;
    struct dirent *de;
    while (rc == 0 && (de = readdir(d)) != NULL) {
        if (de->d_name[0] == '.') continue;
        char bm_path[PATH_MAX];
        n = snprintf(bm_path, sizeof(bm_path), "%s/%s/%03x.bm", fix->indexes_dir, de->d_name, shard);
        if (n < 0 || (size_t)n >= sizeof(bm_path)) { errno = ENAMETOOLONG; rc = -1; break; }
        struct stat st;
        if (stat(bm_path, &st) != 0 || !S_ISREG(st.st_mode)) continue;  /* btree dir / no file for this shard */
        if (fix->n >= MAX_FIELDS || ctx->new_cap > (size_t)(UINT32_MAX / 2)) {
            errno = EOVERFLOW;
            rc = -1;
            break;
        }
        if (!fix->e) {  /* lazy: btree-only objects never allocate */
            fix->e = calloc(MAX_FIELDS, sizeof(*fix->e));
            if (!fix->e) { rc = -1; break; }
        }
        BitmapShard *bm = bm_open(bm_path, 0, 0, 0, 0, 1 /* writer: kf -> bm lock order */);
        if (!bm) { if (!errno) errno = EIO; rc = -1; break; }
        KfBmFixEntry *en = &fix->e[fix->n++];
        en->bm = bm;
        if (bm_slotmap_prepare(bm, (uint32_t)ctx->new_cap, kf_remap_slot, (void *)ctx,
                               en->tmp, sizeof(en->tmp)) != 0) {
            en->tmp[0] = '\0';
            rc = -1;
            break;
        }
    }
    int saved = errno;
    closedir(d);
    if (rc == 0 && fix->n > 0) {
        snprintf(fix->marker, sizeof(fix->marker), "%s/.bmfix-%03x", fix->indexes_dir, shard);
        int fd = open(fix->marker, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) {
            saved = errno;
            fix->marker[0] = '\0';
            rc = -1;
        } else {
            if (fsync(fd) != 0) { saved = errno; rc = -1; }
            if (close(fd) != 0 && rc == 0) { saved = errno; rc = -1; }
            if (rc == 0 && fsync_dir(fix->indexes_dir) != 0) { saved = errno; rc = -1; }
        }
    }
    if (rc != 0) {
        kf_bmfix_abort(fix);
        errno = saved ? saved : EIO;
        return -1;
    }
    if (fix->n == 0) { free(fix->e); fix->e = NULL; }
    return 0;
}

/* Called right after the new keyfile is renamed in. Returns -1 if any bitmap
   could not be published (marker retained for startup repair). */
static int kf_bmfix_commit(KfBmFix *fix) {
    int rc = 0;
    for (int i = 0; i < fix->n; i++) {
        KfBmFixEntry *en = &fix->e[i];
        if (!en->bm) continue;
        if (bm_slotmap_publish(en->bm, en->tmp) != 0) {
            bm_slotmap_discard(en->tmp);
            rc = -1;
        }
        en->tmp[0] = '\0';
        bm_close(en->bm);
        en->bm = NULL;
    }
    if (rc == 0 && fix->marker[0]) {
        if (unlink(fix->marker) != 0 && errno != ENOENT) rc = -1;
        else if (fsync_dir(fix->indexes_dir) != 0) rc = -1;
        fix->marker[0] = '\0';
    }
    free(fix->e);
    fix->e = NULL;
    fix->n = 0;
    return rc;
}

```
- [ ] 3.2 `src/db/slotcask.c`, in `kfcache_resplit_locked`: replace
```c
    new_hdr->total = live_copied;
    new_hdr->deleted = 0;
    uint64_t t_after_rebuild = kf_now_us();
```
  with
```c
    new_hdr->total = live_copied;
    new_hdr->deleted = 0;
    uint64_t t_after_rebuild = kf_now_us();  /* NOTE: B2 prepare + marker fsync time lands in the msync_us bucket of KF_RESPLIT for bitmap objects (observability only) */

    /* === Phase B2: prepare slot-remapped bitmaps (abortable) ===
       Entries just moved to new slot numbers; bitmap bits are slot numbers.
       Failure here aborts the whole rebuild with the old kf untouched. */
    KfBmFix bmfix;
    {
        KfRemapCtx rctx = { .old_map = kh->map, .old_cap = old_cap,
                            .new_map = new_entries, .new_cap = new_cap };
        if (kf_bmfix_prepare(&bmfix, e->path, &rctx) != 0) {
            LOG_ERROR(LOG_SUB_SLOTCASK,
                      "KF_BMFIX_PREPARE_FAILED path=%s new_cap=%zu: %s (resplit abandoned, nothing changed)",
                      e->path, new_cap, strerror(errno));
            munmap(new_base, new_size);
            close(new_fd);
            unlink(new_path);
            return -1;
        }
    }
```
- [ ] 3.3 Same function: replace
```c
    /* === Phase D: rename + parent-dir fsync === */
    if (rename(new_path, e->path) != 0) {
        unlink(new_path);
        return -1;
    }
```
  with
```c
    /* === Phase D: rename + parent-dir fsync === */
    if (rename(new_path, e->path) != 0) {
        unlink(new_path);
        kf_bmfix_abort(&bmfix);   /* kf unchanged: drop temps, marker, bitmap locks */
        return -1;
    }
```
- [ ] 3.4 Same function: replace the single line
  `    uint64_t t_after_rename = kf_now_us();`
  with
```c
    uint64_t t_after_rename = kf_now_us();

    /* Publish the remapped bitmaps now, before Phase E, so disk state is
       consistent even if the remap below fails. Still under the kf wrlock. */
    if (kf_bmfix_commit(&bmfix) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
                  "KF_BMFIX_FAILED path=%s: bitmap indexes of this shard are stale until the next start "
                  "(marker retained, startup rebuilds them) or an explicit reindex", e->path);
    }
```
- [ ] 3.5 `SKIP_TESTS=1 ./build.sh 2>&1 | tail -20` — no new warnings (paste). `dirent.h`, `sys/stat.h`, `fcntl.h`, `bitmap.h`, `MAX_FIELDS` (types.h)
  are already included by `slotcask.c`; `fsync_dir` is defined earlier in the file ("static int fsync_dir(const char *dir_path)").

## Task 4 — Marker helpers for startup recovery

- [ ] 4.1 `src/db/slotcask.h`: replace the line
  `int  slotcask_compact_kf(SlotcaskDb *db);`
  with
```c
int  slotcask_compact_kf(SlotcaskDb *db);

/* Bitmap-remap crash markers (<data_dir>/indexes/.bmfix-NNN, see slotcask.c).
   pending: 1 if any marker exists, 0 if none, -1 on I/O error.
   clear: remove every marker and every leftover "<NNN>.bm.remap" temporary
   (call only after the object's indexes were rebuilt). 0 / -1. */
int  slotcask_bmfix_pending(const char *data_dir);
int  slotcask_bmfix_clear(const char *data_dir);
```
- [ ] 4.2 `src/db/slotcask.c`: insert immediately **before** the comment line
  `/* Rebuild every kf shard in place to drop tombstones (flag=2 entries).`
  this block:
```c
int slotcask_bmfix_pending(const char *data_dir) {
    char idx[PATH_MAX];
    int n = snprintf(idx, sizeof(idx), "%s/indexes", data_dir);
    if (n < 0 || (size_t)n >= sizeof(idx)) return -1;
    DIR *d = opendir(idx);
    if (!d) return errno == ENOENT ? 0 : -1;
    int found = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (strncmp(de->d_name, ".bmfix-", 7) == 0) { found = 1; break; }
    }
    closedir(d);
    return found;
}

int slotcask_bmfix_clear(const char *data_dir) {
    char idx[PATH_MAX];
    int n = snprintf(idx, sizeof(idx), "%s/indexes", data_dir);
    if (n < 0 || (size_t)n >= sizeof(idx)) return -1;
    DIR *d = opendir(idx);
    if (!d) return errno == ENOENT ? 0 : -1;
    int rc = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        char p[PATH_MAX];
        if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
        if (strncmp(de->d_name, ".bmfix-", 7) == 0) {
            snprintf(p, sizeof(p), "%s/%s", idx, de->d_name);
            if (unlink(p) != 0 && errno != ENOENT) rc = -1;
            continue;
        }
        char fdir[PATH_MAX];
        snprintf(fdir, sizeof(fdir), "%s/%s", idx, de->d_name);
        DIR *fd = opendir(fdir);
        if (!fd) continue;                         /* not a field directory */
        struct dirent *fe;
        while ((fe = readdir(fd)) != NULL) {
            size_t len = strlen(fe->d_name);
            if (len > 9 && strcmp(fe->d_name + len - 9, ".bm.remap") == 0) {
                snprintf(p, sizeof(p), "%s/%s", fdir, fe->d_name);
                if (unlink(p) != 0 && errno != ENOENT) rc = -1;
            }
        }
        closedir(fd);
    }
    closedir(d);
    if (fsync_dir(idx) != 0) rc = -1;
    return rc;
}

```
  (`".bm.remap"` is 9 characters, hence `len > 9` / `len - 9`.)

## Task 5 — Startup recovery

- [ ] 5.1 `src/db/embedded.c`: insert immediately **before** the line `int shard_db_recover_before_stamp(const char *db_root,` this function:
```c
/* A keyfile rebuild interrupted between its keyfile rename and its bitmap
   publication (or whose bitmap publication failed and was logged) leaves
   <obj>/indexes/.bmfix-NNN. Bitmap bits are kf slot numbers, so those bitmaps
   may name old slots: rebuild the object's indexes from kf + segments (pure
   function of durable state) with the strict reindex used by the BTRI
   migration, then drop the markers. Must run after KFM2 replay. */
static int bmfix_recover_all(const char *db_root) {
    StartupSchemaEntry *ents = NULL;
    size_t cnt = 0;
    if (startup_read_schema(db_root, &ents, &cnt) != 0) return -1;
    for (size_t i = 0; i < cnt; i++) {
        char eff_root[PATH_MAX], data_dir[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, ents[i].dir);
        snprintf(data_dir, sizeof(data_dir), "%s/%s/%s", db_root, ents[i].dir, ents[i].object);
        int pending = slotcask_bmfix_pending(data_dir);
        if (pending < 0) { free(ents); return -1; }
        if (!pending) continue;
        LOG_WARN(LOG_SUB_REINDEX, "BMFIX-RECOVER %s/%s: interrupted bitmap remap; rebuilding indexes",
                 ents[i].dir, ents[i].object);
        if (objlock_wrlock(eff_root, ents[i].object) != 0) { free(ents); return -1; }
        int n = 0, eno = 0;
        int rc = reindex_object_durable_checked_ex(eff_root, ents[i].object, 0, &n, &eno);
        objlock_wrunlock(eff_root, ents[i].object);
        if (rc != 0) {
            LOG_ERROR(LOG_SUB_REINDEX,
                      "BMFIX-RECOVER %s/%s: reindex failed (errno=%d); startup refused, retried next start",
                      ents[i].dir, ents[i].object, eno);
            free(ents);
            return -1;
        }
        if (slotcask_bmfix_clear(data_dir) != 0) { free(ents); return -1; }
    }
    free(ents);
    return 0;
}

```
- [ ] 5.2 Same file, clean-stop branch: replace
```c
        /* Clean stop: no markers can be pending, so the migration (if it
           ran) is the last recovery work — stamp completion and serve. */
        if (btri_migration_ran &&
```
  with
```c
        /* Clean stop: no KFM2 markers can be pending, but a bitmap-remap
           marker can survive a logged publish failure: repair it before
           serving. The migration (if it ran) is then the last recovery work. */
        if (bmfix_recover_all(db_root) != 0) return -1;
        if (btri_migration_ran &&
```
- [ ] 5.3 Same file, non-clean path: replace the line
  `    /* Replay completed against 'BTRI' — only now is the migration stamp`
  with
```c
    /* Bitmap-remap repair runs after replay (rebuild is a pure function of
       the final kf + segments). */
    if (bmfix_recover_all(db_root) != 0) { free(entries); return -1; }
    /* Replay completed against 'BTRI' — only now is the migration stamp
```
- [ ] 5.4 `SKIP_TESTS=1 ./build.sh 2>&1 | tail -20` (paste; no new warnings). `LOG_SUB_REINDEX` is already used in embedded.c; `LOG_WARN` is new there but is defined in log.h and compiles (confirm the include is present).

## Task 6 — Green, and the revert proof (paste both halves)

- [ ] 6.1 Targeted green:
  ```bash
  for t in bulk legacy single vacuum restart marker-recovery concurrent-readers; do ./build/bin/shard-db-test run test-bitmap-resplit-$t 2>&1 | grep -E '^(not ok|# Failed|# bitmap)' ; ./build/bin/shard-db-test run test-bitmap-resplit-$t 2>&1 | tail -3; done
  ./build/bin/shard-db-test run test-predicate-equivalence 2>&1 | tail -3
  ./build/bin/shard-db-test run test-packed-recompose 2>&1 | tail -3
  ./build/bin/shard-db-test run test-legacy-recompose 2>&1 | tail -3
  rm -rf /tmp/shard-db-*
  ```
  Expect: zero `not ok`. The restart and marker-recovery cases use `test_env_start_at`, so confirm the `rm -rf /tmp/shard-db-*` leaves nothing behind.
- [ ] 6.2 Revert proof (the fix must be what turns the tests green):
  ```bash
  git diff --stat > /tmp/bmfix-stat-before.txt
  git diff -- src/db > /tmp/bmfix-src.patch
  git checkout -- src/db
  SKIP_TESTS=1 ./build.sh 2>&1 | tail -3
  for t in bulk single vacuum marker-recovery; do ./build/bin/shard-db-test run test-bitmap-resplit-$t 2>&1 | grep -c '^not ok'; done
  git apply /tmp/bmfix-src.patch
  SKIP_TESTS=1 ./build.sh 2>&1 | tail -3
  for t in bulk single vacuum marker-recovery; do ./build/bin/shard-db-test run test-bitmap-resplit-$t 2>&1 | grep -c '^not ok'; done
  rm -rf /tmp/shard-db-* /tmp/bmfix-src.patch /tmp/bmfix-stat-before.txt
  ```
  Paste all counts: first loop must be non-zero for every case, second loop `0`. (`git checkout -- src/db` reverts only tracked engine files;
  the new test file, `build.sh`, and docs are untouched. If `git apply` fails, STOP — do not reconstruct by hand; write `PLAN_NOTES-bmfix.md`.)
  Run `git diff --stat | diff - /tmp/bmfix-stat-before.txt` afterwards (must be empty).

## Task 7 — Documentation (same branch)

- [ ] 7.1 `docs/release-notes/2026.11.1.md`: in the **Known issues** bullet delete the sentence
  `Bitmap-indexed fields can under-count after a keyfile shard has grown during inserts until \`reindex\` runs (shard growth renumbers keyfile slots, which the bitmaps are keyed by; run \`reindex\` to repair); this predates the type work and also affects legacy-layout objects. `
  (leave the remaining sentences of that bullet), and in the **Fixes** bullet replace
  `both are fixed (bitmaps are rebuilt after every recompose and read packed cells).`
  with
  `both are fixed (bitmaps are rebuilt after every recompose and read packed cells). Bitmap indexes also stayed correct only until a keyfile shard grew or was compacted: every keyfile rebuild (in-place growth, \`vacuum\`) now renumbers the bitmap bits with the entries, crash-safely, so no \`reindex\` is needed after bulk loads; a startup repair rebuilds the indexes of an object whose rebuild was interrupted.`
- [ ] 7.2 `docs/reference/changelog.md`, under `## Unreleased`, append a paragraph after the "**Packed recompose (R5b).**" paragraph:
  `**Bitmap indexes survive keyfile rebuilds.** Bitmap bit \`i\` means "kf slot \`i\`", and a keyfile growth resplit or \`vacuum\` compaction renumbers slots; previously the bits were not renumbered, so bitmap predicates silently under-counted until \`reindex\` (observed 762 of 1714 rows). \`kfcache_resplit_locked\` now writes a slot-remapped copy of each of the shard's \`.bm\` files before the keyfile rename (an I/O failure aborts the resplit with nothing changed), records a durable \`indexes/.bmfix-NNN\` marker, and publishes the bitmaps right after the keyfile under the same kf lock; a crash in between is repaired at startup by a strict reindex of that object. Log lines: \`KF_BMFIX_PREPARE_FAILED\`, \`KF_BMFIX_FAILED\` (ERROR), \`BMFIX-RECOVER\` (WARN). No on-disk format change.`
- [ ] 7.3 `docs/concepts/concurrency.md`, in "### Cross-cache rule: kfcache before bitmap cache", append a paragraph at its end:
  `Keyfile rebuilds (growth resplit, vacuum compaction) follow the same order: \`kfcache_resplit_locked\` holds the kf entry wrlock and takes each affected bitmap shard's wrlock to publish a slot-remapped copy, so no reader can observe a new keyfile with old bitmap bits.`
- [ ] 7.4 `docs/concepts/indexes.md` line beginning `- **Bitmap** (opt-in via` — at the end of that bullet append:
  ` Bit positions are kf slot numbers, so every keyfile rebuild (growth resplit, \`vacuum\`) remaps the bits with the entries; \`reindex\` is not needed after bulk loads.`
- [ ] 7.5 `AGENTS.md` (+ `CLAUDE.md` imports it; edit only `AGENTS.md`): in the **Storage model** list, after the bullet beginning `- **Dynamic growth**:` insert
  `- **Bitmap slot coherence**: bitmap bit \`i\` of shard \`s\` is "kf slot \`i\`". Every keyfile rebuild (growth resplit, \`slotcask_compact_kf\`) goes through \`kfcache_resplit_locked\`, which first writes a slot-remapped, fsynced copy of each of the shard's \`.bm\` files, then a durable \`indexes/.bmfix-NNN\` marker, renames the keyfile, publishes the bitmaps, and drops the marker, all under the kf wrlock (lock order kf → bitmap). A leftover marker makes startup (\`bmfix_recover_all\`, after KFM2 replay) run the strict reindex for that object.`
- [ ] 7.6 `docs/plans/2026-11-04-bitmap-stale-after-resplit.md`: change the line starting `**Status:** reproduced on this stack; **not fixed**;` to
  `**Status:** fixed by \`docs/plans/2026-11-05-bitmap-stale-after-resplit-fix.md\` (option 1: remap in the resplit + durable marker).` and leave the rest as history.
- [ ] 7.7 `git diff --stat -- docs AGENTS.md` and confirm no other doc moved. Grep that no remaining doc claims the issue is open:
  `grep -rn -i 'under-count after a keyfile\|until `reindex` runs' docs README.md AGENTS.md | grep -v '^docs/plans/'` → no output.

## Task 8 — Full normal suite (fresh, not a subset)

- [ ] 8.1 `SKIP_TESTS=1 ./build.sh` then `./build/bin/shard-db-test run-all --jobs 4 2>&1 | tail -15`; paste the totals. Expected: previous baseline
  2,406,179 passed / 602 cases plus the new 7 cases and the 16 restored assertions; **0 failed**. Then `rm -rf /tmp/shard-db-*`.
- [ ] 8.2 Any failure: do not rerun-until-green. Root-cause it. A flaky test that passes on rerun is a bug until explained (CORE-PROCESS).
- [ ] 8.3 Definition-of-done sweep: `git diff | grep -nE 'TODO|FIXME|XXX|fprintf\(stderr'` → nothing new; compiler output has no new warnings;
  `git status --short` shows only the files this plan names (`src/db/bitmap.{c,h}`, `src/db/slotcask.{c,h}`, `src/db/embedded.c`,
  `src/test/cases/test_bitmap_resplit.c`, `src/test/cases/test_predicate_equivalence.c`, `build.sh`, the docs above, this plan) plus the two known
  untracked docs. No dependency added.

## Task 9 — Sanitizer gate (once; this is what discharges "Dynamic-safety tooling" at hand-back)

Touches locks and bitmap-cache state, so AGENTS.md requires both. Run only after Tasks 0–8 are complete and the human has had the raw diff
(do not run between fix rounds).

- [ ] 9.1 ASan+UBSan: `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then **three fresh** runs of `./build/bin/shard-db-test run-all`
  (default parallelism; no `halt_on_error=0` anywhere). `rm -rf /tmp/shard-db-*` after each. Paste the three summaries.
- [ ] 9.2 TSan: `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then **three fresh** runs of
  `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2`. `rm -rf /tmp/shard-db-*` after each.
  Specifically watch `test-bitmap-resplit-concurrent-readers`. A finding in the new code is a real bug: fix, then restart the affected gate
  (all three runs). A finding inside the test itself (e.g. `rd.stop`) is fixed test-side (`_Atomic int`) — say so in the report.
- [ ] 9.3 Rebuild the normal binary afterwards: `SKIP_TESTS=1 ./build.sh`.

## Task 10 — Hand-back

- [ ] 10.1 Report: branch name, `git diff --stat`, the Task 1 red output, Task 6 revert proof (both halves), Task 8 totals, Task 9 six summaries,
  the Task 0.3 audit lines, and anything that deviated. State explicitly: work is **uncommitted**; the residual risk above (bitmap rename I/O
  failure after the keyfile rename) and the unmeasured resplit cost (human runs `bench-grow`/`bench-kv`).
- [ ] 10.2 Per CORE-PROCESS step 4 this diff touches concurrency and an on-disk side-artifact: request a **blind review** of the raw diff by a
  reviewer that has not seen this plan. Do not commit, push, or open a PR.

---

## Addendum 2026-11-06 — changes after the blind review (human approved)

The blind review of the Task 0–8 diff found one P0 and several P1/P2 items; the human approved the fixes below **within this plan**. Where this
addendum conflicts with the task text above, the addendum wins (the tasks above describe the first implementation).

1. **In-place commit instead of rename** (human request; removes the "bitmap rename fails after the keyfile rename" residual risk).
   `bm_slotmap_publish` is replaced by `bm_slotmap_stage` (prepare phase: reserve blocks with `posix_fallocate`, map the larger window before
   dropping the old one, map the `.remap` image read-only) and `bm_slotmap_apply` (commit: `memcpy` the image over the live mapping, refresh the
   handle header, `msync`). Nothing in apply can fail except the msync; a failed msync keeps the marker (log `KF_BMFIX_FAILED`) and the in-memory
   bitmaps are already correct. A crash mid-copy is repaired by the marker/startup rebuild exactly as before.
2. **P0 lock order.** Bitmap-only readers (`bm_intersect_shard_worker`, bitmap group-by) hold several bitmap read locks without the kf lock; the
   resplit held several write locks in `readdir` order. All multi-bitmap holders now acquire in ascending `<field>/NNN.bm` path order
   (`bm_open_many_ro`; `bm_field_lock_cmp` for the single-insert staging loop in `storage.c`). The remap sorts its collected paths with the same
   comparator. Documented in `concurrency.md`. The bulk window (`bitmap_window_find_or_open`) opens lazily in first-use order and is **not**
   changed here (pre-existing exposure, follow-up). Honest limit: the reader/writer inversion could not be triggered from a black-box test even with
   the order deliberately reversed (6/6 passes), so `test-bitmap-resplit-concurrent-readers` (now 4 readers, two-criterion counts in both orders,
   `_Atomic` stop flag) is a stress guard, not a proven-red test; the fix rests on the lock-order analysis.
3. **P1 marker loss.** A marker that exists when a rebuild prepares is never removed by that rebuild (`marker_preexisting`).
4. **P2** retry storm: after `KF_BMFIX_PREPARE_FAILED` the shard's next resplit is refused for 5 s (`KfCacheEntry.bmfix_retry_ms/_gen`);
   prepare moved after the keyfile msync (bitmap readers no longer wait behind it); a malformed object-layout keyfile path now fails closed
   (`EINVAL`) instead of silently skipping the remap (non-object-layout paths still skip); `_Atomic int` stop flag in the test.
5. **Not changed (reported):** startup repair reindexes the whole object (#9); inline resplit with outstanding window reservations (#10, older
   hazard); no kill-point/fault-injection tests (#11); bitmap-cache-full pinning behaviour (#2) beyond holding handles only for the prepare→commit span;
   `req_bitmap_file_path` (separate plan `2026-11-06-bulk-chain-bitmap-sync-path.md`).

## Addendum 2 (2026-11-06) — changes after the second blind review (human: "fix all")

1. **P1 same-path double read lock.** `bm_open_many_ro` shares one handle for repeated paths and `bm_close_many` closes each distinct handle
   once; used by the AND-count worker, the group-by aggregate and the bitmap post-filter fallback in `query.c` (which now also opens in canonical order).
2. **P1 resplit inside a bulk window.** `kf_plan_window_insert_slot` no longer resplits while the window holds planned slots (`nreserved > 0`) or
   the request's reservation map is non-empty; the record fails instead of being committed into a renumbered table. Single-insert planning is
   unchanged (no reservations exist there).
3. **P2 uncached writer handle.** The remap refuses (errno `EBUSY`, resplit abandoned, 5 s back-off) when `bm_open(writer)` returns a handle that
   does not hold its cache lock (`bm_handle_cached`). **Known limit (not fixed):** a read-only handle opened through the uncached fallback (cache
   saturated) holds no lock and can read a freshly applied layout with its old header for that one request.
4. **P2 torn header after power loss.** `bm_slotmap_stage` `fdatasync`s the file after extending it, before the marker exists; `bm_file_open_mmap`
   rejects a header whose stride or length does not fit the file (`EUCLEAN`). Startup bitmap repair now runs **before** the KFM2 replay on the
   unclean path (as the BTRI migration does), so replay never writes into a torn bitmap.
5. **P2 marker deleted too early.** `kf_bmfix_commit` no longer removes the marker; `kf_bmfix_finish` does, after Phase E (keyfile mapping swap)
   succeeded. A Phase E failure leaves the marker for startup.
6. **P2 failure paths tested.** New TEST_BUILD fault seam (`g_shard_test_bmfix_fault_mode/count`, control-channel `SET_GLOBALS` phase 2,
   `test_env_ctl_set_bmfix_fault`): `test-bitmap-resplit-fault-{prepare,rename,commit-clean,commit-unclean}` cover prepare failure + back-off, keyfile
   rename failure + abort cleanup, unconfirmed commit with marker retention (also across a later rebuild), and startup recovery after a clean stop
   and after SIGKILL. Mutation-checked: removing the abort-path unlink, or letting a later rebuild remove a pre-existing marker, turns the matching test red.
7. **P3.** back-off applies to growth only (vacuum is exempt); commit/finish failure log text corrected; startup logs why it refuses when marker
   clearing fails; a corrupt bitmap (`EUCLEAN`) no longer blocks keyfile growth — it is skipped (`KF_BMFIX_SKIP`), the marker is written and is never
   removed by a runtime rebuild, so the next start reindexes; leftover `.bm.remap` temporaries are swept at startup; wording fixes (AGENTS.md, test comment).
8. **Still open (reported):** bulk-window lazy bitmap open order; uncached read handles (3); whole-object startup reindex; cache-full eviction
   self-wait when a rebuild pins many writer handles; kf write-lock hold time now includes one rewrite+fsync per bitmap (human benches).

## Addendum 3 (2026-11-06) — changes after the third blind review (human: "go ahead")

1. **P1 chain pre-grow.** The window-planning gate forbids a resplit once a chain member holds reservations, so the *stage* of each joiner must size
   the table for the whole chain. `slotcask_bulk_stage_shard` takes a new `chain_pending` argument (records of earlier chain members, staged but
   not yet planned); the owner passes 0, each joiner the running sum; the pre-grow projects `total + chain_pending + n`. Both callers updated.
   `test-bitmap-resplit-chain` (4 concurrent bulk-inserting clients) is a smoke test: removing the argument did not turn it red at this scale.
2. **P2** a bitmap that `bm_open` rejects (header does not fit the file) is detected with `bm_file_is_corrupt` and skipped like the remap-time
   `EUCLEAN` (marker kept, startup reindex repairs; `test-bitmap-resplit-corrupt-bitmap`, driven through vacuum). The keyfile parent-directory fsync
   result is now checked; if it fails the marker is kept. A failed keyfile rename also backs off 5 s (growth only; vacuum is exempt in both cases).
3. **P3** `readdir`/`stat` errors in the prepare scan fail closed; `req_res_empty` replaces the full `req_res_count` scan in the gate; the
   single-insert staging sorts only bitmap fields; AGENTS.md states the unclean-path order and the 5 s delay.
4. **Accepted / not changed:** Phase E (keyfile remap) failure after the commit leaves cached bitmaps in the new layout in front of the old kf
   mapping until restart (marker kept, startup repairs; predates this work); a skipped corrupt bitmap keeps being served until restart;
   bitmap-cache-full eviction self-wait (needs a cache redesign); no test for an unclean start with both pending KFM2 markers and a `.bmfix` marker.

## Addendum 4 (2026-11-06) — Phase E can no longer fail (human request)

Addendum 3 item 4 accepted that a failed keyfile reopen/mmap after the commit left the remapped bitmaps in front of the old keyfile mapping until
restart. That window is removed: `kfcache_resplit_locked` keeps the new table's fd and mapping from Phase A open through the rename and installs
them as the live mapping in Phase E (`reopen_fd = new_fd; fresh = new_base`), instead of reopening and re-mapping the path. The mapped inode is the
one just renamed over `e->path` and was msync'd in Phase C, so nothing after the point of no return can fail except the bitmap msync (which keeps
the marker and leaves the in-memory state correct). The Phase D rename-failure path now releases the mapping itself. Item 4's first limit no longer
applies; the remaining "until restart" case is a skipped corrupt bitmap (use `reindex` or restart).
