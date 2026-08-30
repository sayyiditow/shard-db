# macOS arm64: numeric BETWEEN across zero returns a wrong count

Date: 2026-08-28 (revised 2026-08-30 — execution-ready; 2nd revision
2026-08-30 — B2 lazy-shard-file fix, resolves the PLAN_NOTES.md halt)
Status: ready to resume execution once the human hands it back (per the
halt rule). **Tasks 1–4 are diagnostic only.** Task 1 evidence is
recorded below (macOS `got 2`). The fix itself is deliberately not in
this plan: Task 4 halts execution and requires a human-approved amended
plan (with complete fix code blocks) before any fix code is written.
`test-binary-index` is excluded from the `ci.yml` PR gate only until
fixed; it still runs in local `run-all` gates on Linux, where it passes.

## Problem

On the GitHub `macos-latest` (Apple silicon, Apple clang) leg of CI,
`test-binary-index` assertion 12 fails deterministically:

    not ok 12 - numeric between -1 and 1 = 3

The fixture (`src/test/cases/test_binary_index.c`, `bi_num` object,
`amt:numeric:10,2` with a btree index on `amt`) inserts
`-999.99, -0.01, 0, 0.01, 999.99` (scaled ×100: `-99999, -1, 0, 1,
99999`), then asserts:

- `numeric lt 0 → 2 negatives` — **passes** on macOS (assert 11)
- `numeric between -1 and 1 = 3` — **fails** on macOS (assert 12)

## Verified facts

1. **Pre-existing, not caused by the 2026-08-28 PR**: the diff touches no
   query-path code. The failure was invisible because the macOS leg could
   not compile at all until the `reallocarray` fix landed; assert 12 is
   the first macOS test result in CI history for this case.
2. **Stored values are provably correct on macOS**: `lt 0` returning 2
   requires exactly the scaled values `-99999` and `-1` to be present and
   correctly ordered. So the insert-side numeric encoder
   (`config.c` `encode_field` FT_NUMERIC, plain BE int64) and the index
   key biasing (`typed_field_to_index_key`: BE + `out[0] ^= 0x80`) are
   right; the corruption is in the **between/range path with a negative
   lower bound**.
3. **Bounds are encoded consistently by construction**: criterion values
   go through `encode_criterion_value` → `encode_field_for_index`
   (config.c FT_NUMERIC case: `atof` → `× numeric_scale_mult` → round
   toward zero with ±0.5 bias → `^ (1ULL << 63)` → BE). `-1` → scaled
   `-100` → biased bytes `0x7F..FF9C`. Same IEEE `atof` semantics on all
   platforms; no arch-dependent value can reach ±100 here.
4. **Not a plain clang-vs-gcc divergence**: a full clang build on Linux
   x86_64 (LTO stripped, `-latomic` added) passes the case 22/0.
5. **Not char-signedness**: aarch64-linux gcc compiles with `char`
   unsigned (vs signed on Darwin/x86) and the linux arm64 leg passes.
6. The btree keys of interest (`-1, 0, 1` scaled) sit exactly at the
   biased top-bit boundary (`0x7F…` → `0x80…` carry into the first key
   byte) — the range walk / prefix-compression / anchor logic around that
   boundary is the prime suspect, but no defect has been identified by
   inspection yet.
7. **The actual returned count is already in the CI logs**: the
   `ASSERT_EQ_INT` macro (`src/test/test_assert.h:45-48`) prints
   `#   expected 3 got <N>` on failure. No test edit is needed to learn
   the actual count — read it from the failed macOS run of the
   2026-08-28 PR (Task 1).
8. **Harness facts the probe relies on** (verified in-repo):
   - Test cases may call daemon internals directly
     (`btree_insert`, `btree_range`, `btree_range_ex`,
     `btree_range_iter_open/next/close`, `btree_walk_all_values`,
     `encode_field_for_index`, `build_idx_path`, `index_splits_for`);
     the runner initializes `g_db` and resets caches per case
     (`src/test/test_runner.c:17,124`). Precedent:
     `src/test/cases/test_btree.c`.
   - `test_env_stop()` SIGTERMs the daemon and **`rm -rf`s `db_root`**
     (`src/test/fixtures.h:33-36`) — so Phase B must inspect the real
     index files *before* teardown, while the idle daemon merely holds
     read-only mappings (concurrent reads are safe).
   - `index_splits_for(16) == 4` (types.h curve) — `bi_num` routes each
     record to one of four `indexes/amt/000..003.idx` files by record
     xxh128 via `idx_shard_for_hash`, not by value. **Btree files are
     created lazily: a shard with zero routed records has no file at
     all** (observed on Linux 2026-08-30: only 3 of 4 files exist for
     this fixture — see PLAN_NOTES.md / second revision). Expected
     routing is computable in-probe via `compute_hash_raw(key, klen, h)`
     (types.h:902) + `idx_shard_for_hash(h, splits)`, the same
     derivation the daemon performs.
   - `build_idx_path(buf, buflen, db_root, object, field, shard)`
     (`src/db/storage.c:13-18`) takes `object` as `"<dir>/<obj>"`.
   - `btree_range_iter_next` returns 1 with an entry, 0 when exhausted
     (`src/db/btree.c:2702-2713`).
   - Expected biased key bytes at scale ×100 (BE, top-bit flipped):
     stored `-999.99`→`7FFFFFFFFFFE7961`, `-0.01`→`7FFFFFFFFFFFFFFF`,
     `0`→`8000000000000000`, `0.01`→`8000000000000001`,
     `999.99`→`800000000001869F`; bounds `-1`→`7FFFFFFFFFFFFF9C`,
     `1`→`8000000000000064`.

## Embedded execution rules

- Branch: create `diag/macos-numeric-between-probe` off `main`. Do all
  tasks on it. Per this repo's execution mode, leave work **uncommitted**
  at the end of each task that doesn't require CI. The sole exception is
  Task 3: the scratch branch must commit the probe, the build.sh /
  workflow edits, AND this plan file with the Task 1/3 evidence appended,
  so the diagnostic record is reviewable in the PR and durable (PR
  comments persist after branch deletion — Task 3c). Task 3's pushes are
  human-run or human-directed git operations.
- Do tasks in order. Do not skip ahead to fix work: **this plan contains
  no fix task by design** (see Task 4).
- Build/test commands for this repo:
  - build: `SKIP_TESTS=1 ./build.sh`
  - single case: `./build/bin/shard-db-test run test-numeric-between-probe`
  - local suite: `./build/bin/shard-db-test run-all`
- If a quoted anchor isn't found exactly, write `PLAN_NOTES.md` describing
  the mismatch and halt the entire execution run immediately — do not
  guess, reinterpret, or continue to any further task, even an unrelated
  one. Resuming requires the human (or the planning model, re-engaged)
  to read `PLAN_NOTES.md`, decide whether it's a stale-anchor problem
  (re-derive and patch the plan) or a wrong-assumption problem (rethink
  the plan), and hand back either a patched or a fresh plan — execution
  never resumes on its own initiative.
- If you hit a decision the plan doesn't cover, stop and ask — do not
  improvise.
- Never weaken, delete, or skip an existing test to make a failure
  disappear. The probe case added in Task 2 is diagnostic scaffolding on
  a scratch branch only; it must never reach `main` while red (Task 4
  removes it).

## Task 1 — Recover the actual count from the existing macOS CI log

No code changes. The failing assert already prints the actual value
(Verified fact 7).

1. On GitHub, open the 2026-08-28 PR's CI run → the
   `Build & test (macos arm64)` job → the "Run full C test suite" step
   log.
2. Search for `not ok 12` and copy the full assertion block, including
   the `#   expected 3 got N` line immediately beneath it.
3. Paste that block into this plan file under a new
   `## Evidence — Task 1` heading (append; do not rewrite existing
   sections).
4. Interpret against the fixture's stored set (scaled: `-99999, -1, 0,
   1, 99999`; between `-100..100` must return exactly `-1, 0, 1`):

   | got N | Meaning |
   |---|---|
   | 2 | one in-range key missing — lower-bound seek overshoots past `-1`'s key (`0x7F…FF`), or a leaf/page is skipped |
   | 4 | one out-of-range key included — upper-bound overshoot |
   | 5 | bounds effectively ignored — full-leaf walk returned |
   | 0 / 1 | seek returns empty or stops immediately — lower-bound seek fails to land anywhere in range |

   Record the interpretation in one sentence under the pasted evidence.
   This informs Task 3's read-out but does not gate it — proceed to
   Task 2 regardless of which shape N takes.

## Task 2 — Add the diagnostic probe case

The probe isolates the four bound-consuming layers so exactly one CI run
localizes the defect:

- **A1** — platform arithmetic: an independent in-probe encoder vs
  hardcoded golden bytes (fails only if the Apple toolchain evaluates
  the numeric encoding arithmetic differently — contradicts fact 3).
- **A2** — `encode_field_for_index` vs the same golden bytes (fails only
  if config.c's encoder diverges on macOS).
- **A3/A4** — insert five keys into a bare btree file, then read them
  back with `btree_walk_all_values` (no bounds, no seek — what is
  physically stored).
- **A5** — `btree_range` and `btree_range_ex` on that file (the seek
  path — suspicion set entries 1 and 3).
- **A6** — `BtRangeIter` ASC walk (suspicion set entry 1, streaming
  variant).
- **B1** — the exact failing wire shape plus controls through the real
  daemon (reproduces the CI failure in the same process run).
- **B2** — direct per-shard read of the real `indexes/amt/00N.idx`
  files: full walk vs bounded range, per shard (catches routing loss and
  confirms/rejects the seek defect on production-shaped files). The
  probe first computes each shard's expected record count
  (`compute_hash_raw` + `idx_shard_for_hash`), because btree files are
  created lazily: a shard with zero routed records legitimately has no
  file. An absent/unreadable file is a failure ONLY when records were
  expected to route there; present files must be readable + regular
  (`access`/`stat`) before the walkers run — `btree_walk_all_values()`
  returns 0 both for a valid empty walk and when `bt_acquire()` fails,
  and `btree_range()` is void, so without this gate an I/O/path problem
  would masquerade as an empty shard or a range bug.

### 2a — New file `src/test/cases/test_numeric_between_probe.c`

Create with exactly this content:

```c
/* src/test/cases/test_numeric_between_probe.c
 * TEMPORARY diagnostic probe for
 * docs/plans/2026-08-28-macos-arm64-numeric-between.md — NOT a
 * regression case. Expected to fail on macOS arm64 until the
 * numeric-between defect is root-caused and fixed; must pass 100% on
 * Linux. Delete together with the plan close-out.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "btree.h"
#include "types.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define PROBE_MULT 100LL   /* amt:numeric:10,2 → ×100 */

static const char *PROBE_VALS[5] = { "-999.99", "-0.01", "0", "0.01", "999.99" };
static const char *PROBE_LO = "-1";
static const char *PROBE_HI = "1";

/* Independent encoder: decimal string → scaled int64 → biased BE bytes.
   Mirrors the config.c FT_NUMERIC arithmetic (atof, ×mult, ±0.5 bias,
   ^1<<63, BE) so a mismatch vs encode_field_for_index() localizes
   toolchain divergence in that arithmetic itself. */
static void local_numeric_key(const char *dec, int64_t mult, uint8_t out[8]) {
    double dv = atof(dec);
    int64_t v = (int64_t)(dv * (double)mult + (dv >= 0 ? 0.5 : -0.5));
    uint64_t u = (uint64_t)v ^ (1ULL << 63);
    for (int i = 0; i < 8; i++) out[i] = (uint8_t)(u >> (56 - 8 * i));
}

static void probe_hex(const char *tag, const uint8_t *k) {
    TAP_DIAG("    %s %02X%02X%02X%02X%02X%02X%02X%02X\n", tag,
             k[0], k[1], k[2], k[3], k[4], k[5], k[6], k[7]);
}

/* Shared collectors — reset before each walk. */
static uint8_t seen[8][8];
static int n_seen, n_range, n_walk;

static int range_cb(const char *v, size_t vl, const uint8_t *h, void *ctx) {
    (void)h; (void)ctx;
    if (n_seen < 8 && vl == 8) memcpy(seen[n_seen], v, 8);
    n_seen++; n_range++;
    return 0;
}

static int walk_cb(const char *v, size_t vl, void *ctx) {
    (void)ctx;
    if (n_seen < 8 && vl == 8) memcpy(seen[n_seen], v, 8);
    n_seen++; n_walk++;
    return 0;
}

static int do_count(TestClient *tc, const char *obj, const char *crit) {
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":%s}", obj, crit);
    char *resp = NULL;
    tc_request(tc, req, &resp);
    int n = tu_parse_count(resp);
    free(resp);
    return n;
}

static void phase_a(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-probe-%d.idx", (int)getpid());
    unlink(path);

    /* A1 — local arithmetic vs hardcoded golden bytes. */
    static const struct { const char *dec; const uint8_t key[8]; } golden[7] = {
        { "-999.99", { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFE,0x79,0x61 } },
        { "-0.01",   { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF } },
        { "0",       { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x00 } },
        { "0.01",    { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x01 } },
        { "999.99",  { 0x80,0x00,0x00,0x00,0x00,0x01,0x86,0x9F } },
        { "-1",      { 0x7F,0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0x9C } },
        { "1",       { 0x80,0x00,0x00,0x00,0x00,0x00,0x00,0x64 } },
    };
    char desc[64];
    for (int i = 0; i < 7; i++) {
        uint8_t k[8];
        local_numeric_key(golden[i].dec, PROBE_MULT, k);
        TAP_DIAG("  A1 local %-8s ->", golden[i].dec); probe_hex("", k);
        snprintf(desc, sizeof(desc), "A1 local key %d matches golden", i);
        ASSERT_TRUE(memcmp(k, golden[i].key, 8) == 0, desc);
    }

    /* A2 — config.c encoder vs the same golden bytes. */
    TypedField f; memset(&f, 0, sizeof(f));
    f.type = FT_NUMERIC; f.size = 8; f.numeric_scale = 2;
    f.numeric_scale_mult = PROBE_MULT;
    for (int i = 0; i < 7; i++) {
        uint8_t k[32]; size_t klen = 0;
        encode_field_for_index(&f, golden[i].dec, strlen(golden[i].dec),
                               k, &klen);
        TAP_DIAG("  A2 config %-8s -> len=%zu", golden[i].dec, klen);
        probe_hex("", k);
        snprintf(desc, sizeof(desc), "A2 encode_field_for_index %d golden", i);
        ASSERT_TRUE(klen == 8 && memcmp(k, golden[i].key, 8) == 0, desc);
    }

    /* A3 — five inserts into a bare btree file. */
    for (int i = 0; i < 5; i++) {
        uint8_t k[8]; local_numeric_key(PROBE_VALS[i], PROBE_MULT, k);
        uint8_t hash[BT_HASH_SIZE]; memset(hash, 0, sizeof(hash));
        hash[15] = (uint8_t)(i + 1);
        int rc = btree_insert(path, (const char *)k, 8, hash);
        snprintf(desc, sizeof(desc), "A3 btree_insert %s", PROBE_VALS[i]);
        ASSERT_EQ_INT(rc, 0, desc);
    }

    /* A4 — full-leaf walk: what is physically stored (no bounds/seek). */
    n_walk = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_walk_all_values(path, walk_cb, NULL);
    TAP_DIAG("  A4 walk-all stored %d keys:", n_walk);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_walk, 5, "A4 walk-all sees 5 stored keys");

    /* A5 — inclusive range through both entry points. */
    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);
    probe_hex("  A5 lo", lo); probe_hex("  A5 hi", hi);

    n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_range(path, (const char *)lo, 8, (const char *)hi, 8,
                range_cb, NULL);
    TAP_DIAG("  A5 btree_range returned %d:", n_range);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_range, 3, "A5 btree_range [-1..1] returns 3");

    n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
    btree_range_ex(path, (const char *)lo, 8, 0, (const char *)hi, 8, 0,
                   range_cb, NULL);
    TAP_DIAG("  A5 btree_range_ex returned %d:", n_range);
    for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
    ASSERT_EQ_INT(n_range, 3, "A5 btree_range_ex [-1..1] returns 3");

    /* A6 — streaming iterator (suspicion set entry 1). */
    BtRangeIter *it = btree_range_iter_open(path, (const char *)lo, 8, 0,
                                            (const char *)hi, 8, 0, 0);
    ASSERT_NOT_NULL(it, "A6 btree_range_iter_open");
    int n_iter = 0;
    if (it) {
        const char *v; size_t vl; const uint8_t *h;
        TAP_DIAG("  A6 iter sequence:");
        while (btree_range_iter_next(it, &v, &vl, &h) == 1) {
            if (n_iter < 8 && vl == 8) {
                char t[16];
                snprintf(t, sizeof(t), "#%d", n_iter);
                probe_hex(t, (const uint8_t *)v);
            }
            n_iter++;
        }
        btree_range_iter_close(it);
    }
    ASSERT_EQ_INT(n_iter, 3, "A6 iter [-1..1] yields 3");

    unlink(path);
}

static void phase_b(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) { ASSERT_TRUE(0, "B env start"); return; }
    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "B connect");
    if (!tc) { test_env_stop(&env); return; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"bi_num\","
        "\"splits\":16,\"max_key\":16,\"fields\":[\"amt:numeric:10,2\"],"
        "\"indexes\":[\"amt\"]}", &resp);
    free(resp); resp = NULL;
    for (int i = 0; i < 5; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"bi_num\","
            "\"key\":\"n_%d\",\"value\":{\"amt\":%s}}", i, PROBE_VALS[i]);
        tc_request(tc, req, &resp); free(resp); resp = NULL;
    }

    /* B1 — the exact failing wire shape plus controls. */
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\","
        "\"value2\":\"1\"}]"),
        3, "B1 wire between -1 and 1 = 3 (expected red on macOS)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"lt\",\"value\":\"0\"}]"),
        2, "B1 wire lt 0 = 2 (control)");
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"gte\",\"value\":\"0\"}]"),
        3, "B1 wire gte 0 = 3 (control)");

    tc_close(tc);

    /* B2 — per-shard direct read of the real index files. BEFORE
       test_env_stop, which rm -rf's db_root; the idle daemon only holds
       read-only mappings, so concurrent reads are safe. Index files are
       created lazily — a shard with zero routed records has NO file at
       all — so compute the expected routing first (compute_hash_raw +
       idx_shard_for_hash: the same derivation the daemon performs) to
       distinguish a legitimately absent file from a lost one. */
    int nshards = index_splits_for(16);
    TAP_DIAG("  B2 index_splits_for(16) = %d", nshards);
    int expected[8] = {0};          /* index_splits_for(16) == 4 */
    for (int i = 0; i < 5; i++) {
        char key[16];
        snprintf(key, sizeof(key), "n_%d", i);
        uint8_t h[16];
        compute_hash_raw(key, strlen(key), h);
        expected[idx_shard_for_hash(h, 16)]++;
    }
    char desc[64];
    uint8_t lo[8], hi[8];
    local_numeric_key(PROBE_LO, PROBE_MULT, lo);
    local_numeric_key(PROBE_HI, PROBE_MULT, hi);
    int total_walked = 0, total_ranged = 0, n_problems = 0;
    for (int s = 0; s < nshards; s++) {
        char p[512];
        build_idx_path(p, sizeof(p), env.db_root, "default/bi_num", "amt", s);
        TAP_DIAG("  B2 shard %d (%s) expected=%d", s, p, expected[s]);

        /* A walker cannot report an open failure: btree_walk_all_values()
           returns 0 both for a valid empty walk and when bt_acquire()
           fails, and btree_range() is void. Gate both walkers on one
           combined failure branch over readability/regularity. An
           absent file is a failure ONLY if records were expected to
           route there; a zero-routed shard legitimately has no file. */
        int readable = (access(p, R_OK) == 0);
        struct stat st;
        int regular = (stat(p, &st) == 0 && S_ISREG(st.st_mode));
        if (!readable || !regular) {
            if (expected[s] == 0) {
                TAP_DIAG("  B2 shard %d has no file; 0 records routed —"
                         " legitimately absent, walkers skipped\n", s);
                continue;
            }
            snprintf(desc, sizeof(desc),
                     "B2 shard %d (routes %d records) file readable+regular",
                     s, expected[s]);
            ASSERT_TRUE(0, desc);
            TAP_DIAG("  B2 shard %d (%s) readable=%d regular=%d — walkers skipped\n",
                     s, p, readable, regular);
            n_problems++;
            continue;
        }
        TAP_DIAG("  B2 shard %d size=%lld bytes", s, (long long)st.st_size);

        n_walk = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
        btree_walk_all_values(p, walk_cb, NULL);
        TAP_DIAG("  B2 shard %d stores %d:", s, n_walk);
        for (int i = 0; i < n_seen && i < 8; i++) probe_hex("", seen[i]);
        total_walked += n_walk;

        snprintf(desc, sizeof(desc),
                 "B2 shard %d stores its routed records", s);
        ASSERT_EQ_INT(n_walk, expected[s], desc);

        n_range = 0; n_seen = 0; memset(seen, 0, sizeof(seen));
        btree_range(p, (const char *)lo, 8, (const char *)hi, 8,
                    range_cb, NULL);
        TAP_DIAG("  B2 shard %d btree_range returned %d", s, n_range);
        total_ranged += n_range;
    }
    TAP_DIAG("  B2 totals: walked=%d ranged=%d problems=%d",
             total_walked, total_ranged, n_problems);
    ASSERT_EQ_INT(n_problems, 0,
                  "B2 every routed shard file present, readable, regular");
    ASSERT_EQ_INT(total_walked, 5, "B2 walk-all totals 5 stored keys");
    ASSERT_EQ_INT(total_ranged, 3, "B2 per-shard ranges total 3");

    test_env_stop(&env);
}

static int test_numeric_between_probe_run(void) {
    phase_a();
    phase_b();
    return t_ctx->failed > 0 ? 1 : 0;
}
TEST_REGISTER("test-numeric-between-probe", test_numeric_between_probe_run)
```

### 2b — Register the case in build.sh

The test binary only links cases listed in `build.sh`. Insert the new
case file immediately after the existing anchor line
(`src/test/cases/test_binary_index.c \`):

Anchor (unique):

```
    src/test/cases/test_binary_index.c \
```

Replace with:

```
    src/test/cases/test_binary_index.c \
    src/test/cases/test_numeric_between_probe.c \
```

### 2c — Local validation (Linux must be fully green)

The probe is only a valid differential instrument if it passes cleanly
on Linux (where `test-binary-index` passes 22/0):

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-numeric-between-probe
```

- Expected: every assertion `ok` (A1–A6, B1, B2), exit 0.
- If any assertion fails on Linux, the probe itself is wrong — stop and
  report the TAP output; do not adjust the probe's golden bytes to make
  it pass (they are derived independently in Verified fact 8 and are the
  point of the exercise).
- Also run the neighboring case once to confirm no cross-case pollution:
  `./build/bin/shard-db-test run test-binary-index` → 22/0.

Leave the changes uncommitted per this repo's execution mode.

## Task 3 — CI probe run on all three legs (scratch branch)

### 3a — Temporary workflow step

The dedicated step must run **before** the full-suite step, because on
the scratch branch the macOS suite step will fail (the probe is expected
red there) and would otherwise skip later steps.

In `.github/workflows/ci.yml`, anchor — the exact line:

```
      - name: Run full C test suite
```

Insert immediately **above** it:

```yaml
      # TEMPORARY (scratch branch only) — diagnostic probe for
      # docs/plans/2026-08-28-macos-arm64-numeric-between.md. Runs before
      # the suite step because the macOS suite step is expected to fail
      # on this branch. Remove with the plan close-out.
      - name: PROBE (scratch) — numeric between diagnosis
        run: ./build/bin/shard-db-test run test-numeric-between-probe
```

No `if:` condition — it runs on all three legs so Linux output serves as
the baseline in the same run.

### 3b — Git operations (human-run or explicitly directed)

CI triggers only via PR to `main` (`on: pull_request: branches:[main]`),
so the scratch branch needs a PR. Per this repo's git-safety rules, the
human runs these, or explicitly directs the agent to run this exact
sequence in the moment:

```bash
git checkout -b diag/macos-numeric-between-probe main
git add src/test/cases/test_numeric_between_probe.c build.sh \
        .github/workflows/ci.yml \
        docs/plans/2026-08-28-macos-arm64-numeric-between.md
git commit -m "test: temporary numeric-between diagnostic probe (scratch)"
git push -u origin diag/macos-numeric-between-probe
gh pr create --draft --title "scratch: numeric-between probe (do not merge)" \
  --body "Diagnostic only — see docs/plans/2026-08-28-macos-arm64-numeric-between.md (committed on this branch) Task 3. Never merge."
```

The plan file is included so the PR is self-contained: reviewers see the
Task 1 evidence and the interpretation in the diff, and the PR-body link
resolves on the branch.

The PR is never merged; the branch is deleted after evidence is
captured (Task 4).

### 3c — Evidence capture, durability, and read-out

1. Wait for all three legs; from each leg's "PROBE (scratch)" step log,
   save the full TAP output (assertions + `A*`/`B*` hex dumps) to local
   files (e.g. `/tmp/probe-linux-x86_64.log`, `…-linux-arm64.log`,
   `…-macos-arm64.log`).
2. **Make the record durable before any cleanup** (this is the step that
   keeps the diagnostic from disappearing with the scratch branch):
   a. Append to this plan file: the Task 1 CI block (if not already
      committed), the three per-leg outputs under
      `## Evidence — Task 3`, each with a one-line interpretation.
   b. Commit and push the plan file to the scratch branch (human-run or
      human-directed):

      ```bash
      git add docs/plans/2026-08-28-macos-arm64-numeric-between.md
      git commit -m "docs: numeric-between probe evidence (Task 1/3)"
      git push
      ```

      The PR diff now carries the full evidence for review.
   c. Also post the three full logs as PR comments:

      ```bash
      gh pr comment --body-file /tmp/probe-linux-x86_64.log
      gh pr comment --body-file /tmp/probe-linux-arm64.log
      gh pr comment --body-file /tmp/probe-macos-arm64.log
      ```

      PR comments survive branch deletion — this is the durable copy
      that Task 4's cleanup cannot destroy. Both (b) and (c) must hold
      before Task 4 may delete anything.
3. Localize using this table (first failing layer wins):

   | Failing assert (macOS only) | Localization |
   |---|---|
   | A1 | Apple-toolchain arithmetic divergence — contradicts fact 3; investigate compiler flags/UB in the encoding expression |
   | A2 (A1 green) | `encode_field_for_index` diverges on macOS |
   | A4 (A1/A2 green) | insert path loses/corrupts keys at the `0x7F→0x80` boundary — btree_insert/split |
   | A5 (A4 green) | seek/compare defect in `btree_range`/`btree_range_ex` — suspicion set entries 1/3 confirmed at btree.c level |
   | A6 (A5 green) | `BtRangeIter`-specific defect |
   | B1 (all A green) | defect above btree.c — planner bound encoding (`encode_criterion_value`), `btree_idx_*` fan-out (`shard_walk_dispatch`/`parallel_for_io`), or the count callback |
   | B2 "…routes N records) file readable+regular" assert fails / `problems ≠ 0` | a shard WITH routed records has a missing/unreadable/non-regular file — path construction or daemon cleanup problem; NOT a routing or range defect |
   | B2 per-shard "stores its routed records" fails | an entry is misplaced or lost for that shard — routing/index-build defect |
   | B2 walked ≠ 5 (per-shard asserts passing) | arithmetically impossible — treat as a probe bug; stop |
   | B2 walked = 5 but ranged ≠ 3 | seek defect confirmed on production-shaped files |

4. The differential between Linux (green) and macOS (red) output — same
   code, same golden bytes — is the root-cause evidence.

## Task 4 — HALT: report root cause; re-plan the fix (not in this plan)

This plan does not authorize fix code. After Task 3:

1. **Stop.** Post the evidence summary: failing assert → localization
   row, both platform outputs, and the specific defect mechanism
   (function + line + why the `0x7F→0x80` first-byte boundary triggers
   it). "Fix direction" from the pre-plan revision is a hypothesis
   ranking, not authorization.
2. The human approves an amended/follow-up fix plan, which must contain:
   - the root cause as a specific mechanism (not just where it's
     observed);
   - complete code blocks for every hunk;
   - a codebase-search listing of every consumer of the function being
     changed;
   - the regression proof: `test-binary-index` assert 12 fails on macOS
     at base (existing CI history is the fail-before evidence — paste
     the Task 1 log block again alongside the after-fix green run);
   - the close-out edits below (Acceptance) as explicit tasks.
3. Platform rule for the fix: the defect is platform-neutral C; the fix
   must be platform-neutral too. No `#ifdef __APPLE__` anywhere unless
   byte-level evidence from Task 3 demands it — and if it does, that is
   a decision to escalate to the human, not improvise.
4. Clean up the scratch branch **only after both durability conditions
   from Task 3c step 2 hold**: the evidence commit is pushed to the
   branch AND the three logs are posted as PR comments. Then close —
   never merge — the PR and delete the branch. The closed PR and its
   comment thread remain permanently accessible as the diagnostic
   record; this plan file's `## Evidence` sections become the in-repo
   copy once the fix PR lands.

## Acceptance (whole effort — fix plan's gate, restated here)

1. `test-binary-index` passes 22/0 on macOS arm64, Linux x86_64, and
   Linux arm64 CI legs.
2. `ci.yml` exclusion removed — delete the comment block anchored by
   `# test-binary-index: pre-existing macOS-arm64-only failure (numeric`
   through `# docs/plans/2026-08-28-macos-arm64-numeric-between.md`, and
   remove `,test-binary-index` from the `SHARD_TEST_EXCLUDE` value line.
3. Probe scaffolding deleted: `src/test/cases/test_numeric_between_probe.c`,
   its `build.sh` list line, and the `PROBE (scratch)` workflow step.
4. Sanitizer gate on the fix diff (btree.c is shared cached state — this
   repo's standing gate applies, locally, before "done"):
   - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then three fresh runs of
     `./build/bin/shard-db-test run-all`;
   - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then three fresh runs of
     `TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all`.
5. No new compiler warnings; no leftover probe/debug prints.
6. This plan file's Status line updated to done with the root cause
   recorded in one paragraph.

## Invariants (unchanged from pre-plan revision)

- Do not adjust `test-binary-index`'s expectation.
- No per-platform encoding changes without byte-level evidence from
  Task 3.
- The fix goes where the defect is (root cause), not where the symptom
  is observed.

## Evidence — Task 1

From CI run 33174772843, `Build & test (macos arm64)`, 2026-08-28:

```text
ok 9 - between -1000 and 1000 = 3
ok 10 - gt -2 → 5
ok 11 - numeric lt 0 → 2 negatives
not ok 12 - numeric between -1 and 1 = 3
#   expected 3 got 2
ok 13 - date between 2022 and 2027 = 1
ok 14 - date gte 2026 = 1
```

`got 2` means one in-range key is missing: the lower-bound seek likely
overshoots the `-1` key (`0x7F…FF`), or skips a leaf/page.

## Evidence — Task 3

CI run 33300760755, scratch PR #319:

- Linux x86_64: `test-numeric-between-probe` passed 34/0.
- Linux arm64: the probe step passed.
- macOS arm64: A1–A6 and B2 passed; B1 alone failed with `expected 3 got 2`.

The direct btree range and production-shaped per-shard ranges are correct;
the failure is in the upper-layer indexed count path.

## Task 4 — halt report

Round one localizes the defect to the B1 row: all arithmetic, encoder,
direct-btree, and physical-index probes pass, but the daemon wire count loses
one match. Suspects are `idx_count_cb` TLS batching/flush, fan-out dispatch,
planner-produced criteria, or deadline handling. No fix code was written.
