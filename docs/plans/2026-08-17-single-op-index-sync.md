# Plan: single-op index durability — fix uninit trigram/delete sync flags, batch syncs at hook end

Date: 2026-08-17 (Rev 3 — Rev 2 incorporated external review: fourth delete
hook, bitmap-conditional sync flag, written-fields-only collection. Rev 3:
corrects the Task 5.2 neuter expectations for the insert case and captures
the flush errno in `index_parallel` so real fdatasync errors aren't masked
as generic EIO.)
Branch: `perf/single-op-index-sync` (create from `main`; per AGENTS.md execution
mode, leave all work **uncommitted** at the end for review)
Status: awaiting approval
Companion plan: `docs/plans/2026-08-17-bulk-sync-batching.md` (independent;
see "Interaction with the bulk plan" below)

## Problem

Single-key indexed inserts/updates/deletes apply their secondary-index
mutations with per-field `fdatasync` interleaved into the mutation dispatch,
**two** paths pass an **uninitialized** `sync_after` flag, and the collector
must not touch bitmap fields' own sync or never-written files.

1. **Bug — two uninitialized `sync_after` sites.**
   a. `v2_insert_apply_commit` declares `UpdateIdxArg tg_args[MAX_FIELDS];`
      without zeroing and fills every field **except** `sync_after`
      (storage.c sets `db_root … bm_max_values` only; `update_idx_fn` reads
      `a->sync_after` in the IT_TRIGRAM case). Stack garbage per call: when
      it reads 0, trigram files are **never fdatasynced before the marker is
      cleared** on a single insert (power-loss exposure — index state lagging
      a cleared marker; recovery fails closed).
   b. `v2_delete_apply_commit` (storage.c:1666–1730) — the delete diff hook,
      a separate copy of the diff engine that does NOT route through
      `apply_index_diff`. Its arg fill (storage.c:1701–1714) likewise omits
      `sync_after`. Every indexed single delete on a btree/trigram field
      syncs only if stack garbage says so.
   Both are plain UB (uninitialized read). ASan/UBSan will not flag them;
   only MSan-class tooling or inspection catches them — found by inspection.
2. **Per-field syncs interleaved with mutations, issued serially.** The
   upsert diff (`v2_insert_pre_commit`), the shared diff engine
   (`apply_index_diff`), the delete hook (`v2_delete_apply_commit`), and the
   insert path (`index_parallel` + tg staging) each fdatasync inside the
   per-field worker, so N indexed fields cost N serialized flush latencies
   inside the commit hook — the dominant tail of single-op latency on
   multi-index objects.
3. **Protocol inconsistency with the bulk path** after the companion plan
   lands. This plan gives single ops the same shape: mutate all fields
   (already parallel via `parallel_for`), then flush the touched files once —
   in parallel.

For a single record one hash routes to **one idx shard per field**, so the
touched set is exactly one file per changed non-bitmap field. Flushes target
distinct files (path embeds the field name), so issuing them concurrently is
safe; `btree_sync_path` = `bt_acquire` (locked cache) + `fdatasync` +
`bt_release`, thread-safe by design.

**Bitmap fields are excluded from the collector but keep their own sync.**
`bm_sync` must run while the `BitmapShard` writer handle is open inside
`bitmap_update` / the bitmap prepare-set apply. Therefore at the 4a/4b/4d
arg-fill sites the flag becomes **type-conditional**: bitmap args keep
`sync_after = 1` (their `bm_sync` fires inside `update_idx_fn`), btree/trigram
args get `0` and are flushed by the collector at hook end. A blanket `= 0`
would silently drop bitmap durability on update/delete/upsert paths — and the
crash tests can't force the tiny SIGKILL window that would expose it, so the
correctness lives in the flag and a counted test (Task 2 case 5).

**The collector only syncs files that actually received entries this op.**
`bt_acquire` with `writer=1` O_CREATs + ftruncate + `bt_init_file`s a missing
file (btree.c "fresh-file creation (O_CREAT + ftruncate + bt_init_file)"
writer path), and create-object only mkdirs `indexes/` — index files are
created lazily by first write. Syncing a never-written field would
materialize an empty `.idx`/`.tg` file (du-visible, once per field/shard).
So: update/delete/upsert sites collect only *changed* args (their files just
had mutations); the insert path flushes inside `index_parallel` (which knows
exactly which fields built keys) plus the tg staging set. Accepted cosmetic
edge: a trigram-indexed field whose value is <3 bytes (zero distinct
trigrams) still gets one empty `.tg` from the collector — harmless, once.

## Ordering invariants (must hold after every task)

- I1: every index file mutation of this op is durable **before** the commit
  hook returns — the slotcask caller clears the commit-intent marker only
  after the hooks and kf publish complete. This covers **four** hook
  flavors: `v2_insert_pre_commit` (legacy single-phase upsert/update diff),
  `v2_insert_apply_commit` (two-phase insert apply, post-marker),
  `apply_index_diff` (via `v2_update_pre_commit`, `v2_update_apply_commit`,
  and recovery replay), and `v2_delete_apply_commit` (post-marker delete
  diff).
- I2: failure of any mutation or any sync fails the hook (`return -1`),
  routing into the existing replay / abort-sidecar / fail-closed handling.
  A sync failure must not be swallowed.
- I3: bitmap durability is unchanged end-to-end. Bitmap args at the 4a/4b/4d
  sites keep `sync_after = 1` so `bm_sync` fires inside `bitmap_update` under
  the open writer handle; the insert staging site
  (`v2_insert_prepare_commit`'s `arg.sync_after = 1;`) is untouched; the
  collector filters `IT_BITMAP`. Asserted by the counted test (case 5) and
  the Task 5.2 neuter — without the counter this class is untestable in
  process, which is why the counter exists.
- I4: recovery replay (`storage_recovery_index_diff` → `apply_index_diff`)
  gets the same end-of-diff flush, so replayed index state is durable before
  the replayed marker is cleared.

## Non-goals

- Marker write/clear, segment msync, kf msync on single ops — already
  minimal per op (one each); untouched.
- Bitmap sync mechanics (`bm_sync` call sites inside bitmap.c /
  `bitmap_prepare_set_apply`) — untouched; only the flag that gates them at
  the three arg-fill sites changes (to a conditional that preserves 1).
- Group commit / batching across concurrent ops — architectural, out of scope.
- The bulk plan's files (query_bulk.c, slotcask.c window loops) — untouched.

Caller/consumer enumeration for everything this plan changes:

- `index_sync_record_fields` (new) — called from `v2_insert_pre_commit`,
  `apply_index_diff`, `v2_delete_apply_commit`, `v2_insert_apply_commit` (tg
  set), and `index_parallel` (its written btree set). No external consumers.
- `index_parallel` — **exactly one caller** (storage.c, `v2_insert_apply_commit`;
  verified by grep). Its signature changes: the trailing `int sync_after`
  parameter is **removed** (the only caller now wants end-flush behavior, and
  per-thread sync would fight it). Prototype in the header declaring it must
  drop the parameter accordingly.
- `IndexThreadArg` (index.c-local) — `sync_after` field removed along with
  the sync block in `index_thread_fn`; no other users (grep-verified).
- `apply_index_diff` — three callers, all in storage.c
  (`storage_recovery_index_diff`, `v2_update_pre_commit`,
  `v2_update_apply_commit`); signature unchanged, all three get the new
  behavior via one internal change.
- `update_idx_fn`'s `sync_after` handling — unchanged.

## Interaction with the bulk plan

Both plans edit `btree.c` (sync counter) and conceptually `index.c`. Anchors
below are from current `main`. Two anticipated branches — planned cases, not
anchor mismatches:

- If `btree_test_sync_reset` / `btree_test_sync_count` already exist in
  `btree.c`/`btree.h` (bulk plan Task 2a landed first), **skip that insertion**
  and reuse them. Do not add a second counter.
- If `btree_sync_path` already contains a TEST_BUILD counter increment, skip
  that hunk.
- Any *other* anchor mismatch: write `PLAN_NOTES.md` and halt per the
  execution rules.

## Verification note (read before writing tests)

Same documented deviation as the bulk plan: the observables are syscall
counts/ordering and uninitialized stack bytes, none of which fail
deterministically on the base branch (btree-only counts are identical
before/after; trigram/delete counts on base are garbage-dependent). Each
assertion ships with a **neuter proof**: disable only the named semantic
line, confirm the test fails for the expected reason, restore, confirm green.
Paste both outputs.

## Execution rules

- Branch off `main`: `git checkout -b perf/single-op-index-sync`. Do tasks in
  order. Leave work uncommitted (AGENTS.md standing exception).
- Build: `SKIP_TESTS=1 ./build.sh`. Single case:
  `./build/bin/shard-db-test run test-single-op-index-sync`; full:
  `./build/bin/shard-db-test run-all`.
- If a quoted anchor isn't found exactly (and isn't one of the anticipated
  branches above), write `PLAN_NOTES.md` and halt the entire execution run —
  do not guess, reinterpret, or continue any further task. Resuming requires
  the human (or the planning model, re-engaged) to hand back a patched or
  fresh plan.
- If you hit a decision the plan doesn't cover, stop and ask.
- Never weaken a test to make a failure disappear.

---

## Task 0 — branch

```bash
git checkout -b perf/single-op-index-sync
```

## Task 1 — sync counters (skip a counter if the bulk plan added it)

### 1a. `btree_sync_path` counter — `src/db/btree.c`

Inside the existing TEST_BUILD gate area (anchor: the `#ifdef TEST_BUILD`
block containing `btree_test_delete_gate_is_bypassed`), add:

```c
#ifdef TEST_BUILD
#include <stdatomic.h>
static _Atomic int g_test_btree_sync_count;
void btree_test_sync_reset(void) { atomic_store(&g_test_btree_sync_count, 0); }
int  btree_test_sync_count(void) { return atomic_load(&g_test_btree_sync_count); }
#endif
```

In `int btree_sync_path(const char *path) {`, before the `bt_acquire` call,
add:

```c
#ifdef TEST_BUILD
    atomic_fetch_add(&g_test_btree_sync_count, 1);
#endif
```

In `src/db/btree.h`, adjacent to the `btree_sync_path` declaration:

```c
#ifdef TEST_BUILD
void btree_test_sync_reset(void);
int  btree_test_sync_count(void);
#endif
```

### 1b. `bm_sync` counter — `src/db/bitmap.c`

`bm_sync` (anchor: `int bm_sync(BitmapShard *bm) {` whose body is the
`if (!bm || !bm->writer || bm->fd < 0) { errno = EINVAL; return -1; }`
guard followed by `return fdatasync(bm->fd);`) is the single flush choke
point for bitmap files. Add above it:

```c
#ifdef TEST_BUILD
#include <stdatomic.h>
static _Atomic int g_test_bm_sync_count;
void bm_test_sync_reset(void) { atomic_store(&g_test_bm_sync_count, 0); }
int  bm_test_sync_count(void) { return atomic_load(&g_test_bm_sync_count); }
#endif
```

and between the guard and the return:

```c
#ifdef TEST_BUILD
    atomic_fetch_add(&g_test_bm_sync_count, 1);
#endif
```

Declare the two accessors `#ifdef TEST_BUILD` in whichever header declares
`bm_sync` (locate via `grep -rn "int bm_sync(" src/db/*.h`).

## Task 2 — test first (red: link error or wrong counts)

`src/test/cases/test_single_op_index_sync.c` (registered name
`test-single-op-index-sync`), process-db pattern per
`test_bulk_update_json_oom.c`. Object has 2 btree + 1 trigram + 1 bool field
(bool auto-defaults to a bitmap index at create-object — types.h
"Auto-default for bool fields at create-object"), so both counters are
exercised and the bitmap-conditional fix is regression-guarded:

```c
/* src/test/cases/test_single_op_index_sync.c
 *
 * Single-op index durability shape: one indexed op issues exactly one
 * fdatasync per changed non-bitmap field (btree_sync_path counter), bitmap
 * fields keep exactly one bm_sync per changed bitmap field (bm_sync
 * counter), and the trigram/delete paths sync deterministically (base:
 * stack garbage decided them). Case 5 is the regression guard for the
 * bitmap-conditional sync flag.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include "../db/types.h"
#include "../db/shard_db.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern void btree_test_sync_reset(void);
extern int  btree_test_sync_count(void);
extern void bm_test_sync_reset(void);
extern int  bm_test_sync_count(void);

static int req_ok(ShardDb *db, const char *json, const char *what) {
    char *resp = NULL;
    int rc = tu_pdb_request(db, json, &resp);
    int ok = rc == 0 && resp && strstr(resp, "\"error\"") == NULL;
    if (!ok) fprintf(stderr, "req failed (%s): %s\n", what, resp ? resp : "(null)");
    shard_db_free_result(resp);
    return ok;
}

static int test_single_op_index_sync_run(void) {
    ShardDb *db = test_get_process_db();
    ASSERT_NOT_NULL(db, "process db");
    if (!db) return 1;

    ASSERT_TRUE(req_ok(db, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", "add-dir"),
                "add-dir");
    /* 2 btree + 1 trigram + 1 bool (auto-bitmap) field. */
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"splits\":8,\"max_key\":16,\"fields\":["
        "\"a:varchar:32\",\"b:varchar:32\",\"t:varchar:64\",\"flag:bool\"],"
        "\"indexes\":[\"a\",\"b\",\"t:trigram\",\"flag\"]}", "create"), "create-object");

    /* 1) Fresh insert touching all four fields → 3 btree/tg syncs
          (a, b via index_parallel's internal flush; t via the tg collector)
          + 1 bm_sync (flag, via bitmap prepare/apply). On base the trigram
          leg is garbage-dependent. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha\",\"b\":\"beta\","
        "\"t\":\"trigram text one\",\"flag\":true}}", "insert"), "insert");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "insert: 1 sync per non-bitmap field");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "insert: bitmap field syncs once");

    /* 2) Update changing one btree field → 1 btree sync, 0 bm. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha2\"}}", "update-one"), "update 1 field");
    ASSERT_EQ_INT(btree_test_sync_count(), 1, "update: only changed field syncs");
    ASSERT_EQ_INT(bm_test_sync_count(), 0, "update: no bitmap change, no bm sync");

    /* 3) REGRESSION GUARD (bitmap-conditional flag): update changing only
          the bool field → 0 btree syncs, exactly 1 bm_sync. A blanket
          sync_after=0 at the arg-fill site makes this 0/0 — silent bitmap
          durability loss the crash tests cannot catch. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"flag\":false}}", "update-flag"), "update flag");
    ASSERT_EQ_INT(btree_test_sync_count(), 0, "flag-only update: no btree syncs");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "flag-only update: bitmap still syncs");

    /* 4) Update changing all four → 3 btree + 1 bm. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\",\"value\":{\"a\":\"alpha3\",\"b\":\"beta3\","
        "\"t\":\"trigram text two\",\"flag\":true}}", "update-all"), "update 4 fields");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "update: 3 changed non-bitmap sync");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "update: changed bitmap syncs once");

    /* 5) Delete → the OLD record had all four values → 3 btree/tg + 1 bm.
          Exercises v2_delete_apply_commit (the fourth hook flavor); on base
          its counts are garbage-dependent. */
    btree_test_sync_reset(); bm_test_sync_reset();
    ASSERT_TRUE(req_ok(db,
        "{\"mode\":\"delete\",\"dir\":\"default\",\"object\":\"ssox_t\","
        "\"key\":\"k1\"}", "delete"), "delete");
    ASSERT_EQ_INT(btree_test_sync_count(), 3, "delete: removed entries sync");
    ASSERT_EQ_INT(bm_test_sync_count(), 1, "delete: cleared bitmap syncs");

    tu_pdb_drop_object(db, "default", "ssox_t");
    return 0;
}
TEST_REGISTER("test-single-op-index-sync", test_single_op_index_sync_run)
```

If any count differs from the asserted value after Tasks 3–4 are correctly
applied, stop and write `PLAN_NOTES.md` — do not adjust assertions to match.

Run: must fail (link error or wrong counts). Paste output.

## Task 3 — collector helper in `index.c`

Insert directly above `void *index_thread_fn(void *arg) {` (anchor: the
`IndexThreadArg` struct's closing `} IndexThreadArg;`):

```c
/* Flush the (field, idx-shard) files one record's index mutations touched —
   for a single record that is exactly one file per non-bitmap field — and
   issue the flushes in parallel: they target distinct files, so they are
   independent, and this collapses N serialized flush latencies into ~one.
   Must complete before the calling commit hook returns (invariant I1: index
   durable before marker clear). Bitmap fields are the caller's
   responsibility (bm_sync runs inside the bitmap apply paths while the
   writer handle is open) — callers must filter them out.
   Callers must pass only fields that actually received entries this op:
   bt_acquire(writer) O_CREATs missing files, so syncing a never-written
   field would materialize an empty .idx/.tg.
   Returns 0, or -1 if any flush failed. */
typedef struct {
    char path[PATH_MAX];
    int rc;
} IdxSyncArg;

static void *idx_sync_thread_fn(void *p) {
    IdxSyncArg *a = (IdxSyncArg *)p;
    a->rc = btree_sync_path(a->path);
    return NULL;
}

int index_sync_record_fields(const char *db_root, const char *object, int splits,
                             const uint8_t hash16[16],
                             const char *const *fields,
                             const enum IndexType *types, int nfields) {
    if (nfields <= 0 || !fields) return 0;
    IdxSyncArg *args = malloc((size_t)nfields * sizeof(*args));
    if (!args) {
        /* OOM: serial fallback — same files, same ordering, no dispatch. */
        int rc = 0;
        for (int i = 0; i < nfields; i++) {
            if (types && types[i] == IT_BITMAP) continue;
            char path[PATH_MAX];
            int shard = idx_shard_for_hash(hash16, splits);
            if (types && types[i] == IT_TRIGRAM)
                tg_build_path(path, sizeof(path), db_root, object, fields[i], shard);
            else
                build_idx_path(path, sizeof(path), db_root, object, fields[i], shard);
            if (btree_sync_path(path) != 0) rc = -1;
        }
        return rc;
    }
    int n = 0;
    for (int i = 0; i < nfields; i++) {
        if (types && types[i] == IT_BITMAP) continue;
        int shard = idx_shard_for_hash(hash16, splits);
        if (types && types[i] == IT_TRIGRAM)
            tg_build_path(args[n].path, sizeof(args[n].path), db_root, object,
                          fields[i], shard);
        else
            build_idx_path(args[n].path, sizeof(args[n].path), db_root, object,
                           fields[i], shard);
        args[n].rc = 0;
        n++;
    }
    int rc = 0;
    if (n == 1) {
        idx_sync_thread_fn(&args[0]);   /* skip dispatch for the common case */
    } else if (n > 1) {
        parallel_for(idx_sync_thread_fn, args, n, sizeof(IdxSyncArg));
    }
    for (int i = 0; i < n; i++)
        if (args[i].rc != 0) rc = -1;
    free(args);
    return rc;
}
```

Declaration in `src/db/types.h`, adjacent to the `update_idx_fn` declaration:

```c
/* Flush the touched (field, idx-shard) files for one record's index
   mutations, in parallel. Commit hooks call this after applying mutations,
   before returning (index durable before marker clear). Bitmap fields must
   be filtered out by the caller, and only fields that actually received
   entries may be passed (writer acquire creates missing files). */
int index_sync_record_fields(const char *db_root, const char *object, int splits,
                             const uint8_t hash16[16],
                             const char *const *fields,
                             const enum IndexType *types, int nfields);
```

## Task 4 — storage.c / index.c call-site changes

### 4a. `v2_insert_pre_commit` (update-resolved upsert diff)

1. Declaration — anchor `UpdateIdxArg args[MAX_FIELDS];` whose preceding line
   is `char *old_json = typed_decode(c->idx_ts, old->value, (uint32_t)old->vlen);`;
   add after it:

```c
        const char *ch_fields[MAX_FIELDS];
        enum IndexType ch_types[MAX_FIELDS];
```

2. Inside the `if (changed) {` block, anchor
   `args[n_args].bm_max_values = 0;  /* default cap — header wins on existing */`
   followed by `args[n_args].sync_after = 1;` — replace those two lines with
   (note ch_types is assigned **before** the flag reads it):

```c
                args[n_args].bm_max_values = 0;  /* default cap — header wins on existing */
                ch_fields[n_args] = c->fields[i];
                ch_types[n_args] = c->idx_types ? c->idx_types[i] : IT_BTREE;
                /* Bitmap keeps sync_after=1 so bm_sync fires inside
                   bitmap_update under its open writer handle (I3);
                   btree/trigram durability moves to the collector below. */
                args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
```

3. After the error-capture loop, anchor `        free(old_json);` followed by
   `        bm_flush_thread_bitmap_cache();` — insert the flush before
   `free(old_json);`:

```c
        if (n_args > 0 &&
            index_sync_record_fields(c->db_root, c->object, c->splits,
                                     c->hash, ch_fields, ch_types,
                                     n_args) != 0)
            idx_failed = 1;
        free(old_json);
```

### 4b. `apply_index_diff` (shared: single update/delete pre_commit,
two-phase apply, recovery replay)

1. In the `if (changed) {` block, anchor the sequence
   `args[n_args].hash    = a->hash;` (the `a->` form) down through
   `args[n_args].sync_after = 1;` — replace the last line with and append:

```c
            ch_fields[n_args] = a->idx_fields[i];
            ch_types[n_args] = a->idx_types ? a->idx_types[i] : IT_BTREE;
            /* Bitmap keeps sync_after=1 (I3); btree/trigram move to the
               collector below. */
            args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
```

2. Declaration — add next to this function's `UpdateIdxArg args[MAX_FIELDS];`
   (the one in the function whose error capture uses `a->err_buf`):

```c
    const char *ch_fields[MAX_FIELDS];
    enum IndexType ch_types[MAX_FIELDS];
```

3. After its error-capture loop (the one using
   `capture_index_update_error(a->err_buf, a->err_buf_len,`), anchor
   `    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);` — insert before it:

```c
    if (n_args > 0 &&
        index_sync_record_fields(a->db_root, a->object, a->splits,
                                 a->hash, ch_fields, ch_types,
                                 n_args) != 0)
        idx_failed = 1;
```

### 4c. `v2_insert_apply_commit` (fresh-insert apply, post-marker) +
`index_parallel` (written-fields-only flush)

1. `index_parallel` signature (index.c, anchor `const enum IndexType *types, int sync_after) {`)
   — drop the parameter:

```c
int index_parallel(const char *db_root, const char *object, int splits,
                   const char *value, const uint8_t hash16[16],
                   char fields[][256], int nfields,
                   const enum IndexType *types) {
```

   Update the prototype in the header that declares `index_parallel`
   (locate via `grep -rn "int index_parallel(" src/db/*.h`) to match — the
   single caller is `v2_insert_apply_commit` (enumerated in Non-goals).

2. Remove the now-dead per-thread sync: in `IndexThreadArg` delete the
   `int sync_after;` member (anchor: `const uint8_t *hash16;` followed by
   `int sync_after;`), and in `index_thread_fn` delete the block from
   `if (!a->out_error && a->sync_after) {` through its closing `}` (the one
   containing `build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object, a->field, shard);`).
   In the build loop, delete `args[tcount].sync_after = sync_after;`
   (anchor: `args[tcount].hash16 = hash16;` followed by it).

3. Internal end-flush in `index_parallel` — the build loop only creates args
   for fields that produced a non-empty key (anchor:
   `if (!key_buf || key_len == 0) { free(key_buf); continue; }`), so
   `args[0..tcount)` is exactly the written set. After the error-capture
   loop, anchor:

```c
    for (int i = 0; i < tcount; i++) {
        if (args[i].out_error && rc == 0) {
            rc = -1;
            saved_errno = args[i].out_errno;
        }
        free(idx_keys[i]);
    }
```

   insert after it:

```c
    if (tcount > 0) {
        /* Flush only the files this call actually wrote. Syncing a
           never-written field would O_CREAT an empty .idx via the writer
           acquire path. */
        const char *sync_fields[MAX_FIELDS];
        enum IndexType sync_types[MAX_FIELDS];
        for (int i = 0; i < tcount; i++) {
            sync_fields[i] = args[i].field;
            sync_types[i] = IT_BTREE;
        }
        if (index_sync_record_fields(db_root, object, splits, hash16,
                                     sync_fields, sync_types, tcount) != 0) {
            rc = -1;
            /* Preserve the flush errno for the caller's strerror: the tail
               `if (rc != 0) errno = saved_errno ? saved_errno : EIO;` would
               otherwise mask a real fdatasync error as generic EIO. A prior
               mutation errno still wins if both failed. */
            if (!saved_errno) saved_errno = errno;
        }
    }
```

4. Call site — anchor `c->idx_types, /*sync_after=*/1) != 0) {` — drop the
   argument:

```c
    if (index_parallel(c->db_root, c->object, c->splits,
                       c->value_json, c->hash, c->fields, c->nfields,
                       c->idx_types) != 0) {
```

5. **Bug fix + tg collection.** The tg staging declares its arrays inside
   `if (c->idx_types) {`; hoist so the end-of-hook flush can see them.
   Anchor `    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;\n    if (c->nfields == 0) return 0;`
   — add after:

```c
    UpdateIdxArg tg_args[MAX_FIELDS];
    const char *tg_fields[MAX_FIELDS];
    int n_tg = 0;
```

   Delete the inner declarations (anchor `        UpdateIdxArg tg_args[MAX_FIELDS];\n        int n_tg = 0;`).

   In the staging loop, anchor `tg_args[n_tg].bm_max_values = 0;` — replace
   with:

```c
            tg_args[n_tg].bm_max_values = 0;
            tg_args[n_tg].sync_after = 0;  /* was uninitialized stack garbage;
                                              durability moves to index_sync_record_fields */
            tg_fields[n_tg] = c->fields[i];
```

6. End-of-hook tg flush — anchor
   `        v2_insert_bm_owned_free(c);\n        bm_flush_thread_bitmap_cache();\n    }\n    return idx_failed ? -1 : 0;`
   — insert between `}` and `return`:

```c
    }
    if (n_tg > 0) {
        enum IndexType tg_types[MAX_FIELDS];
        for (int i = 0; i < n_tg; i++) tg_types[i] = IT_TRIGRAM;
        if (index_sync_record_fields(c->db_root, c->object, c->splits,
                                     c->hash, tg_fields, tg_types,
                                     n_tg) != 0)
            idx_failed = 1;
    }
    return idx_failed ? -1 : 0;
```

   Accepted cosmetic edge (documented, not fixed): a tg field whose value has
   <3 bytes (zero distinct trigrams — `update_idx_fn` writes nothing) still
   gets one empty `.tg` created here, once per such field.

7. Do **not** touch the bitmap staging site (`arg.sync_after = 1;` in
   `v2_insert_prepare_commit`) — invariant I3.

### 4d. `v2_delete_apply_commit` (post-marker delete diff — fourth hook
flavor, second uninit-bug site)

1. Declarations — anchor

```c
    UpdateIdxArg args[MAX_FIELDS];
    uint8_t *fb_bufs[MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;
```

   (the one inside the function whose capture loop uses `c->err_buf,
   sizeof(c->err_buf),` with operation `"delete"`) — add after:

```c
    const char *ch_fields[MAX_FIELDS];
    enum IndexType ch_types[MAX_FIELDS];
```

2. **Bug fix + conditional flag** — anchor

```c
        args[n_args].kf_slot  = kf_slot;
        args[n_args].bm_max_values = 0;
        n_args++;
```

   (the `kf_slot` parameter form — not `a->kf_slot`) — replace with:

```c
        args[n_args].kf_slot  = kf_slot;
        args[n_args].bm_max_values = 0;
        ch_fields[n_args] = c->idx_fields[i];
        ch_types[n_args] = c->idx_types ? c->idx_types[i] : IT_BTREE;
        /* sync_after was UNINITIALIZED stack garbage (bug: deletes on
           btree/trigram fields synced only if garbage said so). Bitmap
           keeps 1 so bm_sync fires inside bitmap_update (I3); the rest
           move to the collector below. */
        args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
        n_args++;
```

3. Collector — after the error-capture loop, anchor (unique to this
   function):

```c
        for (int i = 0; i < n_args; i++) {
            if (capture_index_update_error(c->err_buf, sizeof(c->err_buf),
                                           &args[i], "delete"))
                idx_failed = 1;
        }
    }
```

   insert between that closing `}` and
   `    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);`:

```c
    if (n_args > 0 &&
        index_sync_record_fields(c->db_root, c->object, c->splits,
                                 c->hash, ch_fields, ch_types,
                                 n_args) != 0)
        idx_failed = 1;
```

## Task 5 — neuter proofs + suites

1. **Collector neuter:** make `idx_sync_thread_fn` set `a->rc = 0; return
   NULL;` without calling `btree_sync_path`, and comment out the two
   serial-fallback `btree_sync_path` calls in `index_sync_record_fields`.
   Run `./build/bin/shard-db-test run test-single-op-index-sync` → every
   `btree_test_sync_count()` assertion must FAIL (all 0). Paste output.
   Restore → green. Paste output.
2. **Bitmap-flag neuter (I3 guard):** in 4a, 4b, and 4d, temporarily change
   the conditional to a blanket `args[n_args].sync_after = 0;`. Run the test
   → cases 3, 4, 5 must FAIL on `bm_test_sync_count()` (0 vs 1), while case 1
   must STILL PASS: a fresh insert never reaches the 4a/4b/4d conditionals —
   its `bm_sync` comes from `v2_insert_prepare_commit`'s `arg.sync_after = 1`
   → `bitmap_prepare_set_apply`, which this neuter (correctly) does not
   touch, so case 1 keeps btree=3 / bm=1. If case 1 also fails, the neuter
   was over-applied — restore and re-verify before continuing. Paste output.
   Restore the conditionals → green. Paste output. This is the proof that
   the silent-bitmap-durability-loss class is caught.
3. Optional curiosity (not required): on base, whether the tg/delete counts
   ever show garbage-0. Note it in the summary if you tried.
4. `./build/bin/shard-db-test run-all --filter durability` and
   `--filter crash` and `--filter index` and `--filter bitmap` — green
   (recovery replay ordering covered by the durability suite; bitmap
   behavior by the bitmap suite).

## Task 6 — docs sync, gates

1. `grep -rn "sync_after" docs/` — update any prose describing per-field sync
   on single-op paths; if docs only describe marker/apply/clear ordering, no
   edit needed — say so in the summary.
2. `SKIP_TESTS=1 ./build.sh` — no new warnings.
3. `./build/bin/shard-db-test run-all` — fresh, green.
4. Sanitizers (mandatory — diff touches commit hooks, bt/bm cache locking,
   parallel_for threads):
   - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, three fresh runs of
     `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all`
   - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, three fresh runs of
     `TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all`
   - Root-cause findings per AGENTS.md; no blanket suppressions.
5. Bench handoff (human runs): `bench-kv` / `bench-invoice` single-op
   throughput+latency before vs after on a multi-index object (the AGENTS.md
   14-index shape). Expected: per-op index-flush wall time drops from
   Σ(N flushes) to ~max(flush) + dispatch on multi-field ops; flush counts
   unchanged except trigram inserts and deletes, which gain the syncs they
   were randomly missing.

## Expected effect (single op, F non-bitmap + B bitmap indexed fields changed)

| | Before | After |
|---|---|---|
| btree/tg flush count (update/upsert/delete) | F (changed fields) | F |
| btree/tg flush count (fresh insert) | F written (+ garbage for tg) | F written, deterministic |
| trigram insert / any delete flush count | garbage 0..F | F (bug fixed, both sites) |
| bitmap bm_sync count | B per changed bitmap field | B (unchanged — conditional flag) |
| btree/tg flush wall time in hook | ~Σ F flushes (serial, interleaved) | ~1 flush + dispatch (parallel, post-mutation) |
| empty .idx/.tg creation | none | none (collector restricted to written fields; <3-byte tg edge documented in 4c.6) |
