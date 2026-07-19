# Fix `get`+`fields` (Finding 7) and remove the dead v1 ucache subsystem

Source: Finding 7 in `docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`.
Per the agreed order (7 → 4 → 8 → rest), this is the first finding to become
an executable plan.

## Root cause

`dispatch_json_query`'s `mode == "get"` handler has two branches for a
single-key get: one when `fields` is present, one when it isn't. Only the
`else` (no-`fields`) branch was ported to the v2/slotcask storage engine — it
calls `cmd_get()` (`storage.c:1082`), which routes through
`slotcask_registry_get`/`slotcask_get`. The `fields` branch
(`server.c:1410-1456`) was never migrated: it still opens
`<db_root>/<object>/data/<shard>.bin` via `fcache_get_read()` and probes a
`ShardHeader`/`SlotHeader` layout. Those `.bin` files are the v1 on-disk
format; this binary only creates and writes v2 (`data/kf/*.kf` +
`data/streams/*/*.dat`) objects (schema.conf's version slot is hard-pinned to
`2`, and CLAUDE.md documents that this binary refuses to load a v1 object at
all). So for every real object on this binary, `fcache_get_read()` on a
`.bin` path returns `fc.map == NULL` (the file doesn't exist), the `else`
branch immediately fires, and `get`+`fields` unconditionally reports
`{"error":"Not found"}` — even for keys that exist and are readable via plain
`get`.

**Repro** (from the finding):
```
./shard-db insert default t1 k1 '{"name":"Alice","age":30}'
./shard-db get default t1 k1                          # returns the record
./shard-db query '{"mode":"get","dir":"default","object":"t1","key":"k1","fields":["name"]}'
# {"error":"Not found"}   <-- wrong; record exists
```

**Second, independent bug found while investigating**: even if the dead-cache
read were fixed in place, the branch's success path emits
`{"key":"...","value":{...}}`. `docs/query-protocol/overview.md` documents
that both plain `get` and `get`+`fields` return a **bare** value/filtered
dict (no `{key,value}` wrapper) — that's what plain `get` already does via
`cmd_get`. The fix below corrects both the data source and the response
shape in one pass, since they're the same code path.

## Scope decision: fix in place vs. remove the dead subsystem

The v1 ucache subsystem (`fcache_get_read`/`ucache_*`/`UCacheEntry`/etc.) that
this branch depends on has no other live caller once this branch is fixed —
verified exhaustively below. Leaving it in place after the fix would mean
~700 lines of storage.c, a whole scan path in query_find.c, and scattered
call sites across five more files sit as unreachable dead code that the next
person has to re-prove is dead. This plan removes it in the same pass.

## Full call-site inventory (verified by grep across the whole tree, not just the files being edited)

### Functions/types being removed, and every caller of each

| Symbol | Defined | Callers (all) |
|---|---|---|
| `UCacheEntry` (struct) | `types.h:886-913` | `storage.c` internals only (all removed below) |
| `FcacheRead` (struct) | `types.h:871-876` | `storage.c` internals; `RecordRef.fc` (`types.h:1156`, fixed below); `query_find.c:25` (removed below); `server.c:1420` (removed below) |
| `fcache_init` | `storage.c:80` | `server.c:3568`, `embedded.c:115` |
| `fcache_shutdown` | `storage.c:93` | `server.c:3765` |
| `ucache_shutdown` | `storage.c:114` | `embedded.c:292`, `embedded.c:405` |
| `ucache_probe` | `storage.c:138` (static) | only `ucache_ensure` (removed) |
| `shard_init_or_read_header` | `storage.c:160` (static) | only `ucache_ensure` (removed) |
| `ucache_ensure` | `storage.c:190` (static) | only within the removed block |
| `fcache_get_read` | `storage.c:311` | `query_find.c:25` (removed), `server.c:1420` (removed by the fix), `query_find.c:500` (`release_record_ref`, fixed below) |
| `fcache_release` | `storage.c:328` | same three sites |
| `ucache_get_write` | `storage.c:335` | none (grepped — zero callers anywhere; already dead before this plan) |
| `ucache_write_release` | `storage.c:352` | none |
| `ucache_nudge_writeback` | `storage.c:364` | none found in `*.c` outside storage.c |
| `ucache_bump_record_count` | `storage.c:374` | none found outside storage.c |
| `grow_rehash_worker` | `storage.c:404` (static) | only `ucache_grow_to` (removed) |
| `ucache_grow_to` | declared `types.h:934`, defined in removed block | only `ucache_grow_shard` (removed) |
| `ucache_grow_shard` | `storage.c:604` | only `ucache_maybe_grow` (removed) |
| `ucache_peek_slots` | `storage.c:616` | none outside storage.c |
| `ucache_maybe_grow` | `storage.c:625` | none outside storage.c |
| `grow_recovery_dir` | `storage.c:642` (static) | only `grow_recovery` |
| `grow_recovery` | `storage.c:669` | `server.c:3606`, `embedded.c:125` |
| `ucache_slot_count` | `storage.c:673` | none outside storage.c |
| `ucache_stats` | `storage.c:677` | `server.c:1024` (`stats`), `server.c:1097` (`stats-prom`) |
| `ucache_entry` | `storage.c:691` | none found anywhere (already dead) |
| `fcache_invalidate` | `storage.c:698` | `query_schema.c:1163`, `query_schema.c:1438`, `query_maint.c:640`, `query_maint.c:899` |
| `build_shard_filename` | `storage.c:10` | only `build_shard_path` |
| `build_shard_path` | `storage.c:15` | `server.c:1419` (removed by the fix) |
| `mmap_with_hints` | `storage.c:55` | none outside storage.c (checked `bitmap.c`/`btree.c`/`slotcask.c` each define their own private `next_pow2`/`path_hash`/mmap helpers — confirmed not shared) |
| `scan_one_shard` | `query_find.c:17` | only `scan_worker` |
| `scan_worker`, `ScanWorkerArg` | `query_find.c:69-67` | only `scan_shards` |
| `scan_shards` | `query_find.c:78`, declared `types.h:1137` | only `query.c:5322` (`cmd_count`'s v1 fallback branch) |
| `CountCtx` (struct) | `query.c:4771-4784` | only `count_scan_cb` and the `query.c:5321` instantiation (both removed) |
| `count_scan_cb` | `query.c:4801` | only `query.c:5322` |

`count_scan_cb_flush_thread` (`query.c:4842`) and the `count_local` TLS
struct it drains **are** removed in this pass, alongside `CountCtx`/
`count_scan_cb` (revised from an earlier draft of this plan that tried to
keep them — see Task 3d). `count_local.bound_cc` is declared `CountCtx *`,
so it cannot survive `CountCtx`'s deletion, and `count_scan_cb` is the only
thing that ever writes `count_local.pending`; once both are gone,
`count_scan_cb_flush_thread()` would be a permanent no-op kept alive at
every scan-worker call site forever — exactly the "next person has to
re-prove it's dead" leftover this plan's Scope decision says to avoid.
There are three call sites in total and one prototype. Two call sites need
explicit removal: `od_seg_file_worker` (`query_find.c:257`, the v2
O_DIRECT scan worker) and `agg_od_seg_worker` (`query_aggregate.c:2330`,
the v2 O_DIRECT aggregate worker) each call
`count_scan_cb_flush_thread();` unconditionally after their per-file scan;
the prototype lives at `query_internal.h:137-138`. The third call site,
`query_find.c:74` inside `scan_worker`, disappears automatically as part
of 3c's deletion of `scan_worker` itself — no separate action needed
there.

### Confirmed *not* affected (checked so the plan doesn't accidentally break them)

- `build_idx_path` (`storage.c:27-32`) — heavily used across `index.c`,
  `query.c`, `query_bulk.c`, `query_aggregate.c`, `config.c`. Sits physically
  between the two removed `build_shard_*` functions and the removed ucache
  block; stays untouched, including its own doc comment.
- `grow_recovery`'s `*.new` sweep vs. slotcask's own crash recovery: v2 kf
  resplits (`slotcask.c:1140` `kfcache_resplit_locked`) write to `<shard>.new`
  and are independently cleaned up per-shard at open time in
  `slotcask_open_kf_worker` (`slotcask.c:1330-1332`,
  `unlink(kf_new)` — "Cleans any leftover .new staging file from a prior
  crashed resplit"). `grow_recovery`'s directory sweep is a *separate*,
  v1-only mechanism for `*.bin.new` growth artifacts from
  `ucache_grow_to`/`grow_rehash_worker`; removing it does not remove v2's own
  recovery path.
- `fcache_invalidate`'s two call sites in `cmd_restore` (`query_maint.c:640`)
  and the vacuum/rebuild path (`query_maint.c:899`) both sit next to (and
  are redundant with) a `slotcask_registry_invalidate(...)` call
  (`query_maint.c:686` and `:906` respectively) that does the real v2 cache
  invalidation. Removing the `fcache_invalidate` calls does not change v2
  invalidation timing or behavior.
- `RecordRef.fc` (`types.h:1156`) is read only by `release_record_ref`
  (`query_find.c:498-507`) behind `if (r->fc.map)`. Grepped every `.fc =`
  assignment in the tree: there are none — no code path ever populates it
  (v1 population would have been via `fcache_get_read`, which had no
  `RecordRef`-based caller). The field is dead weight that must go together
  with the `FcacheRead` type it holds.

### Externally-visible / documented-protocol changes (flagged per CORE-PROCESS, need explicit human sign-off)

1. **`get`+`fields` response shape**: currently emits
   `{"key":"...","value":{...}}` (when it works at all); after the fix it
   emits a bare filtered dict `{"name":"Alice"}`, matching
   `docs/query-protocol/overview.md`'s documented contract and matching
   plain `get`. Since the current shape is unreachable (every real call hits
   "Not found" first), no client can be relying on the old wrapped shape —
   this is a bug fix, not a behavior change to a working feature.
2. **`stats`/`stats-prom` `ucache` block**: keeps the exact same JSON/table
   fields (`used`, `total`, `bytes`, `hits`, `misses`) but they now report
   `0`/`0`/`0` unconditionally instead of calling `ucache_stats()` (which
   already always returned near-zero/garbage on v2 objects since ucache is
   never populated). Field names and response shape are unchanged — no
   client-visible breakage, just now-truthful zeros instead of a call into
   dead-cache internals.
3. **`count` on an object whose slotcask registry fails to open**
   (`slotcask_registry_get` returns NULL — e.g. can't open kf files): today
   this silently falls back to the v1 scan, which finds zero `.bin` files
   and reports `count = 0`. After this plan it reports
   `{"error":"object not open"}`, matching the existing error text used by
   `cmd_rebuild_kf` for the identical condition (`query_maint.c:867`). This
   is a correctness fix (a real open failure should not be reported as "zero
   matches") but it does change what a client sees in that failure case —
   call this out explicitly when presenting the plan for approval.

## Task 1 — regression test (test-first)

Add `src/test/cases/test_get_fields.c`, modeled on
`test_count_varchar_field.c`'s structure (`TestEnv`/`TestClient` over TCP,
`TEST_REGISTER` at the bottom):

```c
/* src/test/cases/test_get_fields.c
 * Finding 7 regression: single-key `get` with a `fields` projection must
 * read the actual (v2/slotcask) record and return a bare filtered dict,
 * matching plain `get`'s documented response shape — not the dead v1
 * ucache path, which always reported "Not found".
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <sys/stat.h>
#include <unistd.h>

/* Extract the value of a `"key":"..."` field from a JSON response.
   Returns a malloc'd copy of the value (caller frees) or NULL if not
   found. Strict: assumes the value is a JSON string, not a number.
   Copied from test_auto_key.c:26-41 -- this codebase's test cases don't
   share code across files. */
static char *extract_key_field(const char *resp) {
    const char *p = SAFE_STRSTR(resp, "\"key\":\"");
    if (!p) return NULL;
    p += 7;
    const char *end = strchr(p, '"');
    if (!end) return NULL;
    size_t n = (size_t)(end - p);
    char *out = malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, p, n);
    out[n] = '\0';
    return out;
}

static int test_get_fields_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp); free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gf\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\",\"age:int\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"value\":{\"name\":\"Alice\",\"age\":30}}", &resp);
    free(resp); resp = NULL;

    /* Plain get (control) — must already work and return a bare dict. */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "plain get returns the record");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "plain get has no error");
    free(resp); resp = NULL;

    /* get + fields — the bug: must return the record, projected, as a bare
       dict (no {"key":...,"value":{...}} wrapper), not "Not found". */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "get+fields does not report Not found");
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "get+fields returns the projected field");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"age\""), "get+fields excludes unrequested fields");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"key\":\"k1\""), "get+fields is a bare dict, no key wrapper");
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"value\":{"), "get+fields is a bare dict, no value wrapper");
    free(resp); resp = NULL;

    /* Multi-field projection. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"k1\","
        "\"fields\":[\"name\",\"age\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"name\":\"Alice\"", "multi-field: name present");
    ASSERT_CONTAINS(resp, "\"age\":\"30\"", "multi-field: age present");
    free(resp); resp = NULL;

    /* Missing key with fields — must still report Not found, not crash. */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gf\",\"key\":\"nope\","
        "\"fields\":[\"name\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\"", "missing key with fields still errors");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields", test_get_fields_run)

/* Auto-key regression (Task 1 addendum): the fields branch never called
   auto_key_normalize before this fix, so a `get`+`fields` request against
   an auto_key=uuid object using the server-rendered dashed-UUID key would
   silently fail to find the record even though it exists. */
static int test_get_fields_auto_key_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfauto\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:32\"],\"auto_key\":\"uuid\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfauto\","
        "\"value\":{\"name\":\"Bob\"}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "auto-key omit-key insert succeeds");
    char *uuid = extract_key_field(resp);
    ASSERT_NOT_NULL(uuid, "insert response carries generated key");
    ASSERT_TRUE(uuid && strlen(uuid) == 36, "generated key is a 36-char dashed uuid");
    free(resp); resp = NULL;

    char req[256];
    snprintf(req, sizeof(req),
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gfauto\",\"key\":\"%s\","
        "\"fields\":[\"name\"]}", uuid ? uuid : "");
    tc_request(tc, req, &resp);
    ASSERT_TRUE(!SAFE_STRSTR(resp, "\"error\""), "get+fields on auto-key object succeeds");
    ASSERT_CONTAINS(resp, "\"name\":\"Bob\"", "get+fields on auto-key object returns the projected field");
    free(resp); resp = NULL;
    free(uuid);

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields-auto-key", test_get_fields_auto_key_run)

/* decode_field composite-field hardening regression (Task 2.5): two
   varchar fields whose combined decoded length exceeds decode_field's
   4096-byte concatenation buffer must not corrupt the stack when
   requested as a composite "f1+f2" projection field. Run this case under
   AddressSanitizer (or equivalent) locally to actually observe the
   pre-fix stack-buffer-overflow -- a plain build may not visibly crash
   every run even though the memory corruption is real. */
static int test_get_fields_composite_overflow_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfbig\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"f1:varchar:3000\",\"f2:varchar:3000\"]}", &resp);
    free(resp); resp = NULL;

    char *big1 = malloc(2900); memset(big1, 'a', 2899); big1[2899] = '\0';
    char *big2 = malloc(2900); memset(big2, 'b', 2899); big2[2899] = '\0';
    char *ins = malloc(6200);
    snprintf(ins, 6200,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfbig\",\"key\":\"k1\","
        "\"value\":{\"f1\":\"%s\",\"f2\":\"%s\"}}", big1, big2);
    tc_request(tc, ins, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "large composite-source insert succeeds");
    free(resp); resp = NULL;
    free(ins); free(big1); free(big2);

    /* Combined f1+f2 length (5798 bytes) exceeds the old fixed 4096-byte
       cat[] buffer -- pre-fix this overflows the stack. Task 2.5 replaces
       that fixed buffer with one that grows dynamically, so the fix must
       preserve the FULL concatenation, not truncate it: decode_field's
       composite path also feeds composite criteria matching
       (query_plan.c:884), ordering keys (query.c:5495), and aggregate
       grouping (query_aggregate.c:2025) -- silent truncation there would
       silently collapse distinct values or corrupt sort/group order, not
       just degrade a display projection. Assert no error AND the exact
       expected value, not just "a response came back". */
    tc_request(tc,
        "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"gfbig\",\"key\":\"k1\","
        "\"fields\":[\"f1+f2\"]}", &resp);
    ASSERT_NOT_NULL(resp, "get+fields composite overflow returns a response");
    ASSERT_TRUE(resp != NULL && !SAFE_STRSTR(resp, "\"error\""),
        "get+fields composite overflow does not error");
    if (resp) {
        char expected[5799];
        memset(expected, 'a', 2899);
        memset(expected + 2899, 'b', 2899);
        expected[5798] = '\0';
        ASSERT_CONTAINS(resp, expected,
            "composite value is the full untruncated 5798-byte concatenation (no data loss)");
    }
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-get-fields-composite-overflow", test_get_fields_composite_overflow_run)

/* count "object not open" regression: forces slotcask_registry_get to fail
   by revoking read permission on one kf shard file, then confirms count
   reports an explicit error instead of silently returning 0. Skipped when
   running as root, since root bypasses file permission checks. */
/* Root-caused in review round 2 (Finding 1, Blocker): the original version
   of this test sent `count` with EMPTY criteria, which takes the O(1)
   get_live_count metadata fast path in cmd_count (query.c ~line 5331) --
   that path never calls slotcask_registry_get at all, so it can never
   observe an open failure. It also chmod'd the kf shard AFTER the
   preceding `insert`, but insert already opened and cached the object's
   SlotcaskDb handle in the daemon's process-wide registry
   (storage.c ~line 1438), so a later chmod on a still-open handle does not
   force a fresh (failing) reopen.
   Fixed by: (a) sending a non-empty, non-indexed criterion so the request
   reaches the scan_shards_v2_o_direct_match / slotcask_registry_get
   fallback path that Task 3d rewrites, and (b) stopping the daemon
   (test_env_stop_keep -- keeps db_root/port, unlike test_env_stop which
   rm -rf's the tree), chmod'ing the kf shard while nothing has it open,
   then restarting a FRESH daemon process (test_env_start_at) at the same
   db_root/port. The new process starts with an empty registry, so the
   very first count request on this object forces a real open() that
   fails on the now-unreadable file. */
static int test_count_object_not_open_run(void) {
    if (geteuid() == 0) {
        TAP_DIAG("# skipping: running as root, chmod-based open-failure "
                 "injection does not apply\n");
        return 0;
    }

    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_stop(&env); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    /* name is NOT indexed -- every op on it (including "contains" below)
       forces a full scan_dispatch scan rather than an index-driven plan,
       regardless of which operator is used. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"gfnotopen\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"name:varchar:32\"]}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"gfnotopen\",\"key\":\"k1\","
        "\"value\":{\"name\":\"Alice\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc);

    /* Stop (keeping db_root/port -- test_env_stop would rm -rf the tree),
       revoke read/write on the kf shard while no process holds it open,
       then restart fresh at the same db_root/port so the registry cache
       from the insert above is gone. Save db_root/port to independent
       locals first: test_env_start_at's signature takes db_root as a
       plain `const char *`, and passing env.db_root back in as that
       argument while the function's own snprintf writes into
       env->db_root is a same-buffer overlap (restrict-qualifier
       violation) -- undefined behavior on paper even though it happens
       to be an identity copy in practice on this glibc. */
    char saved_db_root[PATH_MAX];
    snprintf(saved_db_root, sizeof(saved_db_root), "%s", env.db_root);
    int saved_port = env.port;
    test_env_stop_keep(&env);

    char kf_path[PATH_MAX];
    snprintf(kf_path, sizeof(kf_path), "%s/default/gfnotopen/data/kf/000.kf", saved_db_root);
    ASSERT_TRUE(chmod(kf_path, 0) == 0, "revoke kf shard permissions");

    ASSERT_TRUE(test_env_start_at(&env, saved_db_root, saved_port) == 0,
        "daemon restarts fresh at the same db_root/port");

    TestClient *tc2 = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc2, "reconnect after restart");
    if (!tc2) {
        chmod(kf_path, 0644);
        test_env_stop(&env);
        return 1;
    }

    tc_request(tc2,
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"gfnotopen\","
        "\"criteria\":[{\"field\":\"name\",\"op\":\"contains\",\"value\":\"A\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"error\":\"object not open\"", "count reports the open failure, not zero");
    free(resp); resp = NULL;

    chmod(kf_path, 0644);
    tc_close(tc2);
    test_env_stop(&env);
    return 0;
}

TEST_REGISTER("test-count-object-not-open", test_count_object_not_open_run)
```
The full file content above (includes, `extract_key_field`, and all four
`TEST_REGISTER`'d cases) is what `src/test/cases/test_get_fields.c` must
contain — copy it verbatim, don't re-derive it.

Register it in `build.sh`. Anchor (exact existing line):
```
    src/test/cases/test_count_varchar_field.c \
```
Add immediately after it:
```
    src/test/cases/test_get_fields.c \
```

**Prove it fails first (test-get-fields)**: build with `SKIP_TESTS=1
./build.sh`, then run `./build/bin/shard-db-test run test-get-fields` on the
unmodified tree (i.e. before Task 2's fix is applied). Expected failure: the
`get+fields does not report Not found` and `get+fields returns the projected
field` assertions fail, because the current code returns
`{"error":"Not found"}`. Paste the actual failing output. Then apply Task 2,
rebuild, rerun the same command, and paste the passing output.

**Prove it fails first (test-get-fields-auto-key)**: run
`./build/bin/shard-db-test run test-get-fields-auto-key` on the unmodified
tree. Expected failure: same shape as above — `get+fields` on an
auto_key=uuid object hits the same broken v1-ucache branch (it's keyed by
the server-generated UUID rather than a client-supplied key, but the buggy
code path is identical), so the projected-field assertion fails with
`{"error":"Not found"}`. Paste the actual failing output, apply Task 2,
rebuild, rerun, paste the passing output.

**Prove it fails first (test-get-fields-composite-overflow)**: this test
issues a `get`+`fields` request (`"fields":["f1+f2"]`), so it depends on
Task 2's dispatch fix just to *reach* `decode_field` at all — on the fully
unmodified tree, the pre-Task-2 `get`+`fields` branch returns
`{"error":"Not found"}` before ever calling `decode_field` (see the
Task 2 root cause above), so the overflow cannot be observed yet. Stage
this red run **after Task 2 lands but before Task 2.5**:
1. Apply Task 2's fix only (not Task 2.5 yet — `decode_field` itself must
   still have the unchecked `cat[4096]` stack buffer).
2. Build with `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`.
3. Run `./build/bin/shard-db-test run test-get-fields-composite-overflow`.
   Expected failure: ASan reports a stack-buffer-overflow inside
   `decode_field`'s composite concatenation (`query_find.c`, the unchecked
   `memcpy(cat + cp, v, sl)` into the fixed 4096-byte `cat[]`), because the
   5798-byte f1+f2 concatenation overruns it. Paste the actual ASan report.
4. Apply Task 2.5's dynamically-grown-buffer fix.
5. Rebuild with the same `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`.
6. Rerun the same command and paste the clean (no ASan report, all
   assertions pass) output.

**Prove it fails first (test-count-object-not-open)**: run
`./build/bin/shard-db-test run test-count-object-not-open` on the
unmodified tree (before Task 3d's fallback rewrite). Expected failure:
the assertion on `"\"error\":\"object not open\""` fails because the
pre-Task-3d legacy v1 `CountCtx`/`count_scan_cb` fallback in `cmd_count`
silently returns a scan result (likely `0`, since the segment reads
against the chmod'd-unreadable kf shard fail open silently in the v1 path
rather than surfacing an explicit open error) instead of the explicit
`{"error":"object not open"}` that Task 3d's rewritten v2-only fallback
produces. Paste the actual failing output. Then apply Task 3d, rebuild,
rerun the same command, and paste the passing output.

## Task 2 — fix `get`+`fields`: new `cmd_get_fields` + rewritten dispatch branch

### 2a. `storage.c` — add `cmd_get_fields`, right after `cmd_get`

Anchor (exact existing text, end of `cmd_get`):
```c
    LOG_DEBUG(LOG_SUB_SLOTCASK, "GET %s (klen=%zu, %zu bytes)", object, klen, vlen);
    TypedSchema *ts = load_typed_schema(db_root, object);
    typed_decode_stream(ts, (const uint8_t *)val, (uint32_t)vlen,
                         g_out ? g_out : stdout);
    fputc('\n', g_out ? g_out : stdout);
    free(val);
    return 0;
}

/* ========== CAS (Compare-and-Swap) helper ========== */
```

Insert a new function between `cmd_get`'s closing `}` and the
`/* ========== CAS ... */` comment:

```c

/* Single-key get with a field projection. Same slotcask read path as
   cmd_get; response is a bare dict of the requested fields (matching
   cmd_get's bare-value contract — no {"key":...,"value":{...}} wrapper).
   fields_csv is a comma-separated field list (composite fields use '+',
   same as decode_field elsewhere). */
int cmd_get_fields(const char *db_root, const char *object,
                    const char *key, size_t klen, const char *fields_csv) {
    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }

    FieldSchema pfs;
    init_field_schema(&pfs, db_root, object);
    char proj_buf[MAX_LINE];
    strncpy(proj_buf, fields_csv, MAX_LINE - 1);
    proj_buf[MAX_LINE - 1] = '\0';
    const char *flds[MAX_FIELDS];
    int nf = 0;
    char *_tok_save = NULL;
    char *tok = strtok_r(proj_buf, ",", &_tok_save);
    while (tok && nf < MAX_FIELDS) { flds[nf++] = tok; tok = strtok_r(NULL, ",", &_tok_save); }

    OUT("{");
    int first = 1;
    for (int fi = 0; fi < nf; fi++) {
        char *pv = json_escape_field(decode_field((const char *)val, vlen, flds[fi],
            (pfs.ts || pfs.nfields > 0) ? &pfs : NULL));
        if (!pv) continue;
        OUT("%s\"%s\":\"%s\"", first ? "" : ",", flds[fi], pv);
        first = 0; free(pv);
    }
    OUT("}\n");
    free(val);
    return 0;
}
```

### 2b. `types.h` — prototype

Add `cmd_get_fields`'s prototype immediately after `cmd_get`'s, in the CRUD
prototype block. Anchor (exact existing text, `types.h:980-981`):
```c
int cmd_get(const char *db_root, const char *object,
            const char *key, size_t klen);
```
Add immediately after:
```c
int cmd_get_fields(const char *db_root, const char *object,
                    const char *key, size_t klen, const char *fields_csv);
```

### 2c. `server.c` — rewrite the `get` dispatch branch

Anchor (exact existing text, `server.c:1409-1468`):
```c
        } else if (key) {
            if (fields && fields[0]) {
                /* Get with projection — uses ucache */
                Schema sc = load_schema(db_root, object);
                uint8_t hash[16]; int shard_id, start_slot;
                size_t klen = strlen(key);
                compute_hash_raw(key, klen, hash);
                shard_id = compute_record_shard(hash, sc.splits);
                start_slot = 0;
                char shard[PATH_MAX];
                build_shard_path(shard, sizeof(shard), db_root, object, shard_id);
                FcacheRead fc = fcache_get_read(shard);
                if (fc.map) {
                    uint32_t slots = fc.slots_per_shard;
                    uint32_t mask = slots - 1;
                    int found = 0;
                    for (uint32_t i = 0; i < slots; i++) {
                        uint32_t s = ((uint32_t)start_slot + i) & mask;
                        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
                        if (h->flag == 0 && h->key_len == 0) break;
                        if (h->flag == 2) continue;
                        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
                            h->key_len == klen &&
                            memcmp(fc.map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
                            const char *raw = (const char *)(fc.map + zoneB_off(s, slots, sc.slot_size) + h->key_len);
                            FieldSchema pfs; init_field_schema(&pfs, db_root, object);
                            char proj_buf[MAX_LINE];
                            strncpy(proj_buf, fields, MAX_LINE - 1);
                            const char *flds[MAX_FIELDS];
                            int nf = 0;
                            char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
                            while (tok && nf < MAX_FIELDS) { flds[nf++] = tok; tok = strtok_r(NULL, ",", &_tok_save); }
                            OUT("{\"key\":\"%s\",\"value\":{", key);
                            int first = 1;
                            for (int fi = 0; fi < nf; fi++) {
                                char *pv = json_escape_field(decode_field(raw, h->value_len, flds[fi],
                                    (pfs.ts || pfs.nfields > 0) ? &pfs : NULL));
                                if (!pv) continue;
                                OUT("%s\"%s\":\"%s\"", first ? "" : ",", flds[fi], pv);
                                first = 0; free(pv);
                            }
                            OUT("}}\n");
                            found = 1; break;
                        }
                    }
                    if (!found) OUT("{\"error\":\"Not found\"}\n");
                    fcache_release(fc);
                } else OUT("{\"error\":\"Not found\"}\n");
            } else {
                Schema sc_chk = load_schema(db_root, object);
                if (sc_chk.auto_key != AK_NONE) {
                    char *bin = NULL; size_t blen = 0;
                    if (auto_key_normalize(&sc_chk, key, &bin, &blen) == 0) {
                        cmd_get(db_root, object, bin, blen);
                        free(bin);
                    }
                } else {
                    cmd_get(db_root, object, key, strlen(key));
                }
            }
        } else {
```

Replace with:
```c
        } else if (key) {
            Schema sc_chk = load_schema(db_root, object);
            char *bin = NULL; size_t blen = 0;
            const char *use_key = key;
            size_t use_klen = strlen(key);
            if (sc_chk.auto_key != AK_NONE) {
                if (auto_key_normalize(&sc_chk, key, &bin, &blen) == 0) {
                    use_key = bin; use_klen = blen;
                } else {
                    use_key = NULL;
                }
            }
            if (use_key) {
                if (fields && fields[0]) {
                    cmd_get_fields(db_root, object, use_key, use_klen, fields);
                } else {
                    cmd_get(db_root, object, use_key, use_klen);
                }
            }
            free(bin);
        } else {
```

This unifies auto-key normalization across both the fields and non-fields
paths (previously only the non-fields path normalized auto-generated keys —
the broken fields path never did, so even after fixing the storage read, an
`auto_key` object combined with `fields` would still silently fail to find
the key). The existing behavior of emitting **no response at all** when
`auto_key_normalize` fails is preserved exactly (`use_key` stays `NULL`,
`if (use_key)` is skipped, `bin` is `NULL` so `free(bin)` is a no-op) — do
not "improve" this by adding an error response; it's out of scope and other
call sites of `auto_key_normalize` in this same file rely on the identical
silent-on-failure convention (the function itself already emits an
`{"error":...}` via `OUT()` before returning -1, so the caller staying silent
avoids double-emitting a response on the wire).

## Task 2.5 — harden `decode_field`'s composite-field concatenation

**Why this is in scope here**: while investigating this plan, a pre-existing
stack-buffer overflow was found in `decode_field`'s `"a+b"` composite-field
path (`query_find.c:526-548`) — it concatenates each `+`-joined field's
decoded string into a fixed `char cat[4096]` with an unchecked `memcpy`, no
bound on the running offset `cp`. This is reachable today via find, fetch,
join, and aggregate's `fields` projection param (all of which call
`decode_field` for composite fields), not something newly introduced here —
but `cmd_get_fields` (Task 2) is a new, additional caller of this exact
function, so this plan fixes the underlying function rather than building a
new caller on top of a known-unsafe one.

**Root cause**: `varchar` fields can hold up to 65535 bytes of content
(CLAUDE.md's typed-record-format table). Two or more large `varchar` fields
named in a composite index (e.g. `"fields":["bio+notes"]` where `bio` and
`notes` are each multi-KB `varchar` fields) decode to strings whose combined
length can exceed the 4096-byte `cat` buffer; the `memcpy(cat + cp, v, sl)`
call has no check that `cp + sl < sizeof(cat)`, so it writes past the end of
a stack array — a classic stack-smashing bug, not merely a truncation bug.

**Why truncation is not an acceptable fix (review round 2, Finding 2,
High)**: `decode_field` is not a display-only helper. Its composite path is
also the fallback for:
  - Composite/unknown-field criteria matching in `match_typed`
    (`query_plan.c:884` — `decode_field` + `match_criterion` is the
    fallback whenever `cc->composite || !cc->tf`).
  - Sort/ordering keys in the cursor-ordered scan callback
    (`query.c:5495` — `decode_field` is the fallback when the order field
    has no typed-schema entry, i.e. it's composite).
  - Aggregate group-by bucketing (`query_aggregate.c:2025` — `decode_field`
    is the fallback for composite/unknown group fields, feeding directly
    into the group-key string used to merge rows into buckets).
  Truncating at a fixed length (however large) means two composites that
  agree on their first N bytes but diverge after that decode to the *same*
  string — silently collapsing distinct values into one sort position or
  one aggregate bucket. That is a correctness bug in query results, not a
  degraded display value. The fix below therefore grows the concatenation
  buffer dynamically instead of capping it, so `decode_field` itself never
  truncates its own output regardless of length.

**Scope note — this fix does not close the aggregate-bucketing gap
end-to-end**: `decode_field` no longer truncates, which fully eliminates
truncation-driven collisions for criteria matching (`match_typed`,
`query_plan.c:884`) and cursor ordering (`query.c:5495`) — both consume
`decode_field`'s return value directly with no further-truncating buffer
of their own. Aggregate group-by bucketing is different: `query_aggregate.c
:2025-2030` copies `decode_field`'s result into its *own* separate fixed
`char gbuf[MAX_FIELDS][512]` buffer with an independent 511-byte
truncation (`if (sl >= sizeof(gbuf[i])) sl = sizeof(gbuf[i]) - 1;`), so two
composite group-by values that agree on their first 511 bytes but diverge
after that still collapse into the same bucket even after this fix lands.
That is a separate, pre-existing bug in `query_aggregate.c`'s own buffer,
not in `decode_field` — fixing it would mean touching the aggregate
hash-table/group-merge code path and adding its own regression test, which
is out of scope for this plan (this plan targets `decode_field` itself,
reachable today via `cmd_get_fields`, find/fetch/join `fields` projection,
criteria matching, and ordering). Flagged here as a known, tracked
follow-up rather than left undocumented.

**Test-first**: `test_get_fields_composite_overflow_run` (added in Task 1)
creates an object with two `varchar:3000` fields joined as a composite
field name, inserts values whose combined length (5798 bytes) exceeds the
old fixed 4096-byte `cat` buffer, and requests `"fields":["f1+f2"]`,
asserting no error AND the exact full 5798-byte concatenation (not a
truncated prefix). This test issues a `get`+`fields` request, so **Task 2
must already be applied** before this red run — on the fully unmodified
tree the pre-Task-2 `get`+`fields` branch returns `{"error":"Not found"}`
before ever calling `decode_field`, which would mask the overflow rather
than reproduce it (see Task 1's own note on this same test). With Task 2
applied (Task 2.5 NOT yet applied), prove the pre-fix behavior under ASan
(stack corruption without a sanitizer is not reliably reproducible on
every run):
```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-get-fields-composite-overflow
```
Expected with Task 2 applied and Task 2.5 not yet applied: ASan reports a
`stack-buffer-overflow` inside `decode_field`'s composite concatenation
(the unchecked `memcpy(cat + cp, v, sl)` into the fixed `cat[4096]`). Paste
the actual ASan report. Then apply the fix below, rebuild with the same
`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, rerun the same test command, and
paste the clean (no ASan report, both assertions pass) output.

**Fix**: replace the fixed `cat[4096]` stack buffer with a heap buffer that
grows (doubling) to fit the full concatenation — no truncation, no
overflow, regardless of how long the composite value is. Anchor (exact
existing text, `query_find.c:526-548`):
```c
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs) {
    if (fs && fs->ts) {
        /* Typed binary: handle composite fields */
        if (strchr(field, '+')) {
            char fb[256]; strncpy(fb, field, 255); fb[255] = '\0';
            char cat[4096]; int cp = 0;
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok) {
                int idx = typed_field_index(fs->ts, tok);
                if (idx >= 0) {
                    char *v = typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            return cp > 0 ? strdup(cat) : NULL;
        }
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
    }
    return NULL;
}
```
Replace with:
```c
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs) {
    if (fs && fs->ts) {
        /* Typed binary: handle composite fields */
        if (strchr(field, '+')) {
            char fb[256]; strncpy(fb, field, 255); fb[255] = '\0';
            /* Grows (doubling) to fit the full concatenation -- never
               truncates. decode_field's composite path also feeds
               criteria matching (query_plan.c), ordering (query.c), and
               aggregate grouping (query_aggregate.c); truncating here
               would silently collapse distinct values sharing a common
               prefix into the same match/sort-key/group-bucket. */
            size_t cap = 256;
            char *cat = malloc(cap);
            if (!cat) return NULL;
            int cp = 0;
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok) {
                int idx = typed_field_index(fs->ts, tok);
                if (idx >= 0) {
                    char *v = typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
                    if (v) {
                        int sl = strlen(v);
                        if ((size_t)cp + (size_t)sl + 1 > cap) {
                            size_t need = (size_t)cp + (size_t)sl + 1;
                            size_t new_cap = cap;
                            while (new_cap < need) new_cap *= 2;
                            char *ncat = realloc(cat, new_cap);
                            if (!ncat) { free(cat); free(v); return NULL; }
                            cat = ncat; cap = new_cap;
                        }
                        memcpy(cat + cp, v, sl); cp += sl;
                        free(v);
                    }
                }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            if (cp == 0) { free(cat); return NULL; }
            return cat;
        }
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
    }
    return NULL;
}
```
Note: the previous version returned `strdup(cat)` from a stack buffer
(freeing the temporary); this version returns the heap buffer `cat`
directly (already independently allocated, no separate `strdup` needed) —
callers already `free()` `decode_field`'s return value on every call site
(display, criteria matching, ordering, aggregation), so this is a drop-in
replacement with no caller-side changes required.

## Task 2.6 — harden retained ucache metrics to exact zero (test-first)

Execute this task immediately after Task 2.5 and before any Task 3
subtask. It supplies the red test for Task 3's externally-visible
zero-value guarantee while the old ucache initialization is still
present; Task 4's full suite supplies the final green proof.

`src/test/cases/test_stats_prom.c` currently asserts the retained ucache
counters are *non-decreasing* rather than asserting they are *exactly
zero* — correct before this plan (ucache was live in earlier releases) but
weaker than it should be once Task 3e hardcodes every ucache stat to a
literal 0. Strengthen it to lock in the zero-value invariant directly, so
a future regression that accidentally starts incrementing a "removed"
counter is caught.

Anchor (exact existing text, `test_stats_prom.c`):
```c
    long hits_before = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_before = sample_value(resp, "shard_db_ucache_misses_total");
    free(resp); resp = NULL;

    /* Generate traffic — slotcask uses kfcache/segcache; the same prom
       sample harness covers both. ucache_hits/misses now report zero on
       a fresh DB but the metric still appears in the export. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"fields\":[\"name:varchar:32\"],\"splits\":16}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp); free(resp); resp = NULL;
    for (int i = 0; i < 5; i++) {
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"prom_test\",\"key\":\"k1\"}", &resp);
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"stats-prom\"}", &resp);
    ASSERT_NOT_NULL(resp, "stats-prom (post-traffic) returned output");
    long hits_after = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_after = sample_value(resp, "shard_db_ucache_misses_total");
    long up_before = sample_value(resp, "shard_db_uptime_seconds");

    /* ucache is unused on v2 (slotcask uses kfcache/segcache), so the
       counter stays at its initial value — assert non-decreasing rather
       than strictly increasing. */
    ASSERT_TRUE(hits_after >= hits_before, "ucache_hits_total non-decreasing");
    ASSERT_TRUE(miss_after >= miss_before, "ucache_misses_total non-decreasing");
```
Replace with:
```c
    long used_before = sample_value(resp, "shard_db_ucache_used");
    long cap_before   = sample_value(resp, "shard_db_ucache_capacity");
    long bytes_before = sample_value(resp, "shard_db_ucache_bytes");
    long hits_before  = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_before  = sample_value(resp, "shard_db_ucache_misses_total");
    free(resp); resp = NULL;

    ASSERT_TRUE(used_before == 0, "ucache_used is exactly 0 (pre-traffic)");
    ASSERT_TRUE(cap_before == 0, "ucache_capacity is exactly 0 (pre-traffic)");
    ASSERT_TRUE(bytes_before == 0, "ucache_bytes is exactly 0 (pre-traffic)");
    ASSERT_TRUE(hits_before == 0, "ucache_hits_total is exactly 0 (pre-traffic)");
    ASSERT_TRUE(miss_before == 0, "ucache_misses_total is exactly 0 (pre-traffic)");

    /* Generate traffic — slotcask uses kfcache/segcache, not ucache. ucache
       is dead on v2 (Task 3, this plan); every one of its stats fields is
       now a hardcoded literal 0, retained only so existing dashboards
       parsing these field/counter names don't break. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"fields\":[\"name:varchar:32\"],\"splits\":16}", &resp); free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"prom_test\","
        "\"key\":\"k1\",\"value\":{\"name\":\"alice\"}}", &resp); free(resp); resp = NULL;
    for (int i = 0; i < 5; i++) {
        tc_request(tc, "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"prom_test\",\"key\":\"k1\"}", &resp);
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"stats-prom\"}", &resp);
    ASSERT_NOT_NULL(resp, "stats-prom (post-traffic) returned output");
    long used_after  = sample_value(resp, "shard_db_ucache_used");
    long cap_after   = sample_value(resp, "shard_db_ucache_capacity");
    long bytes_after = sample_value(resp, "shard_db_ucache_bytes");
    long hits_after  = sample_value(resp, "shard_db_ucache_hits_total");
    long miss_after  = sample_value(resp, "shard_db_ucache_misses_total");
    long up_before = sample_value(resp, "shard_db_uptime_seconds");

    ASSERT_TRUE(used_after == 0, "ucache_used is exactly 0 (post-traffic)");
    ASSERT_TRUE(cap_after == 0, "ucache_capacity is exactly 0 (post-traffic)");
    ASSERT_TRUE(bytes_after == 0, "ucache_bytes is exactly 0 (post-traffic)");
    ASSERT_TRUE(hits_after == 0, "ucache_hits_total is exactly 0 (post-traffic)");
    ASSERT_TRUE(miss_after == 0, "ucache_misses_total is exactly 0 (post-traffic)");
```
Note: this replacement drops the pre-existing `hits_after`/`miss_after`
non-decreasing assertions in favor of the strictly stronger exact-zero
assertions above — do not keep both (the non-decreasing checks become
redundant once exact-zero is asserted on both sides).

**Prove it fails first, then passes**: at this point in the execution
order, Task 1/2/2.5 are applied and none of Task 3a-3k's deletions have
landed yet, so `fcache_init` (`storage.c`) still unconditionally
pre-allocates `g_ucache_slots = next_pow2(FCACHE_MAX * 2)` at daemon
startup regardless of whether any v1 object ever exists. Run
`SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run test-stats-prom`
now (before applying 3a-3k) and expect the new `cap_before == 0` (and
`cap_after == 0`) assertions to fail specifically, not the others:
pre-fix, `shard_db_ucache_capacity` reports that pre-allocated pool size
(non-zero) even on this test's all-v2 objects; `used`/`hits`/`misses` are
separately already 0 pre-fix on an all-v2 run (no v1 shard is ever opened
to populate them), so only the capacity assertions are expected to
distinguish pre-fix from post-fix. Paste the actual failing assertion
output. Then apply Task 3a-3k in full (Task 3e hardcodes the retained wire
metrics to zero while the other subtasks remove the backing ucache
state) and let Task 4's `run-all` serve as the green proof — no separate rerun of this one test is needed since
`run-all` covers it.


## Task 3 — remove the dead v1 ucache subsystem

Do this only after Tasks 2 and 2.5 are green and Task 2.6's red proof is
complete. Task 2's fix is what makes the subsystem provably dead — the fields
branch was the last caller of the read path — while Task 2.6 must observe the
old ucache initialization before Task 3 removes it.

### 3a. `storage.c`

Remove `build_shard_filename` + `build_shard_path`. Anchor (exact existing
text, keep everything else in this span — `build_idx_path` and its comment
stay):
```c
/* Shard filename format: <dir>/NNN.bin (3 hex digits, supports up to MAX_SPLITS=4096).
   Single source of truth — change here if the format ever changes. */
void build_shard_filename(char *buf, size_t buflen,
                          const char *data_dir, int shard_id) {
    snprintf(buf, buflen, "%s/%03x.bin", data_dir, shard_id & 0xFFF);
}

void build_shard_path(char *buf, size_t buflen,
                             const char *db_root, const char *object, int shard_id) {
    char dd[PATH_MAX];
    snprintf(dd, sizeof(dd), "%s/%s/data", db_root, object);
    build_shard_filename(buf, buflen, dd, shard_id);
}

/* Canonical layout for per-shard indexes:
```
becomes:
```c
/* Canonical layout for per-shard indexes:
```
(i.e. delete the `build_shard_filename`/`build_shard_path` functions and
their preceding comment; the `/* Canonical layout for per-shard indexes:`
comment that introduces `build_idx_path` is untouched and now follows
directly after the `#include`/hashing comment block.)

Remove the entire ucache block. Start anchor (exact existing text):
```c
/* ========== Unified Shard Cache (ucache) ==========
   Single persistent MAP_SHARED mmap per shard file. Serves both reads and writes.
```
End anchor — delete up to and including `fcache_invalidate`'s closing brace,
stopping immediately before (exact existing text, do not delete this line):
```c
/* ========== Pre-allocation ========== */
```
This removes, in order: the ucache doc comment, `mmap_with_hints`,
`next_pow2`, `path_hash`, `fcache_init`, `fcache_shutdown`,
`ucache_shutdown`, `ucache_probe`, `shard_init_or_read_header`,
`ucache_ensure`, `fcache_get_read`, `fcache_release`, `ucache_get_write`,
`ucache_write_release`, `ucache_nudge_writeback`,
`ucache_bump_record_count`, `grow_rehash_worker`, `ucache_grow_to` (defined
inside `grow_rehash_worker`'s neighborhood — verify by symbol name during
execution, not just line position, since exact line numbers will have
shifted once the earlier `build_shard_*` deletion lands), `ucache_grow_shard`,
`ucache_peek_slots`, `ucache_maybe_grow`, `grow_recovery_dir`,
`grow_recovery`, `ucache_slot_count`, `ucache_stats`, `ucache_entry`,
`fcache_invalidate`.

**Verification step (required before moving on)**: after this deletion,
`grep -n "ucache_grow_to\b" src/db/storage.c` must return nothing left
outside the deleted span — if `ucache_grow_to`'s definition is not
contiguous with the rest of the block as expected, stop and treat this as a
stale-anchor mismatch per the execution rules below, not something to patch
around.

### 3b. `types.h`

Remove the standalone `ucache_stats` prototype — it sits far from the rest
of the ucache-family prototypes (in the I/O thread pool section, not the
`FcacheRead`/`UCacheEntry` block below) so it's easy to miss in a single
grep-and-delete pass. Anchor (exact existing text, `types.h:579`, between
`log_slow_query`'s prototype and `bt_cache_stats`'s):
```c
int ucache_stats(int *used_slots, int *total_slots, size_t *total_bytes);
```
Delete this line entirely (no replacement) — `ucache_stats`'s definition in
`storage.c` is already removed by 3a.

Remove the prototypes. Anchor (exact existing text):
```c
void build_shard_path(char *buf, size_t buflen, const char *db_root, const char *object, int shard_id);
void build_shard_filename(char *buf, size_t buflen, const char *data_dir, int shard_id);
void build_idx_path(char *buf, size_t buflen,
                    const char *db_root, const char *object,
                    const char *field, int idx_shard_id);
uint8_t *mmap_with_hints(void *addr, size_t len, int prot, int flags, int fd, off_t off);
```
Replace with (keep `build_idx_path`, drop the rest):
```c
void build_idx_path(char *buf, size_t buflen,
                    const char *db_root, const char *object,
                    const char *field, int idx_shard_id);
```

Remove `FcacheRead`, `UCacheEntry`, and every ucache-family prototype.
Anchor (exact existing text) start:
```c
/* Unified shard cache (ucache) — persistent MAP_SHARED mmap per shard.
   Per-entry rwlock: shared for reads, exclusive for writes.
   FcacheRead handle used for both read and write operations. */
typedef struct {
    uint8_t *map;   /* NULL on failure */
    size_t   size;
    uint32_t slots_per_shard;  /* captured at open time */
    int      slot;  /* cache slot index, -1 = invalid */
} FcacheRead;
```
...through (exact existing text, this is the last line to delete):
```c
void       ucache_bump_record_count(int ucache_slot, int delta);
```
Everything from the `FcacheRead` typedef's leading comment through
`ucache_bump_record_count`'s prototype is deleted **except** the `SlotRef`
struct in the middle of that span (`types.h:881-884`, `typedef struct SlotRef
{ int slot; uint64_t gen; } SlotRef;`) — `SlotRef` is the live cache-slot
validation type used by `bt_cache`/`bm_cache`/`kfcache` elsewhere; it is
physically interleaved with the doomed `UCacheEntry` definition but is not
part of the v1 subsystem. Preserve it in place:
```c
/* A lightweight cache reference: cache-slot index + generation counter.
   Validated with a single atomic load — no table lock needed on warm hits.
   slot == -1 means "not yet populated" (safe initial value after calloc/memset). */
typedef struct SlotRef {
    int      slot;
    uint64_t gen;
} SlotRef;
```
So the net result of this deletion is: `FcacheRead` gone, `SlotRef` kept
verbatim in its current position, `UCacheEntry` gone, and every
`fcache_*`/`ucache_*` prototype gone (`fcache_init`, `fcache_shutdown`,
`ucache_shutdown`, `fcache_get_read`, `fcache_release`, `ucache_get_write`,
`ucache_write_release`, `ucache_nudge_writeback`, `ucache_grow_to`,
`ucache_grow_shard`, `ucache_maybe_grow`, `ucache_peek_slots`,
`grow_recovery`, `ucache_entry`, `ucache_slot_count`, `fcache_invalidate`,
`ucache_bump_record_count`), leaving the file to continue directly into
`update_count`'s prototype (`types.h:953` before this edit —
`void update_count(const char *db_root, const char *object, int delta);`),
unchanged.

Delete the now-dangling comment referencing `FcacheRead`, rather than
patching it to keep referencing `ShardHeader` — Task 3j below removes
`ShardHeader` too, so no rewording of this comment survives Task 3 intact;
it describes v1's linear-probe behavior, which no longer exists in the
codebase once 3a/3c land. Anchor (exact existing text):
```c
/* Probe bound is dynamic per-shard: callers use the shard's current
   slots_per_shard (via FcacheRead.slots_per_shard or ShardHeader). Growth at 50%
   load keeps clusters short, so typical probes stop in 1-5 iterations. */
```
Delete this comment entirely (no replacement).

Remove the dangling `scan_shards` prototype. Anchor (exact existing text):
```c
void scan_shards(const char *data_dir, int slot_size, scan_callback cb, void *ctx);
```
Delete this line entirely (no replacement).

Fix `RecordRef` — drop the `.fc` field and its v1 half of the doc comment.
Anchor (exact existing text):
```c
/* Indexed record fetch: layout-agnostic dispatch for hash-based lookups.
   v1 path holds an FcacheRead handle; v2 holds a copy of the record in
   inline_buf (fits) or a malloc'd fallback (too large for inline_buf).
   Either way, key + val point into a contiguous buffer with layout
   `[key bytes][val bytes]`, matching v1 Zone B. Caller must call
   release_record_ref to free both lifetimes. */
typedef struct {
    FcacheRead     fc;        /* v1: kept open to keep mmap alive; .map=NULL on v2 */
    uint8_t       *v2_buf;    /* v2: points at inline_buf (common case) or a
                                  malloc'd fallback (record too large for
                                  inline_buf); NULL on v1 */
    uint8_t        inline_buf[2048]; /* v2 fast path — avoids malloc/free for
                                         records that fit; same size convention
                                         as the stk[2048] pattern in query.c's
                                         KeySet-fallback record collection */
    const uint8_t *key;
    size_t         klen;
    const uint8_t *val;
    size_t         vlen;
} RecordRef;
```
Replace with:
```c
/* Indexed record fetch: holds a copy of the record in inline_buf (fits) or
   a malloc'd fallback (too large for inline_buf). key + val point into a
   contiguous buffer with layout `[key bytes][val bytes]`. Caller must call
   release_record_ref to free the fallback allocation, if any. */
typedef struct {
    uint8_t       *v2_buf;    /* points at inline_buf (common case) or a
                                  malloc'd fallback (record too large for
                                  inline_buf) */
    uint8_t        inline_buf[2048]; /* fast path — avoids malloc/free for
                                         records that fit; same size convention
                                         as the stk[2048] pattern in query.c's
                                         KeySet-fallback record collection */
    const uint8_t *key;
    size_t         klen;
    const uint8_t *val;
    size_t         vlen;
} RecordRef;
```

### 3c. `query_find.c`

Remove the v1 scan path. Anchor (exact existing text) start:
```c
void scan_one_shard(const char *binpath, int slot_size,
                           scan_callback cb, void *ctx) {
```
End anchor — delete up to and including `scan_shards`'s closing brace,
stopping immediately before (exact existing text, do not delete this line):
```c
/* ========== v2 (slotcask) scan bridge ========== */
```
This removes `scan_one_shard`, the `ScanWorkerArg` typedef, `scan_worker`,
and `scan_shards`. Also delete the two lines immediately above
`scan_one_shard`'s definition that exist purely to document the removed
code:
```c
/* g_scan_stop moved to ShardDb struct */

void scan_one_shard(const char *binpath, int slot_size,
```
becomes just the start of the (deleted) function — i.e. the `/* g_scan_stop
moved to ShardDb struct */` comment line is deleted along with the function
it was documenting context for.

Fix `release_record_ref`. Anchor (exact existing text):
```c
void release_record_ref(RecordRef *r) {
    if (!r) return;
    if (r->fc.map) { fcache_release(r->fc); r->fc.map = NULL; }
    /* Only free the malloc'd fallback — inline_buf is part of the
       caller's own RecordRef and needs no explicit release. */
    if (r->v2_buf && r->v2_buf != r->inline_buf) free(r->v2_buf);
    r->v2_buf = NULL;
    r->key = r->val = NULL;
    r->klen = r->vlen = 0;
}
```
Replace with:
```c
void release_record_ref(RecordRef *r) {
    if (!r) return;
    /* Only free the malloc'd fallback — inline_buf is part of the
       caller's own RecordRef and needs no explicit release. */
    if (r->v2_buf && r->v2_buf != r->inline_buf) free(r->v2_buf);
    r->v2_buf = NULL;
    r->key = r->val = NULL;
    r->klen = r->vlen = 0;
}
```

Remove the live `count_scan_cb_flush_thread()` call site in this file (see
3d for why the function itself is being deleted). Anchor (exact existing
text, inside `od_seg_file_worker`):
```c
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    /* Drain per-thread count accumulator (count_scan_cb) so the
       orchestrator sees this worker's contribution after parallel_for
       joins.  No-op for callbacks that don't use the TLS counter. */
    count_scan_cb_flush_thread();
    return NULL;
}
```
Replace with:
```c
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    return NULL;
}
```
(The other live call site, in `agg_od_seg_worker`, is removed as part of
3d, since that function lives in `query_aggregate.c`. The third call site,
in `scan_worker`, was already removed above as part of deleting
`scan_worker` itself.)

### 3d. `query.c`

Remove `CountCtx`, `count_local`, `count_scan_cb`, and
`count_scan_cb_flush_thread` together — see the inventory table above for
why all four have to go in one pass (`count_local.bound_cc` is typed
`CountCtx *` and cannot survive `CountCtx`'s deletion; once `count_scan_cb`
is gone nothing ever populates `count_local.pending` again, so keeping
`count_scan_cb_flush_thread` around would only add a permanent no-op).
Anchor (exact existing text) start:
```c
typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    int count;
    QueryDeadline *deadline;
    int dl_counter;
    /* Hot-path single-leaf pre-resolved criterion — set once before the
     * scan starts when tree is a CNODE_LEAF with a compiled typed
     * criterion.  Skips criteria_match_tree's dispatch (CNODE_AND/OR
     * recursion + indirect function-pointer chain that LTO can't
     * devirtualize through the seg-scan callback pointer cascade).
     * NULL => fall back to the general criteria_match_tree path. */
    const CompiledCriterion *single_leaf_cc;
} CountCtx;
```
End anchor — delete up to and including `count_scan_cb_flush_thread`'s
closing brace, stopping immediately before (exact existing text, do not
delete this line or anything after it):
```c
void cmd_explain_tree(const char *db_root, const char *object, CriteriaNode *tree,
```
This widened span (relative to an earlier draft of this plan, which stopped
before `count_scan_cb_flush_thread`) also removes: the "Per-thread
accumulator for count_scan_cb..." comment block, the `count_local` TLS
struct itself, `count_scan_cb`, its own "Drain this thread's pending
count..." doc comment, and `count_scan_cb_flush_thread`'s definition — all
five must go together (see above).

Remove the now-dangling prototype. Anchor (exact existing text,
`query_internal.h:137-138`):
```c
/* query.c — count scan (used by query_find.c::scan_worker) */
void count_scan_cb_flush_thread(void);
```
Delete both lines entirely (no replacement).

Remove the remaining live call site in this file. Anchor (exact existing
text, inside `agg_od_seg_worker` — note this function lives in
`query_aggregate.c`, not `query.c`; do this edit in that file):
```c
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    count_scan_cb_flush_thread();
    return NULL;
}
```
Replace with:
```c
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    return NULL;
}
```
(The sibling call site in `od_seg_file_worker` is removed as part of 3c,
below, since that function lives in `query_find.c`.)

Rewrite the `cmd_count` fallback branch. Anchor (exact existing text):
```c
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) {
            int64_t match_count = 0;
            scan_shards_v2_o_direct_match(sdb, &fs, fast_cc,
                                            fast_cc ? NULL : tree,
                                            &dl, &match_count);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%lld\n", (long long)match_count);
        } else {
            CountCtx ctx = { tree, &fs, 0, &dl, 0, fast_cc };
            scan_shards(data_dir, sch.slot_size, count_scan_cb, &ctx);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%d\n", ctx.count);
        }
```
Replace with:
```c
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) {
            int64_t match_count = 0;
            scan_shards_v2_o_direct_match(sdb, &fs, fast_cc,
                                            fast_cc ? NULL : tree,
                                            &dl, &match_count);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%lld\n", (long long)match_count);
        } else {
            OUT("{\"error\":\"object not open\"}\n");
        }
```
(This is the "breaking change" flagged above — a `slotcask_registry_get`
failure now surfaces as an error instead of silently reporting 0. No
existing test in the suite forces this condition for *any* command —
`cmd_rebuild_kf` (`query_maint.c:867`) hits the identical
`slotcask_registry_get` NULL path and emits the same
`{"error":"object not open"}` text but has no coverage for it either.
`test_count_object_not_open_run` in `test_get_fields.c` (Task 1) covers
this. Two things this test must get right, both root-caused in review
round 2 (Finding 1, Blocker) — get either wrong and the test cannot go
green even with this fix applied:
  1. **Non-empty, non-indexed criteria.** `count` with empty/absent
     criteria takes the O(1) `get_live_count` metadata fast path in
     `cmd_count` (see the top of this same file, `criteria_json[0] ==
     '\0'` check) and never calls `slotcask_registry_get` at all — it can
     never observe an open failure. The test must send a criterion on a
     field that has no index (so no index-driven plan branch intercepts
     it before reaching this file's `else` fallback at the bottom of
     `cmd_count_with_tree`).
  2. **A genuinely fresh open attempt.** `slotcask_registry_get` caches
     the opened `SlotcaskDb *` in the daemon's process-wide registry the
     first time any command opens the object (e.g. the `insert` that
     seeds the test's one record, per `storage.c` ~line 1438). `chmod
     000`ing the kf shard file *after* that insert, against the *same*
     still-running daemon, will not force a reopen — the cached handle is
     already open and stays open. The test must stop the daemon (keeping
     its db_root/port, not `rm -rf`ing them), chmod the file while nothing
     has it open, then start a fresh daemon process at the same
     db_root/port so the first count request forces a real `open()` that
     fails on the now-unreadable file.
Restore the file's permissions afterward (`chmod` back) so the sandbox
doesn't leak an unreadable file. Skip this case (with a clear diagnostic,
not a silent pass) if running as root, since root bypasses file permission
checks and the induced failure won't occur — check via `geteuid() == 0`.)

### 3e. `server.c`

Remove the three ucache init/shutdown call sites (data_dir/db_root startup
and shutdown sequences). Three separate one-line anchors:
```c
    fcache_init(g_fcache_cap);
```
Delete this line (`server.c:3568`, inside `cmd_server`'s startup sequence;
`slotcask_init(g_fcache_cap, g_fcache_cap);` immediately below it stays —
that's the live v2 cache init).
```c
    grow_recovery(db_root);
```
Delete this line (`server.c:3606`).
```c
    fcache_shutdown();
```
Delete this line (`server.c:3765`).

Replace the two `ucache_stats` calls in the diagnostics handlers with
literal zeros — the surrounding declarations already default to 0, so this
is a pure deletion. Anchor (exact existing text, appears twice — once in
`stats`, once in `stats-prom`; both instances get the same one-line
deletion, `replace_all` is appropriate here):
```c
        ucache_stats(&uc_used, &uc_total, &uc_bytes);
        bt_cache_stats(&bc_used, &bc_total, &bc_bytes);
```
Replace both occurrences with:
```c
        bt_cache_stats(&bc_used, &bc_total, &bc_bytes);
```

Both handlers separately read `g_ucache_hits`/`g_ucache_misses` (the fields
being deleted from `ShardDb` in 3i below) into `u_hits`/`u_miss`, which then
feed both the JSON/table `stats` output and the `stats-prom` counters.
These stay as permanent-zero metrics (not dropped — see Task 5) so
existing monitoring dashboards keep the field/counter names. Anchor (exact
existing text, appears twice — once in `stats`, once in `stats-prom`;
`replace_all` is appropriate):
```c
        uint64_t u_hits   = __atomic_load_n(&g_ucache_hits,    __ATOMIC_RELAXED);
        uint64_t u_miss   = __atomic_load_n(&g_ucache_misses,  __ATOMIC_RELAXED);
```
Replace both occurrences with:
```c
        uint64_t u_hits   = 0;
        uint64_t u_miss   = 0;
```

Fix a stale comment that will otherwise describe removed functionality.
Anchor (exact existing text, `server.c:3366-3372`, just before the
`RLIMIT_NOFILE` block in `cmd_server`'s startup sequence):
```c
    /* Raise the file-descriptor soft limit to the hard limit. ucache holds 1
       fd per cached shard and briefly 2 during ucache_grow_shard (new + retired
       for grace-period). At FCACHE_MAX=4096 defaults, peak need is ~8k fds —
       well above the 1024 default on many distros. Shell-default limits cause
       EMFILE inside ucache_grow_shard at high split counts. Soft → hard needs
       no privilege. If the hard limit itself is below a practical floor, warn
       with actionable guidance. */
```
Replace with:
```c
    /* Raise the file-descriptor soft limit to the hard limit. Each populated
       kfcache, segcache, bt_cache, and bm_cache entry holds 1 fd; a kf
       resplit temporarily opens an additional fd while rebuilding/remapping
       that shard. With FCACHE_MAX=4096, the configured cache capacities alone
       can approach 10k fds — well above the 1024 default on many distros.
       Shell-default limits can therefore cause EMFILE under cache pressure or
       during resplit. Soft → hard needs no privilege. If the hard limit itself
       is below a practical floor, warn with actionable guidance. */
```

Fix a second stale comment describing a v1/v2 coexistence that no longer
applies — this binary refuses to load v1 objects at all (see CLAUDE.md),
so there is no "v1 objects continue to use ucache" case left once ucache
itself is removed. Anchor (exact existing text, `server.c:3595-3597`,
immediately above the `slotcask_init` call in the same startup sequence):
```c
    /* Slotcask kfcache + segcache both sized from FCACHE_MAX. v2 (slotcask)
       objects route reads/writes through these; v1 (legacy) objects continue
       to use ucache. Both engines coexist until migration. */
```
Replace with:
```c
    /* Slotcask kfcache + segcache both sized from FCACHE_MAX; all live
       objects are v2 and route reads/writes through these. */
```

### 3f. `embedded.c`

Three one-line deletions:
```c
    fcache_init(db->fcache_cap);
```
Delete (`embedded.c:115`; `slotcask_init(db->fcache_cap, db->fcache_cap);`
immediately below stays).
```c
    grow_recovery(db->db_root);
```
Delete (`embedded.c:125`).

`ucache_shutdown();` appears twice (`embedded.c:292` and `embedded.c:405`) —
read enough surrounding context at execution time to confirm both are
simple standalone statement lines (not part of a larger expression) before
using `replace_all`; delete both occurrences.

Remove the `g_ucache_table_mutex` init/destroy pair (the mutex field itself
is deleted from `ShardDb` in 3i below). Anchor (exact existing text,
`embedded.c:23`, inside `db_mutexes_init`, between the `g_idx_lock` and
`g_counts_lock` inits):
```c
    pthread_mutex_init(&g_idx_lock,              NULL);
    pthread_mutex_init(&g_ucache_table_mutex,    NULL);
    pthread_mutex_init(&g_counts_lock,           NULL);
```
Replace with:
```c
    pthread_mutex_init(&g_idx_lock,              NULL);
    pthread_mutex_init(&g_counts_lock,           NULL);
```
Anchor (exact existing text, `embedded.c:374`, inside `db_mutexes_destroy`,
same relative position):
```c
    pthread_mutex_destroy(&g_idx_lock);
    pthread_mutex_destroy(&g_ucache_table_mutex);
    pthread_mutex_destroy(&g_counts_lock);
```
Replace with:
```c
    pthread_mutex_destroy(&g_idx_lock);
    pthread_mutex_destroy(&g_counts_lock);
```

### 3g. `query_schema.c`

Two one-line deletions:
```c
    fcache_invalidate(inv_path);
```
Delete (`query_schema.c:1163`).
```c
    fcache_invalidate(obj_dir);
```
Delete (`query_schema.c:1438`).

### 3h. `query_maint.c`

Anchor (exact existing text, `query_maint.c:636-641`):
```c
    /* Drop caches first so readers can't pin the old mappings while we swap.
       fcache_invalidate handles data shards; the btree page cache holds open
       fd+mmap per <field>/<NNN>.idx, so walk indexes/ explicitly (same
       pattern as index.c::reindex_clean_legacy). */
    fcache_invalidate(obj_dir);
    invalidate_idx_cache(db_root, object);
```
Replace with:
```c
    /* Drop caches first so readers can't pin the old mappings while we swap.
       slotcask_registry_invalidate (below) handles data shards; the btree
       page cache holds open fd+mmap per <field>/<NNN>.idx, so walk
       indexes/ explicitly (same pattern as index.c::reindex_clean_legacy). */
    invalidate_idx_cache(db_root, object);
```

Second call site — anchor (exact existing text, `query_maint.c:899`, no
surrounding comment to fix):
```c
    fcache_invalidate(obj_dir);
```
Delete this line (verify at execution time that this exact line appears
only once more in the file after the 636-641 edit above — the earlier
inventory table confirms exactly two occurrences total in this file).

Trim the three stale "v1 only" comments (wording only, no code change):

Anchor (exact existing text, `query_maint.c:203`):
```c
    reset_deleted_count(db_root, object);  /* v1 only; no-op for v2 */
```
Replace with:
```c
    reset_deleted_count(db_root, object);  /* no-op for v2 (kf-derived counts) */
```

Anchor (exact existing text, `query_maint.c:782-784`):
```c
    /* `grows` = log2(max_slots / initial). v1 uses INITIAL_SLOTS=256; v2 uses
       the splits-tier initial from slotcask_default_slots_for_splits(). */
    uint32_t initial = (uint32_t)slotcask_default_slots_for_splits(sch.splits);
```
Replace with:
```c
    /* `grows` = log2(max_slots / initial), initial from
       slotcask_default_slots_for_splits(). */
    uint32_t initial = (uint32_t)slotcask_default_slots_for_splits(sch.splits);
```

Anchor (exact existing text, `query_maint.c:853-858`):
```c
/* recount: report current live-record count. For v2 this is kf-derived
   (sum per-shard kf headers — total - deleted = live), so the result
   matches what get_live_count returns and `set_count` is a no-op. The
   command is functionally redundant on v2 since the kf headers are
   already authoritative; kept as a fast diagnostic / parity wrapper.
    v1 still has the full text-counts file write path below. */
```
Replace with:
```c
/* recount: report current live-record count. This is kf-derived (sum
   per-shard kf headers — total - deleted = live), so the result matches
   what get_live_count returns and `set_count` is a no-op. The command is
   functionally redundant since the kf headers are already authoritative;
   kept as a fast diagnostic / parity wrapper. */
```

### 3i. `shard_db_internal.h`

Remove the `ShardDb` struct fields backing the ucache subsystem — an
earlier draft of this plan didn't touch this file at all, even though it's
where every `g_ucache_*` macro used across 3a/3e/3f ultimately resolves.
Two separate anchors (exact existing text):
```c
    /* stats counters */
    uint64_t ucache_hits;
    uint64_t ucache_misses;
    uint64_t bt_cache_hits;
```
Replace with:
```c
    /* stats counters */
    uint64_t bt_cache_hits;
```
```c
    /* ucache (storage.c) */
    UCacheEntry         *ucache;
    int                  ucache_slots;
    int                  ucache_count;
    pthread_mutex_t      ucache_table_mutex;
    volatile uint64_t    ucache_clock;

    /* counts cache (storage.c) */
```
Replace with:
```c
    /* counts cache (storage.c) */
```

Remove the matching macros. Two separate anchors (exact existing text):
```c
#define g_ucache_hits               (g_db->ucache_hits)
#define g_ucache_misses             (g_db->ucache_misses)
#define g_bt_cache_hits             (g_db->bt_cache_hits)
```
Replace with:
```c
#define g_bt_cache_hits             (g_db->bt_cache_hits)
```
```c
/* storage.c */
#define g_ucache                    (g_db->ucache)
#define g_ucache_slots              (g_db->ucache_slots)
#define g_ucache_count              (g_db->ucache_count)
#define g_ucache_table_mutex        (g_db->ucache_table_mutex)
#define g_ucache_clock              (g_db->ucache_clock)
#define g_counts_cache              (g_db->counts_cache)
```
Replace with:
```c
#define g_counts_cache              (g_db->counts_cache)
```

### 3j. `types.h` — dead v1 shard-file layout scaffolding (review round 2, Finding 6, Medium)

Once 3a (storage.c), 3c (query_find.c), and 3e (server.c) land, the v1
shard-file layout macros, the `ShardHeader` struct, and its four
offset-helper functions have no remaining caller anywhere in the codebase.
Confirmed by direct grep against the current (pre-fix) tree: every
non-comment use of `zoneA_off`/`zoneB_off`/`shard_zoneA_end`/
`shard_file_size`/`HEADER_SIZE`/`SHARD_HDR_SIZE`/`INITIAL_SLOTS`/
`SHARD_MAGIC`/`SHARD_VERSION`/`GROW_LOAD_NUM`/`GROW_LOAD_DEN`/`ShardHeader`
lives in exactly three places: `storage.c` (`shard_init_or_read_header`,
`ucache_grow_to`, `ucache_bump_record_count`, `grow_rehash_worker`,
`ucache_grow_shard` — all already in 3a's removal list), `query_find.c`'s
v1 `scan_one_shard` (already removed by 3c), and `server.c`'s v1-ucache
`get`+`fields` branch (already replaced by Task 2c's rewrite). No v2 code
path touches any of these symbols. `query_maint.c`'s two mentions
(`shard-stats`'s doc comment and the `grows`-estimate comment, both fixed
in 3h) are comment-only and already handled.

`SlotHeader` (the 24-byte Zone-A-entry struct, defined directly below
`ShardHeader` in `types.h`) is **not** part of this removal — it's a live
scan-callback abstraction type shared by v2 code (e.g. `scan_shards_v2_o_direct`
callback shapes) and must be preserved exactly as-is. `SLOT_SIZE` (a
separate, already-unused-before-this-plan constant) is also out of scope —
it's pre-existing dead code unrelated to the v1/v2 split and removing it
would be an unrelated change.

Remove the layout macros. Anchor (exact existing text):
```c
#define HEADER_SIZE 24               /* Zone A entry size */
#define SHARD_HDR_SIZE   32          /* ShardHeader at file offset 0 */
#define INITIAL_SLOTS    256         /* starting slots_per_shard for new shards */
```
Delete these three lines entirely (no replacement).

Remove the grow-load and magic/version macros. Anchor (exact existing text):
```c
#define GROW_LOAD_NUM    1           /* grow when count*DEN >= slots*NUM (50%) */
#define GROW_LOAD_DEN    2
#define SHARD_MAGIC      0x564B4853u /* 'SHKV' little-endian */
#define SHARD_VERSION    1u
```
Delete these four lines entirely (no replacement).

Remove the `ShardHeader` struct and its leading doc comment. Anchor (exact
existing text):
```c
/* Per-shard header at byte 0 of each shard file. Records slots_per_shard
   so a restart picks up the current grown size directly from the header
   instead of trying to derive it from file size. */
typedef struct __attribute__((packed)) {
    uint32_t magic;                  /* SHARD_MAGIC */
    uint32_t version;                /* SHARD_VERSION */
    uint32_t slots_per_shard;        /* current power-of-two slot count */
    uint32_t record_count;           /* active (non-tombstoned) records */
    uint8_t  reserved[16];
} ShardHeader;
```
Delete this entire block (comment + struct), with no replacement.

Remove the "Shard file layout" comment and the four offset-helper
functions. Anchor (exact existing text):
```c
/* Shard file layout:
     [ShardHeader: 32B]
     [Zone A: slots_per_shard * 24B headers]
     [Zone B: slots_per_shard * slot_size payloads]
   Payload holds key+value packed (key_len from header determines value offset).
   slots_per_shard is a per-shard value recorded in ShardHeader. */
static inline size_t zoneA_off(uint32_t slot) {
    return SHARD_HDR_SIZE + (size_t)slot * HEADER_SIZE;
}
static inline size_t zoneB_off(uint32_t slot, uint32_t slots_per_shard, uint32_t slot_size) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * HEADER_SIZE + (size_t)slot * slot_size;
}
static inline size_t shard_zoneA_end(uint32_t slots_per_shard) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * HEADER_SIZE;
}
static inline size_t shard_file_size(uint32_t slots_per_shard, uint32_t slot_size) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * (HEADER_SIZE + slot_size);
}
```
Delete this entire block (comment + all four functions), with no
replacement.

**Verification for this subtask is intentionally deferred until after
Task 3k.** Task 3k removes the remaining comment-only `ShardHeader` and
layout-name references in `query_maint.c`/`bitmap.h`; expecting a global
zero-match result immediately after 3j would fail on those later cleanup
anchors even though the live types/helpers are already gone. Run 3k's two
self-verification greps after all its comment edits; Task 4 repeats the
same symbol-level check as the final gate.

### 3k. Stale-comment cleanup (review round 2, Finding 4 — remaining files)

Comment-only fixes, no functional code change, across both files with
functional edits in earlier Task 3 subtasks and files otherwise untouched
by Task 3. Verified via
`grep -rn "ucache" src/db/*.c src/db/*.h` against every hit that survives
Task 3a/3b/3e/3f/3i's deletions and Task 3c's `scan_one_shard` removal —
each remaining hit is listed below with its exact anchor. Two further
fixes describe `scan_dispatch` as having a v1 fallback it never had
(checked directly against `scan_dispatch`'s actual body in `query_find.c`
— no caller anywhere inspects its return value or falls back to a legacy
path), and one is a test docstring naming the wrong cache.

**`query_bulk.c`** (12 sites):

Anchor (`query_bulk.c:13`):
```c
/* Bulk ops use ucache (unified shard cache in storage.c) */
```
Replace with:
```c
/* Bulk ops use the v2 slotcask storage backend (registry-cached
   SlotcaskDb handles). */
```

Anchor (`BulkInsRecord`'s neighboring struct-doc comment):
```c
/* Per-shard bucket + worker arguments. Each bucket targets exactly one
   shard so the worker can take the ucache wrlock **once**, write every
   record in the bucket, and release **once** — avoiding per-record
   acquire/release churn. Idx entries are collected into per-worker arrays
   and merged into the caller's global arrays after the worker returns
   (same shape bulk-delete's bulk_del_shard_worker uses). */
```
Replace with:
```c
/* Per-shard bucket + worker arguments. Each bucket targets exactly one
   shard so the worker can take the kf-shard wrlock **once**, write every
   record in the bucket, and release **once** — avoiding per-record
   acquire/release churn. Idx entries are collected into per-worker arrays
   and merged into the caller's global arrays after the worker returns
   (same shape bulk-delete's bulk_del_shard_worker uses). */
```

Anchor (profiling comment; note `grow_ms`/`grow_count` are aggregated
post-join but never actually assigned anywhere in this file — a
pre-existing dead-field issue, out of scope for this plan, left alone
here):
```c
    /* Phase-2 profiling: total worker wall time and time spent inside
       ucache_grow_shard. Aggregated post-join to show "of this much
       Phase 2 time, X ms was grow." Helps isolate rehash cost. */
```
Replace with:
```c
    /* Phase-2 profiling: total worker wall time and time spent growing a
       shard mid-batch. Aggregated post-join to show "of this much
       Phase 2 time, X ms was grow." Helps isolate rehash cost. */
```

Anchor (`bulk_insert_shard_worker`'s doc comment):
```c
/* Probe + write every record in one shard's bucket under a single ucache
   wrlock held from start to finish. On shard-full, release the lock, grow
   the shard, reacquire, and retry the **same** record index — avoids
   per-record churn. Collects index entries into sw->idx_pairs for later
   merge/bulk-build. pthread-compatible signature: workers in different
   shard buckets never touch each other's shards, so the wrlocks are
   disjoint and no cross-worker coordination is needed. */
```
Replace with:
```c
/* Probe + write every record in one shard's bucket under a single
   kf-shard wrlock held from start to finish. On shard-full, release the
   lock, grow the shard, reacquire, and retry the **same** record index —
   avoids per-record churn. Collects index entries into sw->idx_pairs for
   later merge/bulk-build. pthread-compatible signature: workers in
   different shard buckets never touch each other's shards, so the
   wrlocks are disjoint and no cross-worker coordination is needed. */
```

Anchor (standalone comment):
```c
    /* ucache handles shard caching */
```
Replace with:
```c
    /* The v2 kfcache/segcache (registry-cached) handle shard caching
       automatically -- no manual fd/mmap management needed here. */
```

Anchor (Phase 1.5 comment):
```c
    /* ===== Phase 1.5: bucket records by shard_id so each worker owns one shard's
       writes and can hold the ucache wrlock once for the entire bucket.
       OOM at any of the allocs below frees every prior allocation
       (records, arena, idx_pairs[], idx_pair_*, json buffer) in reverse
       order before bailing — same cleanup the success path runs at the
       function tail, just earlier. */
```
Replace with:
```c
    /* ===== Phase 1.5: bucket records by shard_id so each worker owns one shard's
       writes and can hold the kf-shard wrlock once for the entire bucket.
       OOM at any of the allocs below frees every prior allocation
       (records, arena, idx_pairs[], idx_pair_*, json buffer) in reverse
       order before bailing — same cleanup the success path runs at the
       function tail, just earlier. */
```

Anchor (Phase 2 comment):
```c
    /* ===== Phase 2: run shard workers in parallel. Each worker owns one shard's
       writes so ucache wrlocks are disjoint across workers — no cross-worker
       coordination needed. Batched pthread_create/join pattern matches
       bulk_del_shard_worker. Serial fallback when thread count ≤ 1 or workload
       is small enough that spawn/join overhead would dominate. */
```
Replace with:
```c
    /* ===== Phase 2: run shard workers in parallel. Each worker owns one shard's
       writes so kf-shard wrlocks are disjoint across workers — no cross-worker
       coordination needed. Batched pthread_create/join pattern matches
       bulk_del_shard_worker. Serial fallback when thread count ≤ 1 or workload
       is small enough that spawn/join overhead would dominate. */
```

Anchor (standalone comment):
```c
    /* ucache keeps mmaps open — OS flushes dirty pages */
```
Replace with:
```c
    /* The v2 kfcache/segcache keep mmaps open -- OS flushes dirty pages. */
```

Anchor (bulk-update phase 2 worker doc comment):
```c
/* Bulk-update phase 2 worker — one per shard, holds the ucache wrlock once
   for the whole bucket. Index updates (btree_insert/btree_delete) are
   serialised by bt_cache_lock inside the btree layer, so concurrent workers
   hitting the same index file are safe. */
```
Replace with:
```c
/* Bulk-update phase 2 worker — one per shard, holds the kf-shard wrlock
   once for the whole bucket. Index updates (btree_insert/btree_delete) are
   serialised by bt_cache_lock inside the btree layer, so concurrent workers
   hitting the same index file are safe. */
```

Anchor (Phase 2 write comment):
```c
    /* Phase 2: Write — bucket matched keys by shard and fan out one worker
       per shard. Each worker takes the ucache wrlock **once** per shard,
       walks its bucket end-to-end, and releases **once** — matching the
       bulk-insert pattern. Index updates (btree_insert/btree_delete) are
       serialised internally by bt_cache_lock, so concurrent workers are
       safe. */
```
Replace with:
```c
    /* Phase 2: Write — bucket matched keys by shard and fan out one worker
       per shard. Each worker takes the kf-shard wrlock **once** per shard,
       walks its bucket end-to-end, and releases **once** — matching the
       bulk-insert pattern. Index updates (btree_insert/btree_delete) are
       serialised internally by bt_cache_lock, so concurrent workers are
       safe. */
```

Anchor (bulk-update-json worker doc comment, exact existing text):
```c
/* === v2 bulk-update-json worker ===
 *
 * v1 patches fields in place under a single ucache wrlock (cheap because the
 * Zone B record lives at a fixed offset). v2 (slotcask) follows the engine's
 * locked design: every update allocates a new slot (snake-game pool reuse),
 * tombstones the old. So per record we:
 *   1. read the old typed payload via slotcask_get
 *   2. memcpy into a heap buffer; encode_field for every touched field
 *      + auto_update fields
 *   3. slotcask_upsert_with_hooks(require_existing=1) with a pre_commit hook
 *      that performs the per-field index drop/insert diff
 *
 * Index entries are written synchronously inside the hook (mirror v1: every
 * indexed field that moved gets `delete_index_entry` + `write_index_entry`).
 * No bulk btree merge phase — the merge phase belongs to bulk-INSERT where
 * entries point at fresh records. */
```
Replace with (drops the v1 comparison — v1 no longer exists anywhere in
the tree once Task 3 lands):
```c
/* === bulk-update-json worker ===
 *
 * slotcask's locked design means every update allocates a new slot
 * (snake-game pool reuse), tombstoning the old rather than patching in
 * place. So per record we:
 *   1. read the old typed payload via slotcask_get
 *   2. memcpy into a heap buffer; encode_field for every touched field
 *      + auto_update fields
 *   3. slotcask_upsert_with_hooks(require_existing=1) with a pre_commit hook
 *      that performs the per-field index drop/insert diff
 *
 * Index entries are written synchronously inside the hook (every indexed
 * field that moved gets `delete_index_entry` + `write_index_entry`).
 * No bulk btree merge phase — the merge phase belongs to bulk-INSERT where
 * entries point at fresh records. */
```

Anchor (concurrency caveat comment, exact existing text):
```c
/* Concurrency caveat (v2 bulk-update-json + delim, partial-field):
   the read-old → patch → upsert sequence is NOT atomic. v1 holds the
   ucache wrlock across the whole shard worker, so partial updates see
   a consistent snapshot. v2 acquires the kf-shard wrlock per record,
   which gives finer parallelism but means a concurrent writer between
   the slotcask_get and the upsert can lose the racing writer's changes
   to fields THIS bulk doesn't touch. Bulk-update has no CAS semantics
   in either version (`if_json` is not a per-record knob in the bulk
   protocol), so the loss is silent. Use single-record cmd_update with
   `if_json` for strict CAS; bulk-update is documented as
   "last-writer-wins on the touched fields, snapshot-of-read on the
   untouched fields." */
```
Replace with (drops the v1 comparison):
```c
/* Concurrency caveat (bulk-update-json + delim, partial-field):
   the read-old → patch → upsert sequence is NOT atomic. The kf-shard
   wrlock is acquired per record, not held for the whole worker, so a
   concurrent writer between the slotcask_get and the upsert can lose the
   racing writer's changes to fields THIS bulk doesn't touch. Bulk-update
   has no CAS semantics (`if_json` is not a per-record knob in the bulk
   protocol), so the loss is silent. Use single-record cmd_update with
   `if_json` for strict CAS; bulk-update is documented as
   "last-writer-wins on the touched fields, snapshot-of-read on the
   untouched fields." */
```

**`query_maint.c`** — `cmd_restore`'s doc comment (not covered by Task
3h's anchors, which touch different lines in this file). Anchor (exact
existing text):
```c
/* ========== RESTORE ==========
   Symmetric to backup: copies data/ + indexes/ + metadata/ + fields.conf
   from `<obj>/backup/<from>` over the live tree, and ensures the
   schema.conf line is in place from object.json.
   Refuses if any live state would conflict, unless force=1. Holds the
   object's write lock for the whole operation; invalidates ucache + bt
   cache + idx cache + schema caches before the swap so the next reader
   sees the new mappings. */
```
Replace with:
```c
/* ========== RESTORE ==========
   Symmetric to backup: copies data/ + indexes/ + metadata/ + fields.conf
   from `<obj>/backup/<from>` over the live tree, and ensures the
   schema.conf line is in place from object.json.
   Refuses if any live state would conflict, unless force=1. Holds the
   object's write lock for the whole operation; invalidates the slotcask
   registry + bt cache + idx cache + schema caches before the swap so the
   next reader sees the new mappings. */
```

Second `query_maint.c` site — `cmd_shard_stats` still names the removed
v1 `ShardHeader` and the wrong 32-byte header size. Anchor (exact existing
text):
```c
/* shard-stats: walk every shard file under data/, read each ShardHeader, report slots/records/load
   plus a hint when splits may be too low. Cheap — reads only 32B per shard.
   as_table=1 emits ASCII table; as_table=0 emits JSON. */
```
Replace with:
```c
/* shard-stats: walk every keyfile shard under data/kf/, read its
   SlotcaskKfHeader, and report slots/records/load plus a hint when splits
   may be too low. Cheap — reads only the 24-byte header per shard.
   as_table=1 emits ASCII table; as_table=0 emits JSON. */
```

**`btree.c`** (three sites):

Anchor (the "File management" header comment):
```c
/* ========== File management ==========
   Unified ucache-style btree cache: one MAP_SHARED mapping per file,
   per-entry pthread_rwlock_t (readers share, writers exclusive). One open
   path for both modes — no MAP_PRIVATE snapshot, no separate writer flock,
   no refcount-based invalidation dance. Mirrors storage.c's UCacheEntry
   model for shard files. */
```
Replace with:
```c
/* ========== File management ==========
   Unified btree cache: one MAP_SHARED mapping per file, per-entry
   pthread_rwlock_t (readers share, writers exclusive). One open path for
   both modes — no MAP_PRIVATE snapshot, no separate writer flock, no
   refcount-based invalidation dance. */
```

Anchor (`bt_acquire`'s doc comment):
```c
/* Acquire a btree handle. writer=0 takes rdlock, writer=1 takes wrlock and
   creates the file (with a fresh header) if missing. On cache pressure we
   evict the least-recently-used slot; if the cache isn't initialised or
   eviction can't free a slot, we fall back to an uncached mapping (slot=-1,
   no rwlock) — same hazard tradeoff as storage.c's ucache. */
```
Replace with (storage.c's ucache no longer exists once Task 3a lands, so
describe the tradeoff standalone):
```c
/* Acquire a btree handle. writer=0 takes rdlock, writer=1 takes wrlock and
   creates the file (with a fresh header) if missing. On cache pressure we
   evict the least-recently-used slot; if the cache isn't initialised or
   eviction can't free a slot, we fall back to an uncached mapping (slot=-1,
   no rwlock). MAP_SHARED keeps duplicate mappings byte-coherent, but
   concurrent uncached writers do not get the cache's rwlock serialization;
   that accepted cache-pressure hazard is unchanged here. */
```

Anchor (the "Adaptive strategy threshold" comment):
```c
    /* Adaptive strategy threshold. The pre-2026.05.1 measurement set this
       at 100:1 and noted point-insert was 10x slower than rebuild at 90:1
       because every btree_insert call did its own
       cache-invalidate + open + lock cycle. Per-call overhead is now ~1µs
       (single bt_acquire wrlock; ucache keeps the file mapped) so the
       crossover moved much closer to the algorithmic prediction:
```
Replace with:
```c
    /* Adaptive strategy threshold. The pre-2026.05.1 measurement set this
       at 100:1 and noted point-insert was 10x slower than rebuild at 90:1
       because every btree_insert call did its own
       cache-invalidate + open + lock cycle. Per-call overhead is now ~1µs
       (single bt_acquire wrlock; the cache keeps the file mapped) so the
       crossover moved much closer to the algorithmic prediction:
```

**`index.c`** — the directory-cleanup comment ahead of the type-aware
`cmd_add_indexes` rebuild. Anchor (exact existing text):
```c
            /* Per-shard layout: indexes/<field>/<NNN>.{idx,bm,tg}.
               Drop every cached btree mapping under this directory
               before rmrf so ucache doesn't keep stale fds alive.
               Bitmap (.bm) and trigram (.tg) files don't use ucache
               but the rmrf cleans them too — they get rebuilt below
               in the type-aware cmd_add_indexes path. */
```
Replace with (this comment actually describes the live `bt_cache`, not
the removed v1 ucache):
```c
            /* Per-shard layout: indexes/<field>/<NNN>.{idx,bm,tg}.
               Drop every cached btree mapping under this directory
               before rmrf so bt_cache doesn't keep stale fds alive.
               Bitmap (.bm) and trigram (.tg) files don't use bt_cache
               but the rmrf cleans them too — they get rebuilt below
               in the type-aware cmd_add_indexes path. */
```

**`bitmap.h`** — the `.bm` file-format doc comment (never touched by any
other Task 3 subtask). Anchor (exact existing text):
```c
 *       8  : slots       u32  data shard's slots_per_shard (matches ShardHeader)
```
Replace with (drops the reference to the `ShardHeader` struct removed by
Task 3j):
```c
 *       8  : slots       u32  matches the data shard's slots_per_shard
```

**`parallel.c`** — the top-of-file architecture doc comment (never
touched by any other Task 3 subtask). Anchor (exact existing text):
```c
/* Global compute-parallelism thread pool.
 *
 * Problem it solves: hot paths (bulk-insert Phase 2, parallel index build,
 * shard activation, scan_shards, ...) each used to spawn their own
 * pthread_create/join batch. Under N concurrent TCP callers, each spawning
 * P threads, the server ran N*P OS threads on 16 cores — 10x overcommit for
 * N=10,P=16 — and most of the wall time was OS scheduling delay, not work.
 *
 * Fix: a fixed-size worker pool sized by THREADS config (default = nproc).
 * All callers submit tasks to one shared queue; workers drain it. No
```
Replace with live `parallel_for` compute-pool callers. Do not name the
O_DIRECT scan workers here: they use the separate `parallel_for_io` pool
and queue, while this header documents the compute pool:
```c
/* Global compute-parallelism thread pool.
 *
 * Problem it solves: hot CPU paths (parallel index build, index-update
 * fan-out, OR-leaf planning, aggregate shard workers, ...) each used to
 * spawn their own
 * pthread_create/join batch. Under N concurrent TCP callers, each spawning
 * P threads, the server ran N*P OS threads on 16 cores — 10x overcommit for
 * N=10,P=16 — and most of the wall time was OS scheduling delay, not work.
 *
 * Fix: a fixed-size worker pool sized by THREADS config (default = nproc).
 * All callers submit tasks to one shared queue; workers drain it. No
```

**`query.c`** (two stale `scan_shards` diagnostics that would otherwise
make Task 4a's symbol grep fail):

Anchor (ordered-find fast-path comment):
```c
       emit the next `limit`. Bypasses scan_shards + qsort entirely.
```
Replace with:
```c
       emit the next `limit`. Bypasses the full-table scan + qsort entirely.
```

Anchor (`cmd_find_do` query-buffer warning):
```c
            LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: scan_shards returned rc=-2 (query buffer cap exceeded, object=%s)", object);
```
Replace with:
```c
            LOG_WARN(LOG_SUB_QUERY, "cmd_find_do: scan dispatch returned rc=-2 (query buffer cap exceeded, object=%s)", object);
```

**`query_aggregate.c`** — one standalone reference to the removed
`scan_shards` function in the `neq_eligible` count-only fast-path comment.
Do not rewrite the live compound identifiers `parallel_agg_scan_shards_v2`
or `parallel_agg_scan_shards`; Task 4a's exact-identifier grep deliberately
does not match those names. Anchor (exact existing text):
```c
        /* COUNT-only fast path: agg(count where neq=X) = live_count - count(eq=X).
           Skip the full-side scan_shards entirely (which decodes every record
           just to increment count) and use the metadata live_count instead.
           Saves ~150ms on a 1M table. */
```
Replace with:
```c
        /* COUNT-only fast path: agg(count where neq=X) = live_count - count(eq=X).
           Skip the full-side record scan entirely (which decodes every record
           just to increment count) and use the metadata live_count instead.
           Saves ~150ms on a 1M table. */
```

**`query_find.c`** — delete the dangling count-scan forward-declaration
comment. Task 3d removes `count_scan_cb`/`CountCtx`, and there is no
declaration under this comment to preserve. Anchor (exact existing text):
```c
/* Forward decl — definition lives alongside count_scan_cb in cmd_count.
   Drains the per-thread TLS count accumulator into the bound CountCtx;
   no-op for scan workers whose callback doesn't use TLS counting. */
```
Delete this entire comment with no replacement.

`query_find.c` — `scan_dispatch`'s own doc comment. Anchor (exact existing
text):
```c
/* Dispatch helper: callers that have a Schema in scope use this to pick
   the right scan path. Returns 0 on success, -1 if v2 dispatch failed
   (caller can fall back to the legacy path or report no rows). */
```
Replace with:
```c
/* Dispatch helper: callers that have a Schema in scope use this to pick
   the right scan path. Returns 0 on success, -1 if slotcask_registry_get
   failed to open the object (logged; no caller currently inspects the
   return value). */
```

`types.h` — the matching prototype-adjacent comment. Anchor (exact
existing text):
```c
/* Storage-version-aware scan dispatch. v2 objects route through the
   slotcask registry; v1 fall through to scan_shards. Returns -1 only
   when v2 dispatch fails (open-time error). */
```
Replace with:
```c
/* Scan dispatch: routes through the slotcask registry. Returns -1 only
   when dispatch fails (open-time error); no caller currently inspects the
   return value. */
```

`src/test/cases/test_restore.c` — the file's docstring names the removed
subsystem as what makes restored data visible; the actual mechanism
(`query_maint.c`'s `cmd_restore`, unchanged by this plan) is
`slotcask_registry_invalidate`, called right before the data/indexes/
metadata wipe-and-copy. Anchor (exact existing text):
```c
/* src/test/cases/test_restore.c
 * Backup → mutate → restore round-trip. Verifies the live data tree
 * is replaced by the backup snapshot, that ucache invalidation lets
 * subsequent reads see the restored state, and that the safety guards
 * (missing backup, non-empty live tree without --force) fire.
 */
```
Replace with:
```c
/* src/test/cases/test_restore.c
 * Backup → mutate → restore round-trip. Verifies the live data tree
 * is replaced by the backup snapshot, that slotcask registry invalidation
 * lets subsequent reads see the restored state, and that the safety guards
 * (missing backup, non-empty live tree without --force) fire.
 */
```

**Self-verification**: after applying every fix above, `grep -rn "ucache"
src/db/*.c src/db/*.h src/test/cases/test_restore.c` must return only the
intentionally-retained hits documented in Task 3e/Task 4b (the JSON stats
key, the text-table label, and the five `shard_db_ucache_*` Prometheus
metric names, each repeated in its HELP/TYPE/sample output lines) — zero
hits in any other file or comment.

Then run the deferred Task 3j/layout-and-count cleanup check:
```
grep -Ern "\<(ShardHeader|SHARD_HDR_SIZE|SHARD_MAGIC|SHARD_VERSION|INITIAL_SLOTS|GROW_LOAD_NUM|GROW_LOAD_DEN|HEADER_SIZE|zoneA_off|zoneB_off|shard_zoneA_end|shard_file_size|scan_shards|count_scan_cb)\>" src/db/*.c src/db/*.h
```
Expected: zero matches. This catches comment/string references as well as
identifiers, deliberately matching Task 4a's strict final expectation,
without false-positive substring matches inside live names such as
`parallel_agg_scan_shards`.

## Task 4 — build, full suite, targeted regression

1. `SKIP_TESTS=1 ./build.sh` — must complete with zero new compiler
   warnings (in particular: confirm no "defined but not used" / "no previous
   prototype" warnings for anything touched above — that's the signal that
   an unused symbol was missed).
2. `./build/bin/shard-db-test run test-get-fields` — must pass (this is the
   regression test from Task 1, now green).
3. `./build/bin/shard-db-test run-all` — full suite, must be 100% green.
   Pay particular attention to any case touching `stats`, `stats-prom`,
   `count` on a freshly-created or edge-case object, `backup`/`restore`,
   `vacuum`, or `rebuild-kf` — those are the call sites this plan touches
   indirectly.
4. Verify no dead ucache *code* survives, while allowing the intentionally
   retained wire/metric names. Two separate greps, because a single plain
   `ucache` grep cannot distinguish removed C symbols from retained
   string-literal names (review round 2, Finding 4, High — the plan's
   earlier draft used one grep that could never pass, since Task 3e
   deliberately keeps `stats`/`stats-prom`'s `"ucache"` JSON key, text-table
   label, and five `shard_db_ucache_*` Prometheus metric names as
   permanent-zero back-compat fields — see Task 3e and Task 5):

   4a. Exact identifier-name grep. It covers both the ucache API surface
   (3a-3i) and the dead v1 layout scaffolding (3j), and intentionally also
   catches those exact names in comments/string literals so stale references
   cannot survive:
   ```
   grep -Ern "\<(fcache_init|fcache_shutdown|fcache_get_read|fcache_release|fcache_invalidate|ucache_get_write|ucache_write_release|ucache_nudge_writeback|ucache_grow_to|ucache_grow_shard|ucache_maybe_grow|ucache_peek_slots|ucache_bump_record_count|ucache_stats|ucache_shutdown|ucache_entry|ucache_slot_count|ucache_clock|grow_recovery|UCacheEntry|FcacheRead|ShardHeader|SHARD_HDR_SIZE|SHARD_MAGIC|SHARD_VERSION|INITIAL_SLOTS|GROW_LOAD_NUM|GROW_LOAD_DEN|HEADER_SIZE|zoneA_off|zoneB_off|shard_zoneA_end|shard_file_size|scan_shards|build_shard_path|build_shard_filename|count_scan_cb)\>" src/db/*.c src/db/*.h
   ```
   Expected: zero matches. The explicit identifier boundaries are required:
   without them, alternatives such as `fcache_init`/`fcache_release` also
   match the live v2 symbols `kfcache_init`/`kfcache_release`, making this
   gate impossible to pass. Paste the actual output.

   4b. Confirm the *only* remaining `ucache` mentions in `src/db/` are the
   intentionally-retained string literals in `server.c`'s `stats`/
   `stats-prom` handlers — the JSON key `"ucache"`, the text-table label,
   and five `shard_db_ucache_*` Prometheus metric names, each emitted in
   HELP/TYPE/sample lines (Task 3e keeps these names for back-compat while
   hardcoding their values to 0; Task 3i
   removes the backing `g_ucache_hits`/`g_ucache_misses`/`ucache_clock`
   fields and macros entirely, so no C identifier named `ucache_*` should
   remain anywhere — only these string literals):
   ```
   grep -rn "ucache" src/db/*.c src/db/*.h
   ```
   Expected: every line is one of `server.c`'s `stats`/`stats-prom` output
   strings named above. Any match in a file/function not named in Task 3e,
   or any match that is a comment or a live C identifier rather than a
   string literal, is a plan gap — flag it and halt per `PLAN_NOTES.md`
   rather than deleting it ad hoc. Paste the actual output and annotate
   each line as retained-wire-name vs. unexpected.

## Task 5 — documentation sync

CORE-PROCESS's "Documentation sync" gate applies here as a **separate pass
after Task 4 is green** — do not start this pass until the code changes are
verified working; a doc-only pass on top of a still-changing code diff
risks drifting out of sync with the final shape of Task 3's edits.

Re-run the file-count grep at execution time rather than trusting a number
written days earlier — a prior draft of this plan claimed 21 files based on
an earlier investigation pass; re-verifying against the current tree found
**17** files under `docs/` referencing "ucache" (`grep -rl "ucache" docs/`),
plus **`CLAUDE.md` itself** (repo root, not under `docs/` — a separate
`grep -n "ucache" CLAUDE.md` is needed since it won't be caught by a
`docs/`-scoped search). Of those 17, **6 are live reference docs** that get
concrete edits below — `docs/cli/shard-cli.md` (5a), `docs/operations/
benchmarks.md` (5b), `docs/operations/monitoring.md` (5c), `docs/query-
protocol/diagnostics.md` (5d), `docs/reference/changelog.md` (5e),
and `docs/operations/tuning.md` (5h). `CLAUDE.md` is a seventh live
current-state file but is outside those 17 because it is not under
`docs/`; Task 5f edits it separately. The other 11 files are historical
plan/spec documents under
`docs/plans/` and `docs/superpowers/` (e.g.
`docs/plans/2026-06-10-embedded-mode.md`,
`docs/plans/2026-07-07-coverity-dead-code-cleanup.md`,
`docs/superpowers/specs/2026-07-11-error-logging-audit-findings.md`, and
this plan file itself) — point-in-time records of past decisions, not
current-state documentation. **Do not rewrite those** — mechanically
editing them to describe post-removal reality would falsify the historical
record they exist to preserve.

The literal `ucache` grep does not catch stale semantic aliases such as
"unified shard-mmap cache" or a dead `fcache_shutdown` name embedded in a
shutdown-sequence description. A second audit found five additional live
reference files requiring synchronized terminology/capacity fixes:
`docs/getting-started/configuration.md`, `docs/getting-started/embedded-mode.md`,
`docs/reference/limits.md`, `docs/concepts/storage-model.md`, and
`docs/concepts/concurrency.md`; Task 5i gives exact edits for all five.
If either audit finds another live file beyond the 6 literal-match docs,
`CLAUDE.md`, and those 5 semantic-match docs, treat it as a plan gap: stop
and write `PLAN_NOTES.md` rather than editing an undocumented file ad hoc.

Do **not** drop the `ucache` fields from the `stats`/`stats-prom`
JSON/Prometheus output schema itself in any of the edits below — Task 3e
keeps them present, permanently zero, precisely so existing dashboards
parsing those field/counter names don't break. Every edit below rewords
prose describing `ucache` as a live, populated subsystem; none of them
remove a documented field/metric name.

### 5a. `docs/cli/shard-cli.md`

Anchor (exact existing text):
```
6. **Stats** — open and watch `ucache.hits` tick up as you re-query.
```
Replace with:
```
6. **Stats** — open and watch `bt_cache.hits` tick up as you re-query
   (`ucache.hits` stays at 0 on v2 — see [monitoring.md](../operations/monitoring.md)).
```

### 5b. `docs/operations/benchmarks.md`

Anchor (exact existing text):
```
The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the unified shard cache (`ucache_get_write` per shard). Pre-2026.05.1 they did per-call `open + flock + mmap MAP_SHARED + munmap`, paying full page-fault tax per request.
```
Replace with:
```
The delete speedups come from `bulk_del_shard_worker` and `single_delete` paths now going through the v2 slotcask storage backend (registry-cached `SlotcaskDb` handles, one open mmap per kf shard) instead of the pre-2026.05.1 per-call `open + flock + mmap MAP_SHARED + munmap`, which paid full page-fault tax per request.
```

Anchor (exact existing text):
```
- **File-descriptor limit.** At `SPLITS ≥ 512`, `ucache_grow_shard` briefly holds 2 fds per shard during migration, so peak can hit ~8,256 fds at the default `FCACHE_MAX=4096`. The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit.
```
Replace with:
```
- **File-descriptor limit.** The server auto-raises its soft limit to the hard limit at startup (no privilege needed); if the hard limit itself is too low (shells default to 1024 on many distros), the startup WARN tells you exactly what to put in `/etc/security/limits.conf` or as `LimitNOFILE=` in a systemd unit. Size your limit for one fd per populated `kfcache` and `segcache` entry (up to `FCACHE_MAX` each), one per populated `bt_cache` and `bm_cache` entry (up to `FCACHE_MAX / 4` each), one temporary extra fd per concurrent kf resplit, and headroom for connections and other open files.
```
(The v1-specific "briefly holds 2 fds per shard during migration" claim and
its "~8,256 fds" figure describe `ucache_grow_shard`'s old-map+new-map
migration window, which no longer exists post-removal. The v2
`kfcache_resplit_locked` path does have its own temporary extra fd, but its
peak depends on concurrent resplits and occupancy across four independently
sized caches, so the guidance states those components instead of inventing a
single fixed peak.)

### 5c. `docs/operations/monitoring.md`

Anchor (exact existing text):
```
  "ucache":    {"used": 128, "total": 4096, "hits": 1820391, "misses": 4102},
```
Replace with:
```
  "ucache":    {"used": 0, "total": 0, "hits": 0, "misses": 0},
```

Anchor (exact existing text):
```
| `ucache.hits / (hits + misses)` | Idle on a slotcask install — kept in the export for back-compat. Slotcask reads route through kfcache + segcache, which aren't surfaced separately yet; use `bt_cache` to gauge read-cache health. |
```
No change needed to this row's content — already accurate — but confirm
during execution that the JSON example above it (just fixed in this
subtask) and this row are consistent (both describe permanent zero).

Anchor (exact existing text):
```
- `shard_db_ucache_used` / `_capacity` / `_bytes` / `_hits_total` / `_misses_total`
```
Replace with:
```
- `shard_db_ucache_used` / `_capacity` / `_bytes` / `_hits_total` / `_misses_total` (always 0 — retained for back-compat, see the `stats` section above)
```

Anchor (exact existing text):
```
| ucache miss rate | `ucache_miss_rate > 15%` for 10 min | Medium |
```
Delete this row entirely — a miss-rate alert on a permanently-zero
hit+miss denominator is meaningless (either it never fires, or it fires
spuriously on a `0/0` division depending on the alerting engine's
handling), and there is no live signal left to alert on for this metric.

### 5d. `docs/query-protocol/diagnostics.md`

Anchor (exact existing text):
```
  "ucache":    {"used": 128, "total": 4096, "bytes": 1073741824, "hits": 1820391, "misses": 4102},
```
Replace with:
```
  "ucache":    {"used": 0, "total": 0, "bytes": 0, "hits": 0, "misses": 0},
```

Anchor (exact existing text):
```
- **`ucache` hit rate** — kept in the export for back-compat; idle on a slotcask install. Reads route through `kfcache` + `segcache` (under the same `FCACHE_MAX` budget) which aren't surfaced as separate JSON fields yet. Watch `bt_cache` instead.
```
Replace with (the caches use the same configuration knob but do not share
one pooled capacity):
```
- **`ucache` hit rate** — kept in the export for back-compat; idle on a slotcask install. Reads route through `kfcache` + `segcache` (each separately sized to `FCACHE_MAX`) which aren't surfaced as separate JSON fields yet. Watch `bt_cache` instead.
```

Anchor (exact existing text):
```
shard_db_ucache_used 128
shard_db_ucache_capacity 4096
shard_db_ucache_bytes 1073741824
shard_db_ucache_hits_total 1820391
shard_db_ucache_misses_total 4102
```
Replace with:
```
shard_db_ucache_used 0
shard_db_ucache_capacity 0
shard_db_ucache_bytes 0
shard_db_ucache_hits_total 0
shard_db_ucache_misses_total 0
```

### 5e. `docs/reference/changelog.md`

Do **not** edit the existing 2026.07.3 (or any prior) entry — historical
entries describe what shipped in that release and must not be rewritten to
describe a later change. Instead, **append a new entry** for this plan's
change. This repo's changelog convention for shipped-but-not-yet-versioned
work is an `## Unreleased` section at the top (see
`docs/plans/2026-07-17-*` sibling history for precedent); use it if not
already present, or add to it if it already exists at execution time.
Confirm the actual next version number with the human before converting
`## Unreleased` to a real `## yyyy.mm.N` header — do not invent one; that
is a release-numbering decision, not a code change, and is outside this
plan's scope.

Insert immediately after the `# Changelog` / intro line (i.e. as the new,
most-recent section — anchor on the exact existing text):
```
## 2026.07.3
```
Insert **before** this line:
```markdown
## Unreleased

### Fixes

- **`get` + `fields` returned "Not found" on v2 objects** — the `get`
  dispatch branch for single-key requests with a `fields` projection still
  read through the v1 ucache shard-file layout (`build_shard_path`,
  `fcache_get_read`, `zoneA_off`/`zoneB_off`), which no v2 (slotcask)
  object has ever populated, so every such request failed even when the
  key existed. Auto-key objects hit the same bug through a second path
  (missing key normalization). Fixed with a new `cmd_get_fields` that reads
  through the v2 slotcask registry, shared by both the JSON and NQL
  dispatch paths.
- **`decode_field` stack-buffer overflow on long composite fields** — the
  `field1+field2`-style composite-field decoder concatenated into a fixed
  4096-byte stack buffer with no bounds check; a composite of two `varchar`
  fields whose combined content exceeded 4096 bytes overflowed the stack.
  Also affected criteria matching, `order_by`, and aggregate `group_by` on
  composite/unrecognized fields (not just `get`+`fields` display), since
  `decode_field` backs all of these. Fixed by switching to a
  dynamically-growing heap buffer that never truncates.
- **`count` on a non-indexed criterion silently returned a count instead of
  erroring when the object couldn't be opened** — `cmd_count`'s fallback
  path for the non-indexed case called the legacy v1 `scan_shards`/
  `count_scan_cb` path on `slotcask_registry_get` failure, returning a bare
  (likely 0) integer rather than surfacing the open failure. Now returns
  `{"error":"object not open"}`, matching `cmd_rebuild_kf`'s existing
  behavior for the same failure.

### Removed

- **Dead v1 ucache/shard-file storage engine** — this binary has only ever
  created v2 (slotcask) objects since 2026.05.5's v1→v2 migration
  requirement; the entire v1 probe-into-slot code path (`ucache_*`,
  `fcache_*`, `ShardHeader`, `zoneA_off`/`zoneB_off` and related shard-file
  layout helpers) was unreachable and has been removed. The `stats`/
  `stats-prom` `ucache` JSON fields and Prometheus metric names are
  retained, permanently reporting zero, so existing dashboards parsing
  those names don't break — see
  [monitoring.md](../operations/monitoring.md) and
  [diagnostics.md](../query-protocol/diagnostics.md).
```

### 5f. `CLAUDE.md`

Three mentions, all in the "Source layout" / "Storage model" sections.

Anchor (exact existing text):
```
- `storage.c` — xxh128 hashing, mmap, GET/INSERT/DELETE, CAS helpers, ucache, `build_idx_path`, `compute_addr`
```
Replace with:
```
- `storage.c` — xxh128 hashing, mmap, GET/INSERT/DELETE, CAS helpers, `build_idx_path`, `compute_addr`
```

Anchor (exact existing text):
```
- **I/O**: kf writes via ucache (mmap MAP_SHARED); segment writes append-only; reindex reads segments via O_DIRECT double-buffered scan.
```
Replace with:
```
- **I/O**: kf writes via the slotcask registry's per-object mmap'd kf cache (MAP_SHARED); segment writes append-only; reindex reads segments via O_DIRECT double-buffered scan.
```

Anchor (exact existing text):
```
- **Concurrency**: per-ucache-entry rwlock; per-object rwlock for schema mutations; per-btree-file rwlock (`BT_CACHE_MAX`).
```
Replace with:
```
- **Concurrency**: per-kf-shard rwlock; per-object rwlock for schema mutations; per-btree-file rwlock (`BT_CACHE_MAX`).
```

### 5h. `docs/operations/tuning.md`

Nine stale cache references in the `FCACHE_MAX` section and its
neighbors. The source of truth is `server.c`'s startup sequence:
`slotcask_init(g_fcache_cap, g_fcache_cap)` gives `kfcache` and
`segcache` separate caches with `FCACHE_MAX` entries each, while
`bt_cache_init(g_btcache_cap)` and `bm_cache_init(g_btcache_cap)` use
`g_btcache_cap = FCACHE_MAX / 4`.

Anchor (the example `db.env` comment):
```
export FCACHE_MAX=4096      # raise if shard-mmap cache hit rate < 90%; allow-list {4096, 8192, 12288, 16384}
```
Replace with:
```
export FCACHE_MAX=4096      # entries per kfcache/segcache; allow-list {4096, 8192, 12288, 16384}
```

Anchor (section heading):
```
## FCACHE_MAX — unified shard mmap cache (drives `BT_CACHE_MAX` too)
```
Replace with:
```
## FCACHE_MAX — kfcache/segcache capacity (drives `BT_CACHE_MAX` too)
```

Anchor (exact existing text):
```
Capacity (in entries, not bytes) of the shared shard mmap cache (`ucache`). Every entry is one shard's mmap region. Since 2026.05.1, `BT_CACHE_MAX` is **derived** from this as `FCACHE_MAX / 4` and is no longer configurable on its own.
```
Replace with:
```
Capacity in entries **per cache** for the separate `kfcache` and `segcache`; both are initialized to `FCACHE_MAX`. `BT_CACHE_MAX` is derived as `FCACHE_MAX / 4` and sizes `bt_cache`; `bm_cache` uses that same derived capacity. `BT_CACHE_MAX` is not independently configurable.
```

Anchor (exact existing text):
```
- Each v2 object has `splits` kf shards in `kfcache` (plus its seg files in `segcache`); each indexed field has `index_splits_for(splits)` files in `bt_cache`. Legacy v1 objects use `ucache` instead of kfcache+segcache. All four caches share the same `FCACHE_MAX` budget.
```
Replace with:
```
- Each object has `splits` kf shards in `kfcache` plus segment files in `segcache`; each B+ tree indexed field has `index_splits_for(splits)` files in `bt_cache`, while bitmap indexes use `bm_cache`. `kfcache` and `segcache` each have `FCACHE_MAX` entries; `bt_cache` and `bm_cache` each use the derived `FCACHE_MAX / 4` capacity.
```

Anchor (exact existing text):
```
- Raise if either `ucache.hits / (hits + misses) < 90%` (read-heavy) **or** `bt_cache.hits / (hits + misses) < 90%` (indexed-query heavy).
```
Replace with:
```
- Raise if `bt_cache.hits / (hits + misses) < 90%` (indexed-query heavy). There is no separate kfcache/segcache hit-rate metric; `ucache` in `stats`/`stats-prom` permanently reports zero (retained only for back-compat dashboards). For kf/seg cache pressure, watch query latency and the `slow_query` counter instead.
```

Anchor (exact existing text):
```
- Sum `objects × avg(splits)` for kfcache (or ucache, on v1) sizing; `objects × avg(indexes) × avg(index_splits_for(splits))` for bt_cache sizing.
```
Replace with:
```
- Sum `objects × avg(splits)` for kfcache sizing; `objects × avg(indexes) × avg(index_splits_for(splits))` for bt_cache sizing. Segment-cache residency additionally depends on the number of active segment files.
```

Anchor (the immediately following sizing example, which incorrectly sums
independently capped caches and promises full residency at a setting that
cannot provide it for the stated `bt_cache` working set):
```
- Bumping `FCACHE_MAX` from 4096 → 8192 doubles both caches. With 100 objects × 64 splits × 14 indexes, the per-shard layout creates 100 × 64 + 100 × 14 × 8 = 17 600 mmap entries — bump to 16384 for full residency.
```
Replace with:
```
- Bumping `FCACHE_MAX` from 4096 → 8192 doubles the capacity of `kfcache` and `segcache`, and doubles the derived capacities of `bt_cache` and `bm_cache`. Size each independently rather than summing them: for 100 objects × 64 splits × 14 B+ tree indexes, `kfcache` needs 6,400 entries while `bt_cache` needs 11,200; even `FCACHE_MAX=16384` gives `bt_cache` only 4,096 entries, so size for the hot working set rather than assuming full residency.
```

Anchor (exact existing text):
```
Leave room for the `ucache`, `bt_cache`, page cache, and working memory.
```
Replace with:
```
Leave room for the `kfcache`, `segcache`, `bt_cache`, `bm_cache`, page cache, and working memory.
```

Anchor (the later "Cold mmap" guidance):
```
- **Cold mmap.** Each kf shard is a separate mmap region in `kfcache` (sized from `FCACHE_MAX`). If your active object × shards count exceeds the cache, lookups fault in 4 KB pages from disk — ~170 slots per page, so still ~1 fault per cold lookup. Watch `kfcache.hits / (hits + misses)` in `stats`; raise `FCACHE_MAX` if < 90 %.
```
Replace with:
```
- **Cold mmap.** Each kf shard is a separate mmap region in `kfcache` (sized from `FCACHE_MAX`). If your active object × shards count exceeds the cache, lookups fault in 4 KB pages from disk — ~170 slots per page, so still ~1 fault per cold lookup. No kfcache hit-rate metric is exported; watch query latency and `slow_query`, then raise `FCACHE_MAX` when cold-cache pressure is the confirmed cause.
```

### 5i. Related live references without the literal `ucache`

The literal inventory misses these five live references. Keep them in
sync with Task 5h and the source initialization described above.

`docs/getting-started/configuration.md` anchor:
```
| `FCACHE_MAX` | `4096` | Unified shard-mmap cache capacity (entries). **Strict allow-list:** `{4096, 8192, 12288, 16384}`. Invalid values fall back to default with a warning. See [Tuning](../operations/tuning.md). |
```
Replace with:
```
| `FCACHE_MAX` | `4096` | Entry capacity for each of `kfcache` and `segcache`. **Strict allow-list:** `{4096, 8192, 12288, 16384}`. Invalid values fall back to default with a warning. Also derives `BT_CACHE_MAX = FCACHE_MAX / 4`; see [Tuning](../operations/tuning.md). |
```

`docs/getting-started/embedded-mode.md` anchor:
```
FCACHE_MAX=4096         # default 4096 — open file-handle cache size
```
Replace with:
```
FCACHE_MAX=4096         # default 4096 — entries each for kfcache/segcache; derives bt_cache/bm_cache at /4
```

`docs/reference/limits.md` anchor:
```
| `FCACHE_MAX` (shard mmap cache) | 4096 | Strict allow-list `{4096, 8192, 12288, 16384}`. |
```
Replace with:
```
| `FCACHE_MAX` (`kfcache` and `segcache`, entries per cache) | 4096 | Strict allow-list `{4096, 8192, 12288, 16384}`. |
```

Second `docs/reference/limits.md` anchor (the storage-limits table):
```
| Indexes per object | no hard cap | Each is a directory of `index_splits_for(splits)` B+ tree files. Both caches (`kfcache` + `bt_cache`; `segcache` shares the kfcache budget) cap *hot* mappings, not on-disk count. |
```
Replace with:
```
| Indexes per object | no hard cap | Each is a directory of `index_splits_for(splits)` B+ tree files. The caches cap *hot* mappings independently, not on-disk count: `kfcache` and `segcache` each use `FCACHE_MAX`; `bt_cache` and `bm_cache` each use `FCACHE_MAX / 4`. |
```

`docs/concepts/storage-model.md` anchor:
```
- **segcache** — path-keyed cache of mmap'd segment files. Same model; capacity `FCACHE_MAX/4`. Routine record writes take rdlock (each writer owns a unique reserved offset, so they don't conflict on bytes); eviction and recovery take wrlock.
```
Replace with:
```
- **segcache** — path-keyed cache of mmap'd segment files. Same model; capacity `FCACHE_MAX`. Routine record writes take rdlock (each writer owns a unique reserved offset, so they don't conflict on bytes); eviction and recovery take wrlock.
```

`docs/concepts/concurrency.md` anchor:
```
`AUTO_VACUUM`/`AUTO_RESHARD_ENABLE`'s background threads are joined (not detached) as part of this same shutdown sequence, before any cache teardown (`slotcask_shutdown`/`kfcache_shutdown`, `bt_cache_shutdown`, `fcache_shutdown`). If either thread is mid-sweep on an object when `stop` is issued, shutdown waits for that item to finish — unbounded in theory, but no worse in practice than the exclusive objlock that operation already holds against all other traffic on that object. This closes a use-after-free race: without the join, `kfcache_shutdown()` could free/destroy the kfcache array while a reshard/vacuum thread was still using it.
```
Replace with:
```
`AUTO_VACUUM`/`AUTO_RESHARD_ENABLE`'s background threads are joined (not detached) as part of this same shutdown sequence, before the live cache teardown (`slotcask_shutdown`/`kfcache_shutdown` and `bt_cache_shutdown`). If either thread is mid-sweep on an object when `stop` is issued, shutdown waits for that item to finish — unbounded in theory, but no worse in practice than the exclusive objlock that operation already holds against all other traffic on that object. This closes a use-after-free race: without the join, `kfcache_shutdown()` could free/destroy the kfcache array while a reshard/vacuum thread was still using it.
```


## Edge cases and invariants (explicit, not left to executor judgment)

- **Composite fields in `get`+`fields`** (`"fields":["a+b"]`): `decode_field`
  already handles the `+`-joined case (`query_find.c:526-543`); `cmd_get_fields`
  passes each comma-separated token straight through to `decode_field`
  unmodified, so `"fields":"a+b,c"` works exactly like it does for
  find/fetch's projection path. No special-casing needed in `cmd_get_fields`
  itself — but `decode_field`'s own composite-concatenation buffer had a
  pre-existing stack-overflow risk on large combined values, fixed in Task
  2.5 since `cmd_get_fields` becomes a new caller of it.
- **Value typing under `fields` projection is stringified, not typed**:
  `cmd_get_fields` returns e.g. `"age":"30"` (a string), not `"age":30`
  (plain `get`'s native-typed form) — confirmed live against the current
  binary that this is the pre-existing, established convention shared by
  every other `fields`-projection path (find/fetch/join/aggregate all
  stringify via `json_escape_field(decode_field(...))`). `cmd_get_fields`
  matches that convention deliberately, not plain `get`'s typing — an
  earlier draft of this section conflated the two. This is a documented,
  human-confirmed decision, not a new inconsistency this plan introduces;
  Task 1's test asserts the stringified form for exactly this reason.
- **`fields` requesting a field name that doesn't exist on the object**:
  `decode_field` returns `NULL` for an unknown field (falls through
  `typed_field_index` returning `-1` → `typed_get_field_str` on an invalid
  index); `cmd_get_fields`'s loop already does `if (!pv) continue;`, silently
  omitting it from the output dict — identical to the pre-existing (broken)
  code's behavior, not a new edge case introduced by this fix.
- **`fields` is an empty string or absent**: dispatched to the `cmd_get`
  branch (unchanged), not `cmd_get_fields` — the `if (fields && fields[0])`
  guard in `server.c` is untouched by this plan.
- **Key doesn't exist, with `fields` present**: `slotcask_get` returns
  nonzero, `cmd_get_fields` emits `{"error":"Not found"}` and returns before
  touching `fields_csv` at all — matches the "missing key with fields still
  errors" case in Task 1's test.
- **`auto_key` object + `fields`**: now normalizes the wire-form key exactly
  like the non-fields path always has (this was previously silently broken
  — an `auto_key=uuid` object combined with `get`+`fields` would fail to
  parse the dashed-UUID wire key at all, since the fields branch never
  called `auto_key_normalize`). An earlier draft of this plan left this as
  an unaddressed follow-up; add explicit coverage to Task 1's
  `test_get_fields.c` instead: create an `auto_key=uuid` object, insert
  without a key (server generates the UUID and returns it), then issue
  `get`+`fields` using that returned dashed-UUID string as `key` and assert
  the requested field comes back correctly — this directly exercises the
  normalize-then-fetch path this bullet describes, rather than leaving it
  implicit.
- **Interrupt/crash safety**: this change touches no on-disk format, no
  multi-step mutation, and no new lock ordering — `cmd_get_fields` is a
  pure read (same `slotcask_get` path `cmd_get` already uses under existing
  locking). The v1 ucache removal deletes code that was never reached
  post-fix; there is no partial-state window to reason about for either
  change.

## Execution rules (embedded, per CORE-PROCESS)

- Branch off `main`. Work stays uncommitted per this repo's standing
  execution-mode exception (`CLAUDE.md`) — Sonnet reviews the raw `git diff`
  before anything is committed.
- Do tasks in order: 1 (test, prove it fails) → 2 (fix, prove the test now
  passes) → 2.5 (harden `decode_field`, prove its own regression case fails
  pre-fix and passes post-fix) → 2.6 (harden `test_stats_prom.c` and prove it
  fails while the old ucache initialization is present) → 3 (removal,
  sub-steps 3a–3k in the order listed — later steps depend on symbols earlier
  steps delete) → 4 (full verification, including Task 2.6's green proof) → 5
  (docs, separate pass).
- Build with `SKIP_TESTS=1 ./build.sh`; test with
  `./build/bin/shard-db-test run[-all]`.
- If a quoted anchor is not found exactly as written, write
  `PLAN_NOTES.md` describing the mismatch and halt the entire execution
  run immediately — do not guess, reinterpret, or continue to any further
  task, even an unrelated one (e.g. do not skip ahead from a stuck Task 3
  sub-step to Task 4). Resuming requires the human (or the planning model,
  re-engaged) to read `PLAN_NOTES.md` and hand back either a patched or a
  fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise. In particular: Task 3d's anchors fully remove
  `count_scan_cb_flush_thread`/`count_local` alongside `CountCtx`/
  `count_scan_cb` — do not deviate from that (e.g. by trying to keep them
  as a "just in case" no-op) without stopping to ask first.
- Never weaken a test to make a failure disappear. If Task 1's test can't
  be made to pass honestly after Task 2, stop and report why.
