# Crash-injection test matrix: named crash hooks + kill/restart + invariant checks

Proposed by the human independently of the storage-durability-and-recovery
audit (`docs/plans/2026-07-16-storage-durability-and-recovery-findings.md`),
as "the highest-value new C test." This plan covers the primary ask: named
test-only crash hooks placed at the exact visibility-transition points in
the write/rebuild paths, each triggering a real `SIGKILL` mid-operation,
followed by restart and a fixed set of invariant checks. The two companion
suites the human also described (an `LD_PRELOAD` syscall-failure-injection
shim, and a model-based randomized restart fuzzer) are explicitly **out of
scope** for this plan — see "Deliberately out of scope" at the end.

Execution mode for this repo (per `CLAUDE.md`): plan only, left
**uncommitted** after execution for review; executed on a fresh branch off
`main` by a model outside the Claude family. Build with
`SKIP_TESTS=1 ./build.sh`; test with `./build/bin/shard-db-test run[-all]`.

If any quoted anchor below does not match exactly at execution time, write
`PLAN_NOTES.md` describing the mismatch and halt the entire run immediately
— do not guess, reinterpret, or continue to any other task. If you hit a
decision this plan doesn't cover, stop and ask.

**Sequencing note:** hook category 5/7 below (rebuild/reshard) doubles as
a real-SIGKILL regression test for Finding 11
(`docs/plans/2026-07-18-rebuild-legacy-crash-recovery.md`). That plan's
own regression test constructs the crash-state directories by hand (no
real crash needed) and is self-contained; this plan's `rebuild_post_walk_pre_cleanup`
case exercises the same code path via an actual `SIGKILL`, so it is
**expected to fail against pre-Finding-11 code** for the same reason
(`.rebuild_legacy_root` is silently discarded on the next rebuild attempt).
Land Finding 11's fix before or together with this plan; if this plan
lands first, its Task 4 run for that one case is expected-red until
Finding 11 lands — do not treat that as a bug in this plan's own code.

## Scope and limitation (state this in the test file's header comment too)

**SIGKILL tests in this plan prove process-crash recovery only, not
power-loss durability.** The engine relies partly on mmap'd pages and the
kernel page cache; a `SIGKILL`'d process still leaves the kernel holding
dirty pages that get written back independently of the process, so these
tests cannot distinguish "durable because committed" from "durable because
the kernel hadn't dropped the cache yet." A true power-loss claim needs a
VM or block-device-level test that discards unflushed page-cache state
(e.g. killing the VM itself, or `dm-flakey`/`blkdiscard`-style fault
injection below the page cache) — out of scope here. The existing Chaos
Mesh pod-kill harness (`chaos/`) and this plan's C `SIGKILL` tests are both
still valuable for process-failure recovery, which is the more common
production failure mode; neither should be read as a power-loss guarantee.

## Existing infrastructure this plan reuses (confirmed present, not reinvented)

- `src/test/fixtures.h` / `fixtures.c`: `TestEnv` (`db_root[256]`, `port`,
  `daemon_pid`), `test_env_start`, `test_env_start_at(TestEnv*, const char
  *db_root, int port)` (restart against existing on-disk state — exactly
  what crash tests need), `test_env_kill` (external immediate `SIGKILL`),
  `test_env_stop_keep` (graceful stop, keeps `db_root`), `test_env_stop`
  (graceful + `rm -rf`), `test_pick_port()`.
- `src/test/test_client.h`: `TestClient`, `TestClientCfg`, `tc_connect`,
  `tc_request` (returns nonzero on a broken/reset connection — already
  relied on by `test_crash_safety.c:101` for exactly the "daemon died
  mid-request" case this plan needs), `tc_close`.
- `src/test/cases/test_crash_safety.c`: canonical external-kill template
  (kill from the *test* process after the client observes the connection
  break). This plan's hooks differ by killing from *inside* the server,
  synchronously inside the request handler, at a named, precise byte-level
  boundary — the client-side handling of a broken connection is identical
  and reused verbatim.
- `KFCACHE_TEST_HOLD_MS` (`shard_db_internal.h:162,320`, `config.c:453-459`,
  consumed in `slotcask.c`): the structural precedent for a new test-only
  db.env knob — struct field + `g_*` macro + env-var parse line. This plan
  mirrors that 3-part pattern exactly, adding a fourth part (the hook fires
  a real crash instead of a sleep).
- `chaos/`: existing Chaos Mesh pod-kill + network-loss harness. Not
  touched or duplicated by this plan.

## Root cause / design rationale

No named-crash-hook mechanism exists in the codebase today. Every "does a
mid-write crash corrupt state" question currently has to be answered either
by an external `SIGKILL` racing against real traffic (nondeterministic —
see `test_crash_safety.c`'s "kill after N of M inserts" pattern) or by
hand-constructing on-disk corruption (`test_rebuild_recovery.c`'s
`corrupt_first_seg_record`). Neither lets a test assert "the process died
**exactly** between step A and step B of this specific operation" — which
is what's needed to exhaustively cover the transition points the human
identified (segment append visibility, keyfile publish, tombstoning,
index/bitmap update, rebuild renames, config rewrites, reshard).

The fix is a single, generic, named hook function checked at each
transition point of interest. A test arms one hook by name via a db.env
knob before starting the daemon; the daemon runs completely normally until
the armed request reaches that exact point, at which point it calls
`raise(SIGKILL)` — an immediate, uncatchable, uncontrolled process
termination, identical in effect to an external kill except deterministic
in *where* it lands.

### Why a single mechanism instead of one flag per site

Mirroring `KFCACHE_TEST_HOLD_MS` (one int knob, one meaning) at every one
of ~15 hook sites would mean 15 new struct fields, 15 new env-var parse
branches, and 15 macros — a maintenance burden with no benefit, since only
one hook is ever armed per test process. A single `char
crash_hook_name[64]` compared against a string literal at each site gives
the same test capability with one field, one macro, one parse branch, and
one helper function.

### Why not gated behind `#ifdef TEST_BUILD`

The existing `KFCACHE_TEST_HOLD_MS`/`WARMUP_TEST_DELAY_MS` hooks are not
`#ifdef`-gated either — they're always compiled in, always checked (a
single relaxed int/byte compare), and are inert (`g_crash_hook_name[0] ==
'\0'`) unless a test explicitly sets the db.env knob. This plan follows
that same precedent for consistency and because it avoids a second build
configuration that would otherwise need its own CI coverage.

## Part 1 — the crash-hook mechanism

### 1a. New struct field

**File:** `src/db/shard_db_internal.h`

Locate this exact existing text:

```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
```

Replace with:

```c
    int kfcache_test_hold_ms; /* test-only; 0 = off in production */
    int warmup_test_delay_ms; /* test-only; 0 = off in production */
    char crash_hook_name[64]; /* test-only; empty = off in production.
                                  When non-empty, crash_hook_fire() raises
                                  SIGKILL the first time the named hook site
                                  is reached. See util.c. */
```

Locate this exact existing text:

```c
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
```

Replace with:

```c
#define g_kfcache_test_hold_ms      (g_db->kfcache_test_hold_ms)
#define g_warmup_test_delay_ms      (g_db->warmup_test_delay_ms)
#define g_crash_hook_name           (g_db->crash_hook_name)
```

`ShardDb` is always `calloc`'d (`src/db/embedded.c:65`,
`shard_db_open_internal`, the single init path shared by daemon and
embedded modes), so `crash_hook_name` defaults to an empty string — no
separate default-init line needed.

### 1b. db.env parse

**File:** `src/db/config.c`

Locate this exact existing text:

```c
        } else if (strncmp(p, "KFCACHE_TEST_HOLD_MS=", 21) == 0) {
            /* Test-only knob (widens kfcache_invalidate_prefix's hold window
               deterministically for the shutdown-race regression test). Not
               a documented production setting — do not add to
               configuration.md. */
            int n = atoi(p + 21);
            if (n >= 0 && g_db) g_kfcache_test_hold_ms = n;
```

Replace with:

```c
        } else if (strncmp(p, "CRASH_HOOK_NAME=", 16) == 0) {
            /* Test-only knob (names a crash_hook_fire() site to raise
               SIGKILL at, for the crash-injection test matrix). Not a
               documented production setting — do not add to
               configuration.md. */
            if (g_db) {
                char *v = p + 16;
                char *e = v + strlen(v) - 1;
                while (e >= v && (*e == '\n' || *e == '\r' || *e == ' ')) *e-- = '\0';
                snprintf(g_crash_hook_name, sizeof(g_crash_hook_name), "%s", v);
            }
        } else if (strncmp(p, "KFCACHE_TEST_HOLD_MS=", 21) == 0) {
            /* Test-only knob (widens kfcache_invalidate_prefix's hold window
               deterministically for the shutdown-race regression test). Not
               a documented production setting — do not add to
               configuration.md. */
            int n = atoi(p + 21);
            if (n >= 0 && g_db) g_kfcache_test_hold_ms = n;
```

### 1c. The hook function

**File:** `src/db/util.c` — add near the other small always-available
helpers (any location in the file is fine; it has no dependency on
anything declared later in the file).

```c
/* crash_hook_fire — test-only named crash-injection point. No-op unless
   CRASH_HOOK_NAME in db.env names this exact hook. When it matches, logs
   once and raises SIGKILL immediately: uncatchable, no cleanup, no atexit,
   identical to a real crash landing at this exact line. g_crash_hook_name[0]
   short-circuits to a single byte compare in the overwhelmingly common
   (disabled) case, matching the existing g_kfcache_test_hold_ms > 0 guard
   pattern already used in these same hot paths. */
void crash_hook_fire(const char *name) {
    if (!g_db || !g_crash_hook_name[0]) return;
    if (strcmp(g_crash_hook_name, name) != 0) return;
    LOG_WARN(LOG_SUB_SERVER, "CRASH_HOOK firing at '%s' — simulating crash via SIGKILL", name);
    raise(SIGKILL);
}
```

**File:** `src/db/types.h` — declare it alongside the other cross-TU
util.c declarations. Locate the existing forward declaration block for
util.c functions (e.g. near `b64_encode`/`b64_decode`/`valid_filename`) and
add:

```c
void crash_hook_fire(const char *name);
```

(Executor: use whatever exact neighboring line util.c's other declarations
sit next to in `types.h` — this is an additive single-line insertion with
no anchor risk since it doesn't replace existing text.)

## Part 2 — hook placement (7 categories, ~15 named sites)

All 15 hook names below are distinct string literals; the matrix test in
Part 3 arms exactly one at a time via `CRASH_HOOK_NAME=<name>` in db.env.

### Category 1 — segment append pre/post visibility

**File:** `src/db/slotcask.c`, inside `seg_record_emit` — the single
low-level primitive every insert/update/rebuild path funnels through to
write a segment record. Locate this exact existing text:

```c
static inline void seg_record_emit(uint8_t *dst, int slot_size,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen) {
    memcpy(dst, hash, 16);
    uint16_t k16 = (uint16_t)klen;
    memcpy(dst + 16, &k16, 2);
    /* Flag stays 0 until payload is fully in place. Use atomic_store with
       relaxed ordering for the initial 0 — readers that see 0 simply skip
       the record (no acquire needed). */
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELAXED);
    dst[19] = 0;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
    memcpy(dst + 24 + klen, value, vlen);
    size_t used = 24 + klen + vlen;
    if (used < (size_t)slot_size) {
        memset(dst + used, 0, (size_t)slot_size - used);
    }
    /* Release-store on the flag commits all the payload writes above.
       Any reader doing acquire-load on flag==1 (seg_rec_live_with_hash)
       will see the full hash/key/value as a coherent snapshot. */
    __atomic_store_n(&dst[18], 1, __ATOMIC_RELEASE);
}
```

Replace with:

```c
static inline void seg_record_emit(uint8_t *dst, int slot_size,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen) {
    memcpy(dst, hash, 16);
    uint16_t k16 = (uint16_t)klen;
    memcpy(dst + 16, &k16, 2);
    /* Flag stays 0 until payload is fully in place. Use atomic_store with
       relaxed ordering for the initial 0 — readers that see 0 simply skip
       the record (no acquire needed). */
    __atomic_store_n(&dst[18], 0, __ATOMIC_RELAXED);
    dst[19] = 0;
    uint32_t v32 = (uint32_t)vlen;
    memcpy(dst + 20, &v32, 4);
    memcpy(dst + 24, key, klen);
    memcpy(dst + 24 + klen, value, vlen);
    size_t used = 24 + klen + vlen;
    if (used < (size_t)slot_size) {
        memset(dst + used, 0, (size_t)slot_size - used);
    }
    crash_hook_fire("seg_pre_publish"); /* payload written, flag still 0 */
    /* Release-store on the flag commits all the payload writes above.
       Any reader doing acquire-load on flag==1 (seg_rec_live_with_hash)
       will see the full hash/key/value as a coherent snapshot. */
    __atomic_store_n(&dst[18], 1, __ATOMIC_RELEASE);
    crash_hook_fire("seg_post_publish"); /* flag=1 committed, kf not yet updated by caller */
}
```

- `seg_pre_publish`: kill leaves the slot at flag=0 forever — invariant:
  record must be absent from `count`/`find`/`get` after restart (never
  written, from the reader's point of view).
- `seg_post_publish`: kill leaves a segment record with flag=1 that no kf
  entry points to yet (insert path) or that the old kf entry still points
  past (update path). Invariant: this orphaned record must not surface as
  a phantom key via any read path, and a subsequent `vacuum`/rebuild must
  not resurrect it — this is the "no phantom keys" check from the human's
  original invariant list, applied at its most fundamental site.

### Category 2 — keyfile pointer/flag publication + count update

**File:** `src/db/slotcask.c`, inside `kf_put_new` (new-key insert path).
Locate this exact existing text:

```c
            memcpy(t->hash, hash, 16);
            t->stream_id = stream_id;
            t->file_id = file_id;
            t->offset = offset;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            t->flag = 1;
            if (hdr) {
                if (reused_tomb) hdr->deleted--;   /* tombstone reclaimed */
                else             hdr->total++;    /* fresh slot occupied */
            }
            (*used_delta)++;
```

Replace with:

```c
            memcpy(t->hash, hash, 16);
            t->stream_id = stream_id;
            t->file_id = file_id;
            t->offset = offset;
            __atomic_thread_fence(__ATOMIC_RELEASE);
            t->flag = 1;
            crash_hook_fire("kf_publish_pre_count"); /* kf slot live, header count not yet bumped */
            if (hdr) {
                if (reused_tomb) hdr->deleted--;   /* tombstone reclaimed */
                else             hdr->total++;    /* fresh slot occupied */
            }
            crash_hook_fire("kf_publish_post_count"); /* kf slot live + header count consistent */
            (*used_delta)++;
```

- `kf_publish_pre_count`: kill here leaves a live kf entry (findable via
  `get`) but the per-shard header `total`/`deleted` counters not yet
  reflecting it. Invariant: `count` (which sums kf headers per
  `CLAUDE.md`'s "Record counts (v2)") may under-count by exactly this one
  record relative to a live-record walk (`keys`/`fetch` with a large
  limit) — the two must not permanently diverge; a `recount`/reopen must
  reconcile them. This is the "count equals live-record walk" invariant.
- `kf_publish_post_count`: fully committed; must behave exactly like a
  normal successful insert after restart.

**File:** `src/db/slotcask.c`, inside `kf_repoint_at_slot` (existing-key
update path — single atomic 8-byte store combining flag+pointer, added to
prove that claimed atomicity under a real kill, not just by inspection).
Locate this exact existing text:

```c
    combo.parts.flag = 1;
    combo.parts.stream_id = new_stream_id;
    combo.parts.file_id = new_file_id;
    combo.parts.offset = new_offset;
    __atomic_store_n((uint64_t *)((uint8_t *)e + 16), combo.u64,
                     __ATOMIC_RELEASE);
}
```

Replace with:

```c
    combo.parts.flag = 1;
    combo.parts.stream_id = new_stream_id;
    combo.parts.file_id = new_file_id;
    combo.parts.offset = new_offset;
    __atomic_store_n((uint64_t *)((uint8_t *)e + 16), combo.u64,
                     __ATOMIC_RELEASE);
    crash_hook_fire("kf_repoint_committed"); /* single atomic store already landed or didn't */
}
```

- `kf_repoint_committed`: kill immediately after this store. Invariant:
  the kf entry must point either fully at the old location or fully at the
  new one — never a torn mix (proves the union-packed atomic store claim
  in the surrounding comment under a real kill, not just by code
  inspection).

### Category 3 — update/delete tombstoning

**File:** `src/db/slotcask.c`, inside `kf_tombstone_at_slot`. Locate this
exact existing text:

```c
static inline void kf_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot) {
    kh->map[slot].flag = 2;
    if (kh->hdr) kh->hdr->deleted++;
}
```

Replace with:

```c
static inline void kf_tombstone_at_slot(SlotcaskKfHandle *kh, size_t slot) {
    kh->map[slot].flag = 2;
    crash_hook_fire("kf_tombstone_pre_count"); /* slot tombstoned, header deleted-count not yet bumped */
    if (kh->hdr) kh->hdr->deleted++;
}
```

- `kf_tombstone_pre_count`: this is a genuine non-atomic 2-step window
  (flag write, then a separate counter increment) — kill here leaves the
  key correctly gone from reads (flag=2 makes `get`/`find` treat it as
  deleted) but the header's `deleted` counter one short. Invariant: `live
  = total − deleted` (per `CLAUDE.md`) may be off by one in the
  *non-deleted* direction (looks like one extra live record) until the
  next `recount`; must self-correct and must never make a genuinely
  deleted key readable again.

### Category 4 — index/bitmap update

**File:** `src/db/index.c`, inside `update_idx_fn`. Locate this exact
existing text:

```c
    switch (a->type) {
        case IT_BTREE:
            if (a->old_key)
                delete_index_entry(a->db_root, a->object, a->field, a->splits,
                                   a->old_key, a->old_len, a->hash);
            if (a->new_key)
                write_index_entry(a->db_root, a->object, a->field, a->splits,
                                  a->new_key, a->new_len, a->hash);
            break;
```

Replace with:

```c
    switch (a->type) {
        case IT_BTREE:
            if (a->old_key)
                delete_index_entry(a->db_root, a->object, a->field, a->splits,
                                   a->old_key, a->old_len, a->hash);
            crash_hook_fire("idx_btree_between_delete_insert"); /* old entry gone, new not yet written (update only) */
            if (a->new_key)
                write_index_entry(a->db_root, a->object, a->field, a->splits,
                                  a->new_key, a->new_len, a->hash);
            break;
```

- `idx_btree_between_delete_insert`: on an update (both `old_key` and
  `new_key` set), kill here leaves the record's old indexed value's btree
  entry deleted and the new value's entry not yet written. Invariant: an
  indexed `find` on either the old or the new value must not return this
  key during the window (matches "no stale index entries" — but note this
  is a narrow, pre-existing, already-documented window, not a new bug;
  the point of this hook is proving the record's *own* data (segment/kf)
  is never corrupted by it, and that the index self-heals via the next
  write or a `reindex`).

**File:** `src/db/index.c`, inside `update_idx_fn`'s `IT_BITMAP` case.
Locate this exact existing text:

```c
        case IT_BITMAP:
            /* The slow-path insert case can call us with kf_slot=0 and
               kf_shard=0 unset (slotcask determined the slot AFTER
               pre_commit). Detect by the absence of a publish: if the
               caller didn't write to the out-params, skip the bitmap
               update. Reindex will catch it. We can't reliably tell
               "unset" from "shard 0 + slot 0", so the field-level
               convention is: callers that don't have the slot leave
               type=IT_BTREE and rely on the btree path. Anyone setting
               type=IT_BITMAP guarantees they've populated shard+slot. */
            a->out_error = bitmap_update(a->db_root, a->object, a->field,
                                         a->kf_shard, a->splits,
                                         a->kf_slot, a->bm_max_values,
                                         a->new_key, a->new_len,
                                         a->old_key, a->old_len);
            break;
```

Replace with:

```c
        case IT_BITMAP:
            /* The slow-path insert case can call us with kf_slot=0 and
               kf_shard=0 unset (slotcask determined the slot AFTER
               pre_commit). Detect by the absence of a publish: if the
               caller didn't write to the out-params, skip the bitmap
               update. Reindex will catch it. We can't reliably tell
               "unset" from "shard 0 + slot 0", so the field-level
               convention is: callers that don't have the slot leave
               type=IT_BTREE and rely on the btree path. Anyone setting
               type=IT_BITMAP guarantees they've populated shard+slot. */
            crash_hook_fire("idx_bitmap_pre_update"); /* record's own data already committed; bitmap file not yet touched */
            a->out_error = bitmap_update(a->db_root, a->object, a->field,
                                         a->kf_shard, a->splits,
                                         a->kf_slot, a->bm_max_values,
                                         a->new_key, a->new_len,
                                         a->old_key, a->old_len);
            crash_hook_fire("idx_bitmap_post_update"); /* bitmap file update returned */
            break;
```

- `idx_bitmap_pre_update` / `idx_bitmap_post_update`: bracket the entire
  bitmap mutation. Invariant: same as above — the record's own segment/kf
  state must be unaffected either way; a `reindex` must converge the
  bitmap regardless of which side of the call the kill landed on.

### Category 5 / 7 — vacuum/compaction/rebuild renames, and reshard/auto-reshard

Reshard and auto-reshard have no separate code path from manual
`vacuum`/rebuild — both funnel into the same `rebuild_object_v2`
(`src/db/query_find.c:1026`, confirmed via `grep -rn "rebuild_object_v2("
src/db` — exactly two callers, `query_schema.c:616` for
edit-field/remove-field and the `rebuild_object` wrapper at
`query_find.c:1351` for add-field/vacuum/manual-reshard/auto-reshard, per
Finding 11's fix plan). One set of hooks in this function covers both
human-specified categories.

**File:** `src/db/query_find.c`. Locate this exact existing text:

```c
    /* Clean any stale data.legacy from a prior crashed rebuild. */
    rmrf(legacy_dir);

    /* Drop the cached slotcask handle so the rename below doesn't tug
       on live mmap regions. The next slotcask_registry_get will
       re-open fresh against the new data/ that we'll create. */
    slotcask_registry_invalidate(db_root, object);

    /* Atomic rename: data/ → data.legacy/. The new slotcask_open below
       will re-create data/ from scratch with the new schema. */
    if (rename(data_dir, legacy_dir) != 0) {
```

Replace with:

```c
    /* Clean any stale data.legacy from a prior crashed rebuild. */
    rmrf(legacy_dir);

    /* Drop the cached slotcask handle so the rename below doesn't tug
       on live mmap regions. The next slotcask_registry_get will
       re-open fresh against the new data/ that we'll create. */
    slotcask_registry_invalidate(db_root, object);

    crash_hook_fire("rebuild_pre_stage1_rename"); /* data/ still live, no staging done yet */
    /* Atomic rename: data/ → data.legacy/. The new slotcask_open below
       will re-create data/ from scratch with the new schema. */
    if (rename(data_dir, legacy_dir) != 0) {
```

Locate this exact existing text (immediately follows, inside the same
function — the second rename):

```c
    char legacy_root[PATH_MAX];
    snprintf(legacy_root, sizeof(legacy_root), "%s/.rebuild_legacy_root", obj_dir);
    rmrf(legacy_root);
    mkdirp(legacy_root);
    char legacy_data_under_root[PATH_MAX];
    snprintf(legacy_data_under_root, sizeof(legacy_data_under_root),
             "%s/data", legacy_root);
    if (rename(legacy_dir, legacy_data_under_root) != 0) {
```

Replace with:

```c
    char legacy_root[PATH_MAX];
    snprintf(legacy_root, sizeof(legacy_root), "%s/.rebuild_legacy_root", obj_dir);
    rmrf(legacy_root);
    mkdirp(legacy_root);
    char legacy_data_under_root[PATH_MAX];
    snprintf(legacy_data_under_root, sizeof(legacy_data_under_root),
             "%s/data", legacy_root);
    crash_hook_fire("rebuild_post_stage1_rename"); /* data.legacy/ exists, data/ absent */
    if (rename(legacy_dir, legacy_data_under_root) != 0) {
```

Locate this exact existing text (the walk-success point — this is
Finding 11's own fix window; requires that plan's `rmrf(legacy_root)`
reorder to already be in place, since this hook fires exactly where that
fix's cleanup now runs):

```c
    /* Walk succeeded — data/ now holds every live record under the new
       schema. Drop .rebuild_legacy_root now, not at the end of this
       function: everything from here on (fields.conf/schema.conf swap,
       cache invalidation, reindex) is best-effort bookkeeping that a
       crash must not be allowed to make ambiguous. Once this line runs,
       ".rebuild_legacy_root exists" is a reliable signal to startup
       recovery that the walk never completed (Finding 11 fix). */
    rmrf(legacy_root);
```

Replace with:

```c
    /* Walk succeeded — data/ now holds every live record under the new
       schema. Drop .rebuild_legacy_root now, not at the end of this
       function: everything from here on (fields.conf/schema.conf swap,
       cache invalidation, reindex) is best-effort bookkeeping that a
       crash must not be allowed to make ambiguous. Once this line runs,
       ".rebuild_legacy_root exists" is a reliable signal to startup
       recovery that the walk never completed (Finding 11 fix). */
    crash_hook_fire("rebuild_post_walk_pre_cleanup"); /* new data/ complete; .rebuild_legacy_root not yet dropped */
    rmrf(legacy_root);
```

- `rebuild_pre_stage1_rename`: kill before any staging — object must be
  completely untouched after restart (as if the rebuild never started).
- `rebuild_post_stage1_rename`: kill with `data.legacy/` staged, `data/`
  gone, second rename not yet attempted. Exercises Finding 11's
  `recover_rebuild_legacy`'s `has_legacy_dir` branch via a real crash.
- `rebuild_post_walk_pre_cleanup`: kill with the new `data/` fully
  populated and `.rebuild_legacy_root` not yet dropped. Exercises Finding
  11's `has_legacy_data` branch via a real crash — this is the direct
  real-SIGKILL analogue of that plan's hand-constructed Stage 2 test.
  Invariant for all three: post-restart record count equals the pre-rebuild
  live count (no data loss, no double-count), and no stray `data.legacy`/
  `.rebuild_legacy_root` survives a second successful rebuild attempt.

### Category 6 — schema/index config rewrites

Indexes have no separate config file to rewrite (per `CLAUDE.md`, an
index is just a directory of btree/bitmap files under `<obj>/indexes/<field>/`
— confirmed via `grep -rn "schema.conf" src/db/index.c`, which shows only
`cmd_reindex` *reading* `schema.conf` to enumerate objects, never writing
an index list into it). The only config-file rewrites in this category are
`fields.conf` (add-field/remove-field/edit-field/rename-field) and
`schema.conf` (splits/streams change), both already staged inside
`rebuild_object_v2` right after the walk. One set of hooks here.

**File:** `src/db/query_find.c`. Locate this exact existing text:

```c
        if (rename(fpath, fpath_old) != 0)
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed", fpath, fpath_old);
        if (rename(fpath_new, fpath) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed — restoring", fpath_new, fpath);
            (void)rename(fpath_old, fpath);
            OUT("{\"error\":\"Failed to swap fields.conf\"}\n");
            return 1;
        }
        unlink(fpath_old);
    }

    int streams_changed = (new_sch->streams != old_sch->streams);
    if (splits_changed || streams_changed)
        update_schema_conf_splits_streams(db_root, object,
                                           splits_changed  ? new_sch->splits  : 0,
                                           streams_changed ? new_sch->streams : 0);
```

Replace with:

```c
        crash_hook_fire("fields_conf_pre_swap"); /* fields.conf.new staged, fields.conf still the old version */
        if (rename(fpath, fpath_old) != 0)
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed", fpath, fpath_old);
        if (rename(fpath_new, fpath) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed — restoring", fpath_new, fpath);
            (void)rename(fpath_old, fpath);
            OUT("{\"error\":\"Failed to swap fields.conf\"}\n");
            return 1;
        }
        unlink(fpath_old);
        crash_hook_fire("fields_conf_post_swap"); /* fields.conf now the new version */
    }

    int streams_changed = (new_sch->streams != old_sch->streams);
    if (splits_changed || streams_changed) {
        crash_hook_fire("schema_conf_pre_update"); /* data/ already rebuilt; schema.conf still describes old splits/streams */
        update_schema_conf_splits_streams(db_root, object,
                                           splits_changed  ? new_sch->splits  : 0,
                                           streams_changed ? new_sch->streams : 0);
        crash_hook_fire("schema_conf_post_update"); /* schema.conf now matches the rebuilt data/ */
    }
```

- `fields_conf_pre_swap` / `fields_conf_post_swap`: kill mid-rename — the
  existing `rename(fpath, fpath_old)` / `rename(fpath_new, fpath)` /
  restore-on-failure sequence is the *existing* crash-safety mechanism for
  this file (already covered generically by `recover_one_object`'s
  `fields.conf.new`/`fields.conf.old` sweep in `objlock.c`); these hooks
  prove that sweep actually works under a real kill, not just by
  inspection. Invariant: after restart, `fields.conf` must be readable and
  match either the old or the new field layout — never a partial/missing
  file — and the object's data (already rebuilt under the new schema at
  this point) must remain queryable.
- `schema_conf_pre_update` / `schema_conf_post_update`: kill with `data/`
  already on the new splits/streams but `schema.conf` still describing the
  old ones (or vice versa after). Invariant: this is the one case where a
  transient mismatch between `schema.conf` and on-disk `data/` layout is
  possible — the test must confirm the object is still fully queryable
  after restart (the daemon reads `data/`'s actual layout for I/O, not a
  cached assumption from `schema.conf`, so a stale `splits` value here
  should be self-correcting on the next `vacuum`/reindex, not
  data-destroying). If this invariant fails, treat it as a new finding,
  not a bug in this test.

## Part 3 — the test file

**File (new):** `src/test/cases/test_crash_hook_matrix.c`

Table-driven: one `CrashHookCase` per hook name, one generic runner
function reused by every case. Each case creates its own uniquely-named
object (`chm_<hookname>`, sanitized) so cases don't interfere if run with
`--jobs 1` (sequential, shared process-local state per `CLAUDE.md`'s test
harness description) — though every case here uses a fresh per-test daemon
(`test_env_start_at`), consistent with `test_rebuild_recovery.c` and
`test_crash_safety.c`, not the process-local shared-`ShardDb` path, so
cross-case interference is not actually possible; the unique object names
are kept anyway for clearer failure output.

```c
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>

/* Writes db.env with CRASH_HOOK_NAME=<hook_name> before starting the
   daemon. Mirrors test_env_start_at's normal db.env but adds the one
   test-only line; fixtures.c's test_env_start_at always (re)writes
   db.env from its own template on every call, so this helper duplicates
   just enough of that to add the extra line — confirmed necessary because
   fixtures.h exposes no "extra env lines" parameter on test_env_start_at. */
static int start_with_hook(TestEnv *env, const char *db_root, int port,
                            const char *hook_name) {
    if (test_env_start_at(env, db_root, port) != 0) return -1;
    char envpath[512];
    snprintf(envpath, sizeof(envpath), "%s/db.env", db_root);
    FILE *f = fopen(envpath, "a");
    if (!f) return -1;
    fprintf(f, "CRASH_HOOK_NAME=%s\n", hook_name);
    fclose(f);
    /* db.env is read at process start, not live-reloaded — restart once
       more so the appended line takes effect. test_env_stop_keep leaves
       db_root intact for the immediate restart. */
    test_env_stop_keep(env);
    return test_env_start_at(env, db_root, port);
}

typedef struct {
    const char *hook_name;
    const char *category;
} CrashHookCase;

static const CrashHookCase CASES[] = {
    { "seg_pre_publish",                  "segment append" },
    { "seg_post_publish",                 "segment append" },
    { "kf_publish_pre_count",             "keyfile publish" },
    { "kf_publish_post_count",            "keyfile publish" },
    { "kf_repoint_committed",             "keyfile publish" },
    { "kf_tombstone_pre_count",           "tombstoning" },
    { "idx_btree_between_delete_insert",  "index update" },
    { "idx_bitmap_pre_update",            "index update" },
    { "idx_bitmap_post_update",           "index update" },
    { "rebuild_pre_stage1_rename",        "rebuild/reshard" },
    { "rebuild_post_stage1_rename",       "rebuild/reshard" },
    { "rebuild_post_walk_pre_cleanup",    "rebuild/reshard" },
    { "fields_conf_pre_swap",             "schema config" },
    { "fields_conf_post_swap",            "schema config" },
    { "schema_conf_pre_update",           "schema config" },
    { "schema_conf_post_update",          "schema config" },
};
#define N_CASES ((int)(sizeof(CASES) / sizeof(CASES[0])))

/* Runs one hook case end to end. Returns 0 on pass (all invariants held),
   1 on any assertion failure. Uses t_ctx->failed as the shared counter,
   same convention as every other case file. */
static int run_one_case(const CrashHookCase *c) {
    char base[256], db_root[256];
    snprintf(base, sizeof(base), "/tmp/shard-db-chm-%s-%d", c->hook_name, (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    tu_run_cmd("mkdir -p %s", db_root);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Phase 1: plain daemon, no hook armed — build a baseline object with
       committed data, so every hook case has real records to protect. */
    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "phase1 daemon start"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "phase1 connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"chm\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"title:varchar:64\"],"
        "\"indexes\":[\"score\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create object");
    free(resp); resp = NULL;

    for (int i = 0; i < 40; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"chm\","
            "\"key\":\"item%04d\",\"value\":{\"score\":%d,\"title\":\"t%d\"}}",
            i, i, i);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "baseline insert OK");
        free(resp); resp = NULL;
    }

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"chm\"}", &resp);
    int baseline = tu_parse_count(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(baseline, 40, "baseline count before hook phase");
    tc_close(tc);
    test_env_stop_keep(&env);

    /* Phase 2: restart with the named hook armed, then send the one
       operation that reaches it. Different categories need different
       trigger requests; rather than special-case each, send an update to
       an existing key (exercises delete+insert on segment/kf/index paths)
       and, for the rebuild/reshard/schema-config hooks, a splits bump
       (the only request that reaches rebuild_object_v2). */
    if (start_with_hook(&env, db_root, port, c->hook_name) != 0) {
        ASSERT_TRUE(0, "phase2 daemon start with hook armed");
        tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "phase2 connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    int is_rebuild_hook = (strncmp(c->hook_name, "rebuild_", 8) == 0) ||
                          (strncmp(c->hook_name, "fields_conf_", 12) == 0) ||
                          (strncmp(c->hook_name, "schema_conf_", 12) == 0);
    if (is_rebuild_hook) {
        tc_request(tc,
            "{\"mode\":\"vacuum\",\"dir\":\"default\",\"object\":\"chm\",\"splits\":16}",
            &resp);
    } else {
        tc_request(tc,
            "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"chm\","
            "\"key\":\"item0000\",\"value\":{\"score\":9999,\"title\":\"updated\"}}",
            &resp);
    }
    free(resp); resp = NULL;

    /* The armed request either got the SIGKILL mid-flight (connection
       reset, tc_request already returned above) or, if this hook site
       was never reached by this particular request shape, the daemon is
       still alive — wait briefly and reap either way. */
    int status = 0;
    int reaped = 0;
    for (int i = 0; i < 100; i++) {  /* up to ~5s */
        pid_t r = waitpid(env.daemon_pid, &status, WNOHANG);
        if (r == env.daemon_pid) { reaped = 1; break; }
        usleep(50000);
    }
    tc_close(tc);

    ASSERT_TRUE(reaped, "daemon terminated (hook fired)");
    if (reaped) {
        ASSERT_TRUE(WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL,
                    "daemon died from SIGKILL, not a normal exit/other signal");
    }
    env.daemon_pid = -1; /* already reaped; test_env_stop must not wait/kill again */

    /* Phase 3: restart clean (no hook armed) and check invariants. */
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "phase3 restart"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "phase3 connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Invariant A: count is well-formed (no negative/garbage) and never
       exceeds baseline+1 (the one update-in-flight) or drops below
       baseline-… — for rebuild hooks the object must still exist with all
       baseline records; for per-record hooks item0000 may or may not have
       taken the update, everything else must be untouched. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"chm\"}", &resp);
    int count_after = tu_parse_count(resp);
    free(resp); resp = NULL;
    ASSERT_TRUE(count_after >= baseline - 1 && count_after <= baseline,
                "post-crash count matches baseline (no loss, no duplication)");

    /* Invariant B: count equals a live-record walk (fetch with a large
       limit, count returned rows) — catches the header-count-vs-kf-slot
       divergence classes (categories 2 and 3). */
    tc_request(tc,
        "{\"mode\":\"fetch\",\"dir\":\"default\",\"object\":\"chm\",\"limit\":1000,\"fields\":[]}",
        &resp);
    int walk_count = tu_count_json_array_items(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(walk_count, count_after, "count matches live-record walk");

    /* Invariant C: indexed find matches what a full scan would show —
       every record with score>=0 must be findable via the score index
       (catches category 4's stale/missing index entries). */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"chm\","
        "\"criteria\":{\"score\":{\"gte\":0}},\"limit\":1000}",
        &resp);
    int indexed_count = tu_count_json_array_items(resp);
    free(resp); resp = NULL;
    ASSERT_EQ_INT(indexed_count, count_after,
                  "indexed find count matches full record count (no stale/missing index entries)");

    /* Invariant D: no stray staging artifacts survive (categories 5/6/7). */
    char obj_dir[512], legacy_dir[512], legacy_root[512];
    snprintf(obj_dir, sizeof(obj_dir), "%s/default/chm", db_root);
    snprintf(legacy_dir, sizeof(legacy_dir), "%s/data.legacy", obj_dir);
    snprintf(legacy_root, sizeof(legacy_root), "%s/.rebuild_legacy_root", obj_dir);
    ASSERT_TRUE(access(legacy_dir, F_OK) != 0, "no stray data.legacy after restart");
    ASSERT_TRUE(access(legacy_root, F_OK) != 0, "no stray .rebuild_legacy_root after restart");

    tc_close(tc);
    test_env_stop(&env);
    tu_run_cmd("rm -rf %s", base);
    return t_ctx->failed > 0 ? 1 : 0;
}

static int test_crash_hook_matrix_run(void) {
    int any_failed = 0;
    for (int i = 0; i < N_CASES; i++) {
        int rc = run_one_case(&CASES[i]);
        if (rc != 0) {
            fprintf(stderr, "[crash-hook-matrix] case '%s' (%s) FAILED\n",
                    CASES[i].hook_name, CASES[i].category);
            any_failed = 1;
        }
    }
    return any_failed;
}

TEST_REGISTER("test-crash-hook-matrix", test_crash_hook_matrix_run)
```

### New test-only utility: `tu_count_json_array_items`

`fixtures.h`/`fixtures.c` does not currently expose a "count elements in a
JSON array response" helper (`tu_parse_count` only parses a bare integer
response, used for `count`/`orphaned`, not for arrays returned by
`find`/`fetch`). Add one.

**File:** `src/test/fixtures.h` — locate the existing declaration of
`tu_parse_count` (exact neighboring line to be confirmed by the executor
via `grep -n "tu_parse_count" src/test/fixtures.h`) and add immediately
after it:

```c
int tu_count_json_array_items(const char *json_array_resp);
```

**File:** `src/test/fixtures.c` — add the implementation near
`tu_parse_count`'s own implementation:

```c
/* Counts top-level JSON array elements in a find/fetch array response
   (default format, e.g. [{"key":...},{"key":...}]). Bracket/brace-depth
   scan, ignores commas inside nested objects/strings. Returns -1 if
   json_array_resp is NULL or doesn't start with '['. */
int tu_count_json_array_items(const char *json_array_resp) {
    if (!json_array_resp) return -1;
    const char *p = json_array_resp;
    while (*p == ' ' || *p == '\t' || *p == '\n') p++;
    if (*p != '[') return -1;
    p++;
    int depth = 0;
    int in_string = 0;
    int count = 0;
    int seen_any_char_at_depth0 = 0;
    for (; *p; p++) {
        char c = *p;
        if (in_string) {
            if (c == '\\') { p++; continue; }
            if (c == '"') in_string = 0;
            continue;
        }
        if (c == '"') { in_string = 1; continue; }
        if (c == '[' || c == '{') { depth++; if (depth == 1) { count++; } continue; }
        if (c == ']' || c == '}') { if (depth == 0) break; depth--; continue; }
        if (depth == 0 && c != ',' && c != ' ' && c != '\t' && c != '\n') {
            seen_any_char_at_depth0 = 1;
        }
    }
    (void)seen_any_char_at_depth0;
    return count;
}
```

## build.sh registration

Locate this exact existing text:

```
    src/test/cases/test_crash_safety.c \
```

(Executor: confirm the exact surrounding lines via `grep -n
"test_crash_safety.c" build.sh` first, since this plan does not have that
file's neighbors memorized precisely — insert the new line immediately
after it in the same list, matching the existing one-file-per-line, `\`
continuation style used throughout that list.)

Add:

```
    src/test/cases/test_crash_hook_matrix.c \
```

## Edge cases

- **A hook site is never reached by the matrix's chosen trigger request.**
  The per-record hooks (categories 1-4) are triggered by a single `update`
  on an existing key — this reaches `seg_record_emit` (insert of the new
  version), `kf_repoint_at_slot` (existing-key repoint) and
  `kf_tombstone_at_slot` (old slot tombstoned) but **not** `kf_put_new`
  (new-key path only). `kf_publish_pre_count`/`kf_publish_post_count` need
  an `insert` of a brand-new key instead. The runner above uses `update`
  uniformly for simplicity; if `run_one_case`'s reap-loop times out with
  `reaped == 0` for the two `kf_publish_*` cases, that is expected given
  the trigger mismatch, not a real failure — the executor must special-case
  those two names to send an `insert` of a new key (e.g.
  `item_new_<hookname>`) instead of the `update`. State this as a required
  fix-during-execution, not a silent skip: the plan's own table only
  documents *which* request reaches which hook; getting the dispatch
  exactly right per-case is part of implementing Task 2 below, and the
  executor must verify (via the reap assertion itself) that every one of
  the 16 cases actually reaches its named hook and gets killed — a case
  that finishes Phase 2 with `reaped == 0` is a failing test, not a no-op,
  and must be fixed by choosing the correct trigger request for that hook
  name.
- **`bitmap_update` requires a bitmap-typed index, but the matrix's test
  object only has a btree index on `score`.** The `idx_bitmap_pre_update`/
  `idx_bitmap_post_update` cases will never fire against `chm`'s schema as
  written above. The executor must add a second object (or a bitmap-typed
  index field) specifically for those two cases — a `bool` or low-cardinality
  `int` field with an explicit bitmap index type at `create-object` time
  (see `docs/query-protocol/schema-mutations.md` / `create-object`'s index
  type parameter for the exact JSON shape) — and route those two hook
  names to update that field instead of `score`. This is a required
  correction during Task 2/3, not optional polish; the same
  reaped-but-didn't-fire failure mode from the previous bullet is the
  signal that it's needed.
- **`waitpid` reaping a daemon that `test_env_stop_keep`/`test_env_stop`
  later also tries to reap.** The runner sets `env.daemon_pid = -1` after
  a confirmed reap specifically so the later `test_env_stop` call (which,
  per `fixtures.c`, sends a graceful signal and waits) doesn't operate on
  an already-dead, already-reaped pid. Confirm `fixtures.c`'s
  `test_env_stop`/`test_env_kill` tolerate `daemon_pid <= 0` as a no-op
  (grep `test_env_stop` in `fixtures.c` to verify before relying on this);
  if they don't, this plan's Task 3 must add that guard to `fixtures.c` as
  a small prerequisite fix (call out any such change explicitly in the
  PR/diff, since it touches shared test infrastructure).
- **Flakiness from the 50ms-poll reap loop.** 100 iterations × 50ms = 5s
  ceiling; a hook that never fires (see first two bullets) burns the full
  5s per case before the "not reaped" assertion fails, so a buggy dispatch
  table makes the whole matrix run slowly, not just incorrectly — the
  executor should get every case actually reaping well under 1s before
  considering Task 3 done, as a sign the dispatch table is correct.
- **This test is inherently slower than most existing cases** (16 cases ×
  3 daemon start/stop cycles each). Acceptable given it runs as one
  `TEST_REGISTER` entry (`test-crash-hook-matrix`), same as any other
  single case in `run-all`; not parallelized further within itself.

## Documentation sync

None needed for the production surface — `CRASH_HOOK_NAME` is a test-only
db.env knob, explicitly excluded from `docs/getting-started/configuration.md`
by the same convention already established for `KFCACHE_TEST_HOLD_MS`/
`WARMUP_TEST_DELAY_MS` (see the comments at each parse site above).

## Tasks

### Task 1 — mechanism (Part 1)

Implement 1a/1b/1c exactly as specified. No test yet — this task has no
independent regression test of its own (the mechanism is only observable
through Part 2's hook sites + Part 3's test, which is Task 3).

Build: `SKIP_TESTS=1 ./build.sh`. Confirm it compiles with no new warnings.

### Task 2 — hook placement (Part 2, all 7 categories)

Apply every anchor in Part 2 exactly as specified, in any order. After
all insertions, `grep -c "crash_hook_fire(" src/db/slotcask.c
src/db/index.c src/db/query_find.c` must show 5, 4, 6 respectively (16
call sites total across the three files, plus the definition itself in
`util.c` — one `crash_hook_fire(` string per site listed above: 2 in
Category 1, 3 in Category 2, 1 in Category 3 = 6 in `slotcask.c` not 5 —
executor must recount against the actual sites added and treat any
mismatch against this comment as a signal to recheck which anchors were
actually applied, not silently proceed).

Build: `SKIP_TESTS=1 ./build.sh`. Confirm it compiles with no new
warnings.

### Task 3 — test file + build.sh registration (Part 3)

1. Add `tu_count_json_array_items` to `fixtures.h`/`fixtures.c` first;
   write a tiny standalone sanity check (can be inline in the same task,
   not a separate registered test) confirming it returns the right count
   for a hand-written 3-element array string, before wiring it into the
   matrix test.
2. Add `src/test/cases/test_crash_hook_matrix.c` as specified, applying
   the two required corrections from "Edge cases" (per-hook trigger
   dispatch for the `kf_publish_*` cases; a bitmap-indexed field/object
   for the `idx_bitmap_*` cases).
3. Register in `build.sh`.
4. Build: `SKIP_TESTS=1 ./build.sh`.

### Task 4 — run and fix until every case reaps correctly

```
./build/bin/shard-db-test run test-crash-hook-matrix
```

Paste full output. Every one of the 16 cases must show `reaped` (the
daemon actually died from the named hook) — any case reporting "daemon
terminated" as failed means the trigger-dispatch table needs a fix (see
Edge cases), not that the crash-hook mechanism itself is broken; iterate
until all 16 fire correctly.

Once all 16 reap, re-run and confirm invariants A-D pass for every case
**except** the one expected-red case discussed in the "Sequencing note"
above (`rebuild_post_walk_pre_cleanup`, which fails until Finding 11's
fix lands). If Finding 11's fix has already landed by the time this task
runs, all 16 cases must pass; if not, paste the one expected failure and
its exact assertion message so the human can confirm it matches Finding
11's known signature (data loss / `.rebuild_legacy_root` surviving) rather
than some other, unrelated bug.

### Task 5 — full suite regression check

```
./build/bin/shard-db-test run-all
```

Confirm no other existing case regresses — in particular, confirm the
`crash_hook_fire` calls added to hot paths (`seg_record_emit`,
`kf_put_new`, `kf_repoint_at_slot`, `kf_tombstone_at_slot`,
`update_idx_fn`) add no observable behavior change when
`CRASH_HOOK_NAME` is unset (every other test in the suite runs with it
unset, so a regression here would show up as unrelated test failures).

## Deliberately out of scope (separate future plans, not part of this one)

The human's original proposal included two companion suites beyond the
crash-hook matrix. Neither is written here — each needs its own scoping
pass (call-site inventory for the syscalls to intercept, and a from-scratch
reference-model design, respectively) rather than being tacked onto this
already-large plan:

- **`LD_PRELOAD` failure-injection shim** — intercepts
  `write`/`pwrite`/`ftruncate`/`msync`/`rename`/`fsync` and fails them with
  `ENOSPC`/`EIO`/short-writes; asserts the DB errors cleanly without
  corrupting already-committed data. Complementary to this plan (tests
  syscall-failure handling instead of mid-operation process death) but is
  a distinct mechanism (a shim `.so`, not an in-process named hook) and
  should be its own plan document.
- **Model-based randomized restart fuzzer** — seeded seqeuence of
  insert/update/delete/schema/index operations against both the real
  daemon and an in-memory reference model, kill/restart at random points,
  diff against the model after every restart. This is a substantially
  larger undertaking (a full reference-model implementation) than the
  fixed, hand-picked hook matrix here and deserves independent design
  review before a plan is written.

If the human wants either scoped and written as a plan, that's a separate
follow-up request — this document does not attempt either.

## Definition of done

- [ ] `Task 4`'s full 16-case run pasted, showing every case reaping via
      `SIGKILL` at its named hook.
- [ ] All invariants (A-D) pass for every case except the one expected-red
      case tied to Finding 11 (if that fix hasn't landed yet) — and that
      one case's failure output matches Finding 11's documented signature.
- [ ] Full suite green otherwise: `./build/bin/shard-db-test run-all`.
- [ ] No new compiler warnings from `./build.sh`.
- [ ] `grep -c "crash_hook_fire("` counts across `slotcask.c`/`index.c`/
      `query_find.c` match the sites actually added, reconciled per Task
      2's note.
- [ ] Diff limited to: `src/db/shard_db_internal.h`, `src/db/config.c`,
      `src/db/util.c`, `src/db/types.h` (Part 1); `src/db/slotcask.c`,
      `src/db/index.c`, `src/db/query_find.c` (Part 2, hook calls only —
      no unrelated changes to these already-large files);
      `src/test/fixtures.h`, `src/test/fixtures.c` (new helper),
      `src/test/cases/test_crash_hook_matrix.c` (new), `build.sh` (one
      line).
