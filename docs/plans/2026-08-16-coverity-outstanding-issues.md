# Coverity outstanding-issues export (5th) — implementation plan

Date: 2026-08-16. Scope confirmed by human 2026-08-16: **11 fixes + triage doc**
(the "Recommended" option). CIDs not listed in a task below are verdicts
recorded in `docs/coverity-triage-2026-08.md` (Task 12) and get **no code
change**.

## Evidence and root causes

Coverity export `Outstanding+Issues(5).csv`, 36 CIDs, all New/Unassigned.
Triage method: 4 parallel source-code investigations (one per module cluster)
plus direct verification of every high-severity claim against the working
tree. Each task below quotes the verified root cause.

Two findings are **real bugs reachable by ordinary input**:

1. **CID 1699814 (btree.c, High)** — stack-buffer overflow in
   `btree_bulk_build_locked`. `leaf_append` copies `vlen` bytes into
   `char last_key[BT_MAX_VAL_LEN]` (512) with no bound. Every other insert
   path guards `vlen > BT_MAX_VAL_LEN` (`btree.c:1415`, `btree.c:2025-2028`,
   `btree.c:2048`) — the bulk-build path does not. Reachable: create an
   object with an indexed `varchar:N`, N > 510 (no index-key cap exists at
   create-object), bulk-insert with the index pre-existing →
   `query_bulk.c:810` sets `bp->vlen = key_len` → `btree_bulk_merge`
   (`query_bulk.c:27`) → `btree_bulk_build_locked` on the empty tree →
   `memcpy(last_key, value, vlen)` for 512 < vlen ≤ ~4080.
2. **CID 1699817 (query_bulk.c)** — null dereferences in
   `bulk_upd_json_run`: unchecked allocations at `4520, 4542, 4600, 4622,
   4633, 4760-4761` dereferenced at `4524-4528, 4747-4748` and the memcpy
   sites. OOM-only.

Three findings are **real but only OOM- or lock-race-reachable**:

3. **CID 1699832 (query_plan.c, High)** — `parse_one_criterion` IN-list
   failure paths: on `xrealloc_or_free` failure the quoted path double-frees
   the already-freed array (`1383-1386`), and the bare/comma paths leak the
   parsed element strings **and silently return 0 with an empty IN list**
   (`1403-1404`, `1429-1430`) — wrong query results, not just a leak.
4. **CID 1697144 (slotcask.c)** — real data race in
   `segcache_invalidate_prefix`: `e->path` is read unlocked (`1468`) while
   `segcache_acquire` installs into the same slot. The kfcache twin
   (`kfcache_invalidate_slot_if_prefix`) holds the table lock for the
   pre-scan; this one does not.
5. **CID 1699818 (index.c, High)** — `cmd_add_indexes` writes
   `typed_field_index()` results into `MFFieldDesc` with no local `fi_t >= 0`
   guard; the trigram branch stores `-1` into `d->field_indices[0]`
   (`3003-3008`), `bm_bool_fastpath` indexes `ts_for_idx->fields[fi_t]` with
   `fi_t == -1` (`2994`). A validation loop makes it unreachable today, but
   the inner path is one refactor away from a real out-of-bounds read.

Five findings are **benign but cheap to clean** (remove dead code / add the
same annotation the neighboring paths already carry / defensive parity):

6. **CID 1697135 (server.c)** — dead ucache stats block: `uc_used`,
   `uc_total`, `uc_bytes` are initialized to 0 and never written; the
   `u_hit_pct` ternary at `1056` is constant.
7. **CID 1699820 (index.c)** — vestigial `if (descs)` guards (`2985`,
   `3002`, `3035`); `descs` is non-NULL at that point.
8. **CID 1700140 (slotcask.c)** — `kfcache_acquire_ex` install return
   (`683-685`) lacks the `coverity[missing_unlock]` annotation the other two
   handoff sites have (`488-489`, `565-566`).
9. **CID 1700139 (slotcask.c)** — plain `e->used = 1` store to an `_Atomic`
   field at `658`; convert to `atomic_store_explicit` (matches
   `656-657, 662`).
10. **CID 1699811 (btree.c)** — `btree_delete` lacks the `vlen >
    BT_MAX_VAL_LEN` guard `btree_insert` has (`2025-2028`); add parity.

One **same-root-cause finding not in the CSV**, verified reachable, folded
into Task 2: `mf_append_field` (`index.c:3424-3456`) writes an unbounded
index key into `uint8_t kb[4096]` on the simple (non-composite) path — the
composite path bounds each append (`3433`), the simple path has no cap
(`3443`). The same unbounded-generation pattern exists in `query_bulk.c`'s
composite index-key accumulation (`649-658`). These are reachable via
add-index/reindex and bulk-insert-with-pre-existing-index on `varchar:N`
fields with N > 4094 (and > 4096 − accumulated offset for composites).

The remaining 26 CIDs are false positives — init-before-threads cache
setups, mkstemp temp files, documented intentional lock handoffs, already
present bounds checks, and checksum-validated marker recovery. Verdicts with
one-line rationale go into the Task 12 triage doc so the CIDs can be marked
in Coverity.

## Global execution rules

- Start from current `main` on a fresh `fix/coverity-outstanding-issues`
  branch.
- Leave all changes **uncommitted** for raw-diff review (repo standing
  exception in AGENTS.md); nothing is committed until the reviewing agent +
  human review the diff.
- Build with `SKIP_TESTS=1 ./build.sh`; test with
  `./build/bin/shard-db-test run-all` (default all-core parallelism; do not
  add `--jobs`).
- If a quoted anchor below is absent, write `PLAN_NOTES.md` and stop.
- Never run benchmarks (`./build/bin/shard-db-bench` is human-run only).
- New/changed code gets no comments beyond what the tasks quote.
- Each task is test-first: write the red test, confirm it fails for the
  stated reason, then apply the fix and confirm green.

## Task 1 — CID 1699814: bound `vlen` in `btree_bulk_build_locked`

**Root cause.** `btree.c:2899` declares `char last_key[BT_MAX_VAL_LEN];`
(512). The bulk-build loop (`2902-2941`) calls `leaf_append` with
`entries[i].vlen` unbounded; `leaf_append` at `1307-1308` does
`memcpy(last_key, value, vlen)` — a stack overflow for 512 < vlen ≤ ~4080
(an entry that still fits the 4096-byte leaf page, so `leaf_append` returns
success and the `result == -1` page-full path never fires).

**Red.** New case `src/test/cases/test_index_oversized_varchar.c` (wire
pattern per `test_bulk_upsert.c`):

- `add-dir default`; `create-object oversized_t` with
  `"fields":["tag:varchar:600"],"indexes":["tag"],"splits":8`.
- `bulk-insert` 50 records whose `tag` values are 580 bytes (within the
  varchar cap, above BT_MAX_VAL_LEN).
- Assert the response contains no `"error"`; assert `count` (no criteria)
  == 50; assert `count` with `{"field":"tag","op":"eq","value":"<580B>"}`
  == 0 (documents the skip policy below).

Before the fix this test **crashes with a stack overflow under ASan** (run
the suite with `BUILD_MODE=asan` per AGENTS.md; the case alone via
`./build/bin/shard-db-test run test-index-oversized-varchar`).

**Fix.** Guard at the top of the bulk-build loop, mirroring the documented
best-effort skip of `btree_insert_batch_locked` (`btree.c:2048`). In
`btree_bulk_build_locked`, replace:

```c
    for (size_t i = 0; i < count; i++) {
        uint8_t *page = bt_page(&bt, cur_leaf);
```

with:

```c
    for (size_t i = 0; i < count; i++) {
        /* Best-effort index policy (mirrors btree_insert_batch_locked):
           an index key longer than BT_MAX_VAL_LEN cannot be stored in a
           leaf, so skip it rather than overflow the prefix buffer. The
           record still exists in the data shard. */
        if (entries[i].vlen > BT_MAX_VAL_LEN) continue;
        uint8_t *page = bt_page(&bt, cur_leaf);
```

This also covers the retry `leaf_append` at `2936-2939`, which no oversized
entry can reach.

**Green.** Same case passes under ASan; existing btree/bulk suites green
(`test_btree`, `test_btree_bulk_merge_*`, `test_slotcask_v2_bulk`,
`test_bulk_upsert`).

## Task 2 — same root cause: bound index-key generation scratch buffers

**Root cause.** Three index-key generation sites write an unbounded key into
a fixed stack buffer *before* the btree guard of Task 1 would apply:

- `mf_append_field` (`index.c:3424-3456`), simple path `3440-3445`:
  `typed_field_to_index_key(ts, value, fidx, kb, &kl)` with
  `uint8_t kb[4096]`, no cap; overflow for `varchar:N` with N > 4094.
  Reachable via `cmd_add_indexes` / `reindex` on such a field.
- `mf_append_field` composite path `3426-3439`: each
  `typed_field_to_index_key` writes into `cat + cpos` and the
  `cpos + blen < sizeof(cat)` check (`3433`) runs *after* the write — a
  single field with N > 4096 − cpos overflows before the check.
- `query_bulk.c` composite index-key accumulation (`649-658`): same
  write-then-check pattern into its `cat` buffer; reachable via
  bulk-insert-with-pre-existing-index on a composite-indexed long-varchar
  object.

**Red.** Extend `test_index_oversized_varchar.c` from Task 1:

- `create-object` `rebuild_t` with `"fields":["big:varchar:5000","a:varchar:3000","b:varchar:3000"]`, **no** indexes; `bulk-insert` 20 records with 4500-byte `big` values and 3000-byte `a`/`b` values.
- `add-index` on `big` (simple path); `add-index` on `a+b` (composite
  path); `reindex` the object.
- Assert each command's response contains no `"error"`, and `count` with
  no criteria == 20.

Before the fix this overflows `kb[4096]` / `cat[4096]` under ASan.

**Fix.** A conservative per-field pre-check: the maximum encoded index key
length for any field type is `field.size` (varchar's on-disk size is N+2,
content N — an over-estimate of 2 bytes, safe). Add the check *before* each
generating call.

In `mf_append_field` (`index.c`), replace the composite loop head:

```c
        for (int i = 0; i < d->field_index_count; i++) {
            size_t blen = 0;
            typed_field_to_index_key(ts, value, d->field_indices[i],
                                      (uint8_t *)cat + cpos, &blen);
```

with:

```c
        for (int i = 0; i < d->field_index_count; i++) {
            /* Pre-check remaining scratch space before the write: the
               field's max encoded key length is its on-disk size. */
            if ((size_t)ts->fields[d->field_indices[i]].size >
                sizeof(cat) - (size_t)cpos) { ok = 0; break; }
            size_t blen = 0;
            typed_field_to_index_key(ts, value, d->field_indices[i],
                                      (uint8_t *)cat + cpos, &blen);
```

and replace the simple path:

```c
    } else {
        int fidx = d->field_indices[0];
        if (fidx < 0) return;
        typed_field_to_index_key(ts, value, fidx, kb, &kl);
        if (kl == 0) return;
    }
```

with:

```c
    } else {
        int fidx = d->field_indices[0];
        if (fidx < 0) return;
        if ((size_t)ts->fields[fidx].size > sizeof(kb)) return;
        typed_field_to_index_key(ts, value, fidx, kb, &kl);
        if (kl == 0) return;
    }
```

In `query_bulk.c` (`649-658` composite accumulation): apply the same
pre-check — before each `typed_field_to_index_key` call, if
`(size_t)ts->fields[tidx].size > cat_cap - cpos`, set the skip flag and
break the accumulation (the executor reads the exact loop at
`query_bulk.c:640-672` and mirrors the `mf_append_field` pattern; if the
loop's structure differs from this anchor, write `PLAN_NOTES.md` and stop).

**Green.** `test_index_oversized_varchar.c` passes under ASan; reindex /
add-index suites green (`test_add_indexes_single_scan`,
`test_bitmap_index`, `test_binary_index`).

## Task 3 — CID 1699817: null-check the allocations in `bulk_upd_json_run`

**Root cause.** Six unchecked allocations in `query_bulk.c`:
`json = malloc(len + 1)` (`4520`, dereferenced at `4524-4528`),
`records = malloc(...)` (`4542`, dereferenced at `4747-4748`),
object-format `key = malloc(klen + 1)` (`4600`, deref `4601-4602`),
`obj_str = malloc(obj_len + 1)` (`4622`, deref `4623-4624`),
array-format `key = malloc(ivl + 1)` (`4633`, deref `4634-4635`), and
`r->field_indices / r->field_values` (`4760-4761`, deref `4765-4766`).

**Red.** No deterministic red is possible without alloc-failure injection.
Add a TEST_BUILD-only fail counter in `query_bulk.c` (precedent:
`index.c:2229-2237` errcode injection): `static int g_bulk_upd_fail_alloc;`
with a setter, checked at each of the six sites before the allocation
(`#ifdef TEST_BUILD`), returning the same error path as a real `NULL`.
New case `src/test/cases/test_bulk_update_json_oom.c` (wire pattern per
`test_bulk_update_json.c`): create a typed object, arm the counter, send a
`bulk-update-json` request, assert the response carries `"error"` and the
server keeps serving (follow-up request succeeds). Under ASan, the pre-fix
run crashes at the injected NULL deref — that is the red.

**Fix.** Add a single cleanup label and route all six sites through it.
At each site, replace the unchecked allocation with:

```c
            key = malloc(klen + 1);
            if (!key) goto bulk_upd_oom;
```

(with the same shape for the other five sites; for `4520`, close `ifd`
first — the current code closes it at `4533`). Add the label at the
function's existing cleanup tail, reusing the established free pattern
(quoted at `4562-4565` for json/records, and the per-record frees at
`4738-4741`); the executor reads `bulk_upd_json_run`'s full tail and places
`bulk_upd_oom:` immediately before the final `OUT(...); return` so every
resource allocated so far is freed, then:

```c
bulk_upd_oom:
    if (json_mmaped) munmap((void *)json, len);
    else if (input_is_file) free(json);
    free(records);
    free(key_refs);
    bulk_upd_json_if_free(&if_crit, &if_ncrit);
    for (size_t i = 0; i < rec_count; i++) {
        free(records[i].field_indices);
        free(records[i].field_values);
        free(records[i].key);
        bulk_upd_json_if_free(&records[i].if_crit, &records[i].if_ncrit);
    }
    free(errors);
    OUT("{\"error\":\"oom: bulk_update_json\"}\n");
    return 1;
```

(The executor adapts the exact frees to the function's real cleanup tail;
any divergence from this anchor → `PLAN_NOTES.md` and stop.)

**Green.** `test_bulk_update_json_oom.c` passes (error surfaced, server
alive); `test_bulk_update_json.c` and `test_bulk_cas.c` remain green under
ASan.

## Task 4 — CID 1699832: fix the `parse_one_criterion` IN-list failure paths

**Root cause.** `query_plan.c:1380-1393` (quoted-array path): on
`xrealloc_or_free` failure the array is already freed, then
`1385-1386` frees elements *through the freed array* and frees it again —
double-free / use-after-free. `1403-1404` (bare tokens) and `1429-1430`
(comma path) instead set `in_values = NULL; ... break` — leaking every
already-parsed element string **and returning 0 with a silently empty IN
list** (a `field IN (…)` criterion matches nothing).

**Red.** Add a TEST_BUILD-only fail counter in `query_plan.c`
(precedent: Task 3 / `index.c:2229`): `static int g_query_plan_fail_grow;`,
checked at the three grow sites; when armed, behave as if `realloc` failed.
New case `src/test/cases/test_query_plan_in_oom.c` (unit case using the
runner's process-local ShardDb, per `test_planner_*.c`): parse a criteria
array containing `{"field":"f","op":"in","value":["a","b","c"]}`, arm the
counter so the third element's grow fails, assert the parser returns an
error (non-zero) rather than a criterion with an empty IN list. Pre-fix:
returns 0 with `in_count == 0` (and, under ASan, the double-free at
`1385-1386`). That is the red.

**Fix.** Replace `xrealloc_or_free` at all three grow sites with plain
`realloc` + an explicit failure cleanup that is correct in both worlds
(`realloc` leaves the original array live on failure). Quoted path
(`1380-1393`) becomes:

```c
                        if (c->in_count >= c->in_cap) {
                            int new_cap = c->in_cap * 2;
                            char **t = realloc(c->in_values,
                                               (size_t)new_cap * sizeof(char *));
                            if (!t) {
                                free(val);
                                for (int i = 0; i < c->in_count; i++) free(c->in_values[i]);
                                free(c->in_values); c->in_values = NULL;
                                c->in_count = 0; c->in_cap = 0;
                                free(v); free(v_raw); free(v2);
                                return -1;
                            }
                            c->in_values = t;
                            c->in_cap = new_cap;
                        }
                        c->in_values[c->in_count++] = val;
```

Bare path (`1401-1407`) and comma path (`1427-1433`) get the same
realloc + cleanup shape and **return -1** (frees elements + array +
`v`/`v_raw`/`v2`, same as the existing quoted-path cleanup at `1362-1367`)
instead of `break` — no silent empty IN list.

**Green.** `test_query_plan_in_oom.c` passes (error surfaced, ASan-clean);
the planner/IN suites green (`test_planner_*`, `test_and_intersection`,
`test_find_indexed_orderby`).

## Task 5 — CID 1697144: lock the pre-scan in `segcache_invalidate_prefix`

**Root cause.** `slotcask.c:1462-1488`: the pre-scan reads
`atomic_load(&e->used)` then `strncmp(e->path, …)` (`1467-1468`) with no
lock, while `segcache_acquire` installs into the same slot under
`g_segcache_lock`. The kfcache twin (`kfcache_invalidate_slot_if_prefix`)
holds its table lock across the pre-scan and discards via the drop-slot
helper; the segcache version mutated inline under the entry rwlock only.

**Fix.** Rewrite the function to mirror the kfcache twin, reusing the
existing `segcache_drop_slot` (contract quoted at `1490-1491`: caller holds
`g_segcache_lock`, returns with it held; it releases the table lock while
taking the entry wrlock at `1502-1505` and re-verifies `used/gen/path` at
`1507-1512` — entry → table lock order preserved):

```c
static void segcache_invalidate_prefix(const char *prefix) {
    if (!g_segcache || !prefix || !prefix[0]) return;
    size_t pl = strlen(prefix);
    pthread_mutex_lock(&g_segcache_lock);
    for (int i = 0; i < g_segcache_slots; i++) {
        SegCacheEntry *e = &g_segcache[i];
        if (!atomic_load_explicit(&e->used, memory_order_acquire)) continue;
        if (strncmp(e->path, prefix, pl) != 0) continue;
        segcache_drop_slot(i, CACHE_DROP_DISCARD, 1);
    }
    pthread_mutex_unlock(&g_segcache_lock);
}
```

`CACHE_DROP_DISCARD` skips the dirty flush branch (`1513-1518`), matching
the current function's structural-discard semantics (`1472-1483`).

**Red/verification.** A deterministic functional red is not feasible for
this race; the red condition is TSan on the existing cache-invalidation
suite (`test_segcache_staleness`, `test_kfcache_staleness`,
`test_bitmap_kfcache_lock_order`, `test_btcache_evict_race`), which
exercises concurrent acquire/invalidate. The final TSan gate (below) is the
evidence; the correctness argument is the now-identical lock discipline
with the kfcache twin.

**Green.** `test_segcache_staleness` and friends green in the normal run
and the TSan gate; behavior unchanged (same discard semantics).

## Task 6 — CID 1699818: guard `fi_t` in `cmd_add_indexes`

**Root cause.** `index.c:2994` reads `ts_for_idx->fields[fi_t].type` with an
unguarded `fi_t` (would be `-1` for an unknown field name), and `3003-3008`
stores that `-1` into `d->field_indices[0]`; `3037/3055` likewise. A
validation loop upstream makes this unreachable today, but the inner path
must not index with `-1` (negative-array-index read if that loop is ever
reordered).

**Fix.** Guard locally in each branch, erroring out rather than silently
continuing. Bitmap branch (`2985-2996`) becomes:

```c
            if (descs) {
                int fi_t = typed_field_index(ts_for_idx, names[i]);
                if (fi_t < 0) {
                    OUT("{\"error\":\"add-indexes: unknown field \\\"%s\\\"\"}\n", names[i]);
                    free(descs);
                    return -1;
                }
                MFFieldDesc *d = &descs[n_desc++];
                memset(d, 0, sizeof(*d));
                d->type = MF_BITMAP;
                strncpy(d->name, names[i], sizeof(d->name) - 1);
                d->field_indices[0] = fi_t;
                d->field_index_count = 1;
                d->bm_max_values = maxes[i];
                d->bm_bool_fastpath = (ts_for_idx->fields[fi_t].type == FT_BOOL) ? 1 : 0;
            }
```

Trigram branch (`3002-3011`): same `fi_t < 0` → error guard after
`typed_field_index(ts_for_idx, names[i])`. Btree branch (`3037-3057`):
guard `fi_t < 0` for the simple case (same error text), and for the
composite case keep the existing `ci >= 0` per-part skip (`3051`).

**Red/verification.** Not wire-reachable today (unknown field names already
error at `2955-2964`); the regression net is the existing add-indexes suite
(`test_add_indexes_single_scan`) plus the ASan gate, and the red is the
Coverity finding itself. The Task 2 `rebuild_t` portion of
`test_index_oversized_varchar.c` also exercises these branches after the
guards exist.

**Green.** add-indexes suite green; the new test's add-index steps green.

## Task 7 — CID 1699811: `btree_delete` vlen parity guard

**Root cause.** `btree_delete` (`btree.c:2156-2170`) descends the tree with
an unbounded `vlen`, while its sibling `btree_insert` rejects
`vlen > BT_MAX_VAL_LEN` (`2025-2028`). Coverity flags the tainted value
flowing into the search/compare reads of `btree_delete_locked`.

**Fix.** Mirror the insert guard in `btree_delete`, before the mutation
gate:

```c
int btree_delete(const char *path, const char *value, size_t vlen,
                 const uint8_t hash[BT_HASH_SIZE]) {
    if (vlen > BT_MAX_VAL_LEN) {
        errno = EINVAL;
        return -1;
    }
#ifdef TEST_BUILD
```

**Red.** Extend `src/test/cases/test_btree.c`'s delete coverage: call
`btree_delete` with a 600-byte key on a small tree and assert it returns -1
with `errno == EINVAL` and the tree is untouched. Pre-fix the call
succeeds (and reads garbage) — the assertion fails, that is the red.

**Green.** The new assertion passes; `test_btree.c` green.

## Task 8 — CID 1697135: remove the dead ucache stats block

**Root cause.** `server.c`: `uc_used/uc_total/uc_bytes` are initialized to 0
(`1037`) and never written; `u_hits/u_miss` are constant 0 (`1040-1041`),
so the `u_hit_pct` ternary (`1056`) is constant and the ucache output lines
are permanent zeros. The "ucache" concept no longer exists in the codebase.

**Fix.** Remove the ucache block from all three formatters:

- Table (`1037-1041` `uc_*` declarations, `1055-1056` `u_hit_pct`, and the
  `ucache` line `1062-1063`).
- JSON (`1080-1081` `"ucache":{...}` field and the `uc_used, uc_total,
  uc_bytes, u_hits, u_miss` arguments at `1087`).
- Prometheus (`1121` `uc_*` declarations, `1124-1125` `u_hits/u_miss`, and
  the eight `shard_db_ucache_*` lines `1144-1158`).

The CLI stats view (`cli/views.c:1114-1135`) flattens any nested object
generically — no CLI change needed; the ucache mention in its comment
(`1114-1115`) is updated to a generic wording. No test asserts ucache today
(grep over `src/test` shows none), so the regression net is
`test_bench_stats` + `test_bare_shapes` (stats JSON consumers).

**Red/verification.** No functional red exists; verification is: no
`ucache`/`u_hits`/`u_miss`/`uc_used` references remain in `src/db/server.c`
(grep), `test_bench_stats` green, CLI builds (`./build.sh` builds
`shard-cli`).

**Green.** Stats suites green; `stats`, `stats-prom` output contains no
ucache lines.

## Task 9 — CID 1699820: remove the vestigial `if (descs)` guards

**Root cause.** `index.c:2985`, `3002`, `3035`: `descs` is unconditionally
allocated and non-NULL at every point where the guard is checked, so the
branch is always taken.

**Fix.** Remove the `if (descs) {` guard and its closing brace at each of
the three sites, dedenting the bodies to the surrounding column (the
bitmap body quoted in Task 6 is the result after this task; do Task 6 and
Task 9's edits to the same three blocks in one pass to avoid conflicts).

**Red/verification.** Dead-guard removal; regression net is the add-indexes
suite from Task 6.

**Green.** Same as Task 6.

## Task 10 — CID 1700140: annotate the intentional handoff return in `kfcache_acquire_ex`

**Root cause.** The install path returns with the per-slot rwlock held
(`slotcask.c:683-685`) without the annotation the two other handoff sites
carry (`488-489`, `565-566`).

**Fix.** In `kfcache_acquire_ex`, replace:

```c
    h->slot = slot;
    kf_handle_from_entry(h, e);
    return 0;
}
```

with:

```c
    h->slot = slot;
    kf_handle_from_entry(h, e);
    /* coverity[missing_unlock] intentional: returning with the per-slot
       rwlock held; caller releases via kfcache_release. */
    return 0;
}
```

**Green.** Existing kfcache tests green (`test_kfcache_staleness`,
`test_bitmap_kfcache_lock_order`).

## Task 11 — CID 1700139: atomic store for `e->used` in `kfcache_acquire_ex`

**Root cause.** `slotcask.c:658` writes `e->used = 1` to an `_Atomic` field
as a plain store; readers use `atomic_load_explicit` (e.g. `1467` in the
segcache twin).

**Fix.** Replace:

```c
    e->used = 1;
```

with:

```c
    atomic_store_explicit(&e->used, 1, memory_order_release);
```

**Green.** Same as Task 10.

## Task 12 — triage doc and Coverity bookkeeping

Write `docs/coverity-triage-2026-08.md` in the format of
`docs/coverity-triage-2026-07.md` (36-row table: CID | Function | Verdict |
Note; verdicts TP/FP with one-line rationale), covering all 36 CIDs of the
5th export, marking the 10 fixed CIDs as **TP** (fix in
`docs/plans/2026-08-16-coverity-outstanding-issues.md`) and the 26 as
**FP** with the rationale each task's root-cause section names. Note the
11th fix (index-key scratch guards) as a same-root-cause addition.

## Final gate — dynamic-safety runs (Definition of done, AGENTS.md)

This diff touches cache/lock state (`segcache_invalidate_prefix`), stack
buffers and allocation paths, so the full dynamic-safety gate applies.
Run each suite **three consecutive times** locally before calling done:

```sh
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all   # ×3
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all   # ×3
```

Default all-core parallelism; no `--jobs` unless a demonstrated harness
limitation requires it. Any new finding gets root-caused and either fixed
now or written up per CORE-PROCESS — never papered over.

Then leave the tree **uncommitted** for the reviewing agent + human raw-diff
review. After review and merge, mark the 10 fixed CIDs as resolved in
Coverity per the Task 12 doc.

## Verification log (execution)

All runs: workspace DB_ROOT (`./db`), dev daemon stopped (required only by
`test-removed-storage-surfaces`, see below), full `run-all` suite:

| Build | Runs | Result |
|---|---|---|
| default | 1 | 12066 passed, 0 failed (423 cases) |
| ASan | 3 | 12066 passed, 0 failed each |
| TSan | 3 | 12066 passed, 0 failed each; 0 TSan warnings |

### Deviations found during execution

1. **Task 1 interplay discovered during repro** — the oversized-skip in
   `btree_bulk_build_locked` initially still counted skipped entries in the
   published `fh->entry_count`, so the first window's merge published a
   poisoned header (entry_count>0, zero leaf entries) and any *concurrent*
   window's merge on the same per-shard file then failed `extract-all`
   (`count != fh->entry_count`, EINVAL) → `apply_window` failure → the
   slotcask window protocol rejected the whole window's records. Repro:
   47/50 records rejected on an oversized-key bulk insert. Fix: count only
   entries actually appended (`stored`). `btree_insert_batch_locked`
   already skips before `entry_count++` and needed no change.
   `test_btree.c` extended with a rebuild-over-skipped-tree regression.

2. **Task 8 premise was wrong** — the plan claimed no test asserts the
   ucache metrics; `test-stats-prom` asserts all five (HELP/TYPE/sample
   lines + zero values pre/post traffic). Updated the test in lockstep
   (metrics removed; the integer-format sample check now pins
   `shard_db_bt_cache_hits_total` instead).

3. **Task 3 test semantics** — `bulk-update-json` is update-only (missing
   keys are skipped, never upserted), so the OOM test now seeds k1/k2 via
   `bulk-insert` first and asserts count==2 after the over-armed control.

4. **`query_plan.c` grow-injection design fix** — the original injection
   nulled `t` *after* a successful realloc, so the failure cleanup freed
   through the old (still live) pointer (`free(): invalid pointer` under
   ASan). The injection now substitutes NULL for the realloc *result* at
   all three grow sites, keeping the original block alive.

### Pre-existing, out of scope

`test-removed-storage-surfaces` fails "standalone compact is rejected" on
the base tree whenever a daemon is listening on `db.env` PORT (9199): the
test shells out to `./build/bin/shard-db compact default missing`, which
reads `db.env`'s port (not the test env's), gets a server error response,
and exits 0. With no daemon on 9199 (CI condition) the CLI fails to
connect and the test passes. Verdict: environment-dependent CLI
exit-code assertion, pre-existing (reproduced on the unmodified base
tree); not touched by this plan.