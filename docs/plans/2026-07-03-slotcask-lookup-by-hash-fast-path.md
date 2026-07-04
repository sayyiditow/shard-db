# Perf: wire slotcask_lookup_by_hash into the lock-free kfcache fast path

## Execution rules (read first)

- Branch off `main`: `git checkout -b perf/lookup-by-hash-fast-path`.
- Build with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- **Never claim the task passed without pasting the real command output.** "# total: N passed, 0 failed" from the actual test binary is the only acceptable evidence it's done.
- The edit below is located by **quoted anchor text** from the current file, not line numbers. If the anchor is not found character-for-character, **stop immediately** and write `docs/plans/PLAN_NOTES.md` describing what you searched for and what you found instead — do not guess.
- Leave all changes **uncommitted** on the branch when done.

## Background

`slotcask_lookup_by_hash` (`src/db/slotcask.c`) acquires its kf shard via the plain `kfcache_acquire()`, which always takes the full table mutex + per-slot rwlock path. A lock-free fast path already exists — `kfcache_acquire_direct()` — that skips the table mutex entirely on a warm hit (one atomic generation-counter load + a per-slot rdlock), falling back to the slow path only on a cold miss or eviction race. It requires a caller-owned, persistent `SlotRef *` that survives across calls to get the warm-hit benefit (a fresh/local `SlotRef` on every call would never warm up and provide zero benefit — this is why the fix isn't just "swap the function name").

That persistent state already exists and is already proven: `slotcask_get` (`src/db/slotcask.c`, the plain `get` command's implementation) indexes into `db->kf_slot_refs[sid_kf]` — a per-shard array of `SlotRef`s owned by the `SlotcaskDb` struct itself, so it stays warm across every separate `get` call against that shard, from any thread. `slotcask_lookup_by_hash` already receives the same `db` pointer and computes the same `sid_kf` — it just never got wired into this array.

This isn't only about plain `get`: `slotcask_lookup_by_hash` is what `read_record_ref` (`src/db/query.c`) calls to fetch a record once it already has a `hash16` in hand — and `read_record_ref` is the record-fetch step behind indexed `find`/`count`/joins/CAS lookups (confirmed call sites at `query.c:9227`, `11105`, `11753`, `16099`, `16249`, `17531`, `18959`, `19542`, `23585`). Fixing this speeds up every one of those, not just single-key `get`.

## Task 1 — Wire in the fast path

### File: `src/db/slotcask.c`

Find this exact block:

```c
int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0) return -1;
```

Replace it with:

```c
int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                            SlotcaskScanCb cb, void *ctx) {
    if (!db || !cb) return -1;
    int sid_kf = shard_for_hash(hash16, db->num_shards);
    char kf_path[PATH_MAX];
    kf_path_for(kf_path, db->data_dir, sid_kf);
    SlotcaskKfHandle kh;
    /* Same lock-free warm-hit fast path slotcask_get already uses:
       db->kf_slot_refs[sid_kf] is a per-shard SlotRef owned by the
       SlotcaskDb instance, so it stays warm across every call against
       this shard regardless of caller. kfcache_acquire_direct falls
       back to the plain kfcache_acquire slow path (and refreshes the
       ref) on a cold miss or eviction race — same correctness, fewer
       table-mutex acquisitions on the common warm path. */
    SlotRef *kf_ref = (db->kf_slot_refs) ? &db->kf_slot_refs[sid_kf] : NULL;
    if (kfcache_acquire_direct(&kh, kf_ref, kf_path, db->slots_per_shard,
                               db, sid_kf) != 0) return -1;
```

Nothing else in the function changes — the rest of the probe loop, `segcache_acquire`, and `kfcache_release(&kh)` calls are agnostic to which acquire path populated `kh` (verified by reading `kfcache_release`: it only branches on `h->slot >= 0`, which both acquire paths set identically).

### Invariant this preserves

`kfcache_acquire_direct` with `writer=0` is documented as read-only-caller-only (see the comment directly above its definition in `slotcask.c`) — `slotcask_lookup_by_hash` only ever reads (never calls `kfcache_acquire` with `writer=1`), so this is a safe fit. On the very first call for a given shard (or after that shard's cache slot gets evicted), `ref->slot` won't match and it transparently falls through to the exact same `kfcache_acquire` slow path this function already used — so worst case is identical to today's behavior, not worse.

## Verification

1. `SKIP_TESTS=1 ./build.sh` — must complete with no compile errors.
2. `./build/bin/shard-db-test run-all` — paste the real output; must show `# total: N passed, 0 failed` with `N` equal to whatever `run-all` reports on a clean `main` checkout *before* this change (run it once before starting, note `N`, then confirm the post-change run reports the same `N` with 0 failed) — this change only affects lock acquisition strategy, not any command's output or correctness.

Do not report this plan as complete without pasting the actual output of step 2.
