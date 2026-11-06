# Follow-ups from the bitmap-resplit blind reviews (no stone unturned)

Date: 2026-11-06. Planner: Claude Sonnet 5.5. Executor: whichever model the human designates (do not assume).
Status: **investigation + fix list, not yet an execution plan.** Each item below must be turned into a CORE-PROCESS task (quoted anchors,
complete code, test-first, red-then-green proof) before it is executed; the first step of every item is to re-read the cited code on the
then-current tree. Comes **after** `2026-11-06-bulk-chain-bitmap-sync-path.md`. Source: three blind reviews of
`2026-11-05-bitmap-stale-after-resplit-fix.md` (see its Addenda 1–4). Rule from the human: anything found here must be fixed, not parked.

Execution rules when this becomes a plan: work on `fix/type-correctness-recovery`, leave work uncommitted until the raw diff is reviewed,
build `SKIP_TESTS=1 ./build.sh`, full suite `./build/bin/shard-db-test run-all --jobs 4`, `rm -rf /tmp/shard-db-*` after every run, no benches
by the executor, sanitizers once at the end (items F1, F3, F5, F6 touch locks/cache). Any quoted anchor missing → `PLAN_NOTES-followups.md`, halt.

## Items (severity = worst credible outcome)

| # | Sev | Item | Why it is open |
|---|-----|------|----------------|
| F1 | P1 | **Bulk-insert window opens bitmap shards lazily in first-use order** (`bitmap_window_find_or_open`, `index.c`; caller `query_bulk.c` window loop). | The canonical lock order (ascending `<field>/NNN.bm`) covers the remap, single insert, AND-count, group-by and post-filter, but not this path. A window holding `kind` and later opening `flag` can deadlock against a bitmap-only reader or the remap. Pre-existing; the new remap raised the stakes. |
| F2 | P1 | **Bulk records can now fail where an inline resplit used to rescue them.** `kf_plan_window_insert_slot` no longer resplits while the window/request holds reservations. | Correct (the old behaviour corrupted the keyfile) but unproven that pre-grow always sizes the table. Needs a proof or a hardening: make the planner fail the *window* with a retryable status after growing the table between windows, or assert pre-grow coverage. Test with the `kf_max_slots` seam and an under-sized pre-grow. |
| F3 | P2 | **Uncached bitmap read handles see an in-place layout change.** `bm_open_impl` falls back to an uncached, lock-free handle when the cache is saturated or a stale inode is found; the in-place apply rewrites header/stride under the entry lock only. | One request can read the new layout with its old header → wrong count/find. Options: refuse uncached handles for bitmap reads (bounded wait/retry), or validate a layout generation after reading. Same fallback also gives *writers* no lock (pre-existing). |
| F4 | P2 | **A skipped corrupt bitmap keeps being served** until restart/`reindex` (`KF_BMFIX_SKIP`). | Already unreadable before the rebuild, but the cached entry may still answer. Options: invalidate the cache entry and fail reads of that shard with a clear error, or schedule the rebuild immediately. |
| F5 | P2 | **Startup repair reindexes the whole object and refuses startup on any failure.** `bmfix_recover_all`, `embedded.c`. | A per-shard bitmap problem costs a full btree+trigram+bitmap rebuild, and a persistent failure leaves no way to start (operator cannot run `reindex` on a daemon that will not start). Fix: rebuild only the bitmaps of the marked shards; add an escape (start with that object's bitmap planner disabled, log loudly). |
| F6 | P2 | **Bitmap-cache saturation: eviction self-wait / spin.** `bm_open_impl` ignores the return of `pthread_rwlock_wrlock`; a writer needing a slot can wait on an entry its own thread holds (`EDEADLK` is treated as "dropped"). The remap pins up to `MAX_FIELDS` writer handles under the kf wrlock. | Only reachable with a very small `BT_CACHE_MAX` or very many bitmap fields, but the failure is an unbounded spin holding the kf wrlock. Fix: check the lock return codes; make the remap refuse (abandon + back off) when it cannot get all its handles without evicting its own. |
| F7 | P2 | **Missing/weak tests.** (a) unclean start with pending KFM2 markers **and** a `.bmfix` marker (the real risk of the startup reorder); (b) deterministic lock-order test (pause hook between the first and second `bm_open` in `bm_open_many_ro`); (c) `test-bitmap-resplit-chain` cannot force a chain merge and did not turn red without the chain pre-grow fix; (d) `fault-prepare`/`fault-rename` depend on a 6 s sleep against the hard-coded 5 s back-off; (e) `br_log_has` greps the whole test dir. | Tests are the only proof for items F1, F2, F5, F6. Add a TEST_BUILD seam for the back-off interval and a chain-merge hook; assert on a specific log file. |
| F8 | P3 | **kf write-lock hold time** now includes one rewrite+fsync per bitmap of the shard (plus marker and directory fsyncs). | Latency only. The human benches `bench-grow`/`bench-kv`; if the regression matters, options: write the remap image before taking the kf wrlock where the old table is still valid, or parallel-write images. |
| F9 | P3 | `req_res_empty` is O(cap) when the map is empty; keep a running reservation count in `ReqShard`. | Minor CPU at ≥75% load. |
| F10 | P3 | **Docs.** AGENTS.md "inserts never refuse for capacity" is now conditional (bitmap trouble delays growth 5 s; window records can fail, F2). `concurrency.md` should state the uncached-handle limit until F3 lands. | Keep docs true. |
| F11 | P3 | **`.bm.remap` temporaries from a crash during prepare** are swept only at startup (`slotcask_bmfix_sweep`). | Cosmetic disk use between crash and restart; no action unless F5 changes startup. |

## Order

1. F1 (lock-order completeness; pairs with F7b) → 2. F2 (+ test) → 3. F6 → 4. F3 + F4 (shared design: what an uncached or damaged bitmap handle may do)
→ 5. F5 → 6. F7 remainder → 7. F9, F10, F11 → F8 only if the human's benches show a regression.

## Separate plan (already drafted)

`2026-11-06-bulk-chain-bitmap-sync-path.md` — the `%03d` vs `%03x` bitmap path in the bulk commit flush. Runs first.

## Done criteria

Every row above is either fixed with a red-then-green test, or has a written, human-approved reason it cannot be, recorded here.
