# Planner upgrade — completion runbook (handoff)

Self-contained steps to finish the planner upgrade if Claude isn't available. All
plans are written; this is the review → merge → deploy → cleanup sequence.

## Status snapshot (2026-06-04)

| Item | State |
|---|---|
| **WS1** materialization guard | ✅ merged to `main` (PR #125). **NOT deployed to prod.** |
| **WS2** ordered-path cost (A varchar bound / B sort-vs-walk / C cursor fetch+sort) | 🔄 executing on `perf/planner-ws2-ordered-path-cost`. Part A done+verified; B/C in progress. |
| **WS3** op-capability + in-fold + issue-C + scale | 📝 plan written (`docs/plans/2026-06-04-planner-ws3-op-capability.md`), not started. |
| **Prod daemon** (`/usr/local/bin/shard-db` @ 152.53.131.43) | At **Path A + cursor range-bounds (#124)**. Does **NOT** have WS1/WS2/WS3. |
| **Explorer** | ✅ done+deployed (top-commenters, hourly slow-stats, All-time trending removed, `type+score` index, `?near=` removed, pollopts deleted, `type in` filter removed). |
| **Known flaky test** | `test-planner-materialization-guard` → `not ok 4 - selective bitmap AND still materializes`, **full `run-all` only**, test-isolation, **not a product bug**. |

## Conventions (apply to every step)

- **One fresh branch per plan, off `main`, AFTER the prior plan merges** (they all edit `src/db/query.c` → conflict otherwise). Never reuse a branch / never work on `main`.
- **Build:** `SKIP_TESTS=1 ./build.sh`. **Test:** `./build/bin/shard-db-test run-all` (or `run <name>`). Never build/run while a model is mid-execution in the same working tree (colliding `build/bin`).
- **Never commit on red** — except the one known flake. If `run-all` shows exactly `test-planner-materialization-guard: 5 passed, 1 failed` with `not ok 4 - selective bitmap AND still materializes` and nothing else, **re-run**; it'll pass. Any *other* failure (or that case failing a different assertion) is real.
- **Commit authorship:** `git -c user.email=hashim@sayyiditow.dev -c user.name="Hashim Sayyid" commit --author="Hashim Sayyid <hashim@sayyiditow.dev>" -m "..."` with trailers:
  ```
  Co-Authored-By: DeepSeek <deepseek@sayyiditow.dev>
  Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>
  ```
- **Merge:** auto-merge is disabled; admin-merge is authorized: `gh pr create ...` then `gh pr merge <N> --merge --admin --delete-branch`, then `git checkout main && git pull`.
- When committing a workstream, stage **only that workstream's files** (and its own plan doc) — do **not** stage other plan docs or this runbook.

---

## Step 1 — Finish & merge WS2  (when the model reports B and C done)

1. Confirm the model is fully stopped. Then review the diff vs `docs/plans/2026-06-04-planner-ws2-ordered-path-cost.md`:
   - **Part B:** one `prefer_fetch_sort(candidates, N, offset, limit)` (crossover `candidates² < (offset+limit)·N`) used by **both** `pick_sort_or_walk` (signature now takes offset/limit — both call sites updated) **and** the executor's old `SMALL_PREFILTER_THRESHOLD=1000` override.
   - **Part C:** cursor path gained a small-candidate fetch+sort branch gated by the same `prefer_fetch_sort`, with a correct next-page cursor + a multi-page test (no dupes/gaps). **This is the highest-risk part — scrutinize pagination correctness.**
2. `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` → green modulo the known flake.
3. Commit (only WS2 files + the WS2 plan doc), push, PR, `--admin` merge, sync `main`.

## Step 2 — Run WS3  (after WS2 is on `main`)

1. Fire the model: branch `perf/planner-ws3-op-capability` off fresh `main`; execute `docs/plans/2026-06-04-planner-ws3-op-capability.md` parts in order (A op-cap table → B issue-C → C in-fold → D scale). Parts may be separate PRs.
2. Review focus: **A** must be behavior-preserving (captured plan-matrix identical pre/post); **B** = bitmap `OP_IN` card-est sums all values (est ≈160 not 100); **C** (in-fold k-way merge) = correctness — `tag in (a,c) ORDER BY t` returns a∪c in strict order, no b/d leakage, bounded scan, plan = `composite`.
3. Each part: `run-all` green (known flake aside), commit (authorship above), PR, admin-merge.

## Step 3 — Deploy shard-db to prod  (after WS1+WS2+WS3 on `main`)

Ships everything since #124 in one restart. **No `./migrate`** (planner-only, no on-disk format change).
```bash
cd <shard-db repo>; git checkout main && git pull
SKIP_TESTS=1 ./build.sh
scp build/bin/shard-db root@152.53.131.43:/usr/local/bin/shard-db.new
ssh root@152.53.131.43 'mv /usr/local/bin/shard-db.new /usr/local/bin/shard-db && chmod +x /usr/local/bin/shard-db && systemctl restart shard-db && sleep 4 && systemctl is-active shard-db'
```
- **CAUTION:** do NOT restart while any `bulk-delete` is running (known delete↔insert deadlock). Normal explorer refresh is fine.
- Daemon facts: systemd unit `shard-db`, db root `/var/lib/shard-db`, port `19199`, env there has `SLOW_QUERY_MS=2000`.

## Step 4 — Verify on prod (warm)

Run each; expect the "after". (Temporarily set `SLOW_QUERY_MS=500` in `/var/lib/shard-db/db.env` + restart to surface sub-2s ones, then revert.)
```
export HOST=127.0.0.1 PORT=19199   # on the server, cd /var/lib/shard-db
```
- `by=lif` comments `ORDER BY time` (non-cursor): 31s → **<~50ms** (WS2-A).
- `type=job` + 7d window `ORDER BY score` **cursor:null**: ~22s → **<~100ms** (WS2-C).
- `title starts "Ask HN" ORDER BY time` cursor:null: 6s → **fast** (WS2-C).
- `dead=false AND deleted=false ORDER BY score` cursor:null (homepage): 1.4s → **<~50ms** (WS1 gap-D).
- `type in (a,b) ORDER BY t` uses composite, not scan (WS3-C in-fold).
- Re-check the slow log is quiet at `SLOW_QUERY_MS=500`.

## Step 5 — Test-isolation flake fix (LAST, after WS3)

- Symptom: `test-planner-materialization-guard` `not ok 4` only in full `run-all` (serial runner, 182 cases). In-process `*_for_test` hooks accumulate a process-global (a non-evicting config/index cache or pool state) so a late case's `plan_filter` mis-evaluates. **Not a product bug** (daemon = fresh process per instance; prod counts correct).
- Likely fix: add a cache-reset and call it per-case in `test_run_all` (src/test/test_runner.c), or have the hooks drop the config caches before computing. Doing it last hardens all WS1/WS2/WS3 hooks in one pass.
- Verify: loop full `run-all` ~10× → 0 failures.

## Step 6 — Remaining backlog (separate, lower priority)

1. **bulk-delete ↔ concurrent bulk-insert deadlock** — `bulk-delete` (criteria & key-list) stalls the daemon when a `bulk-insert` runs concurrently (reproduced: stalled prod twice during pollopt cleanup; single-record delete unaffected). Storage-layer lock-ordering. Repro: concurrent delete + insert on an indexed object.
2. **criteria-delete perf** — `cmd_bulk_delete_criteria` Phase 1 full-scans (ignores indexes for the match); Phase 2 drops entries from every index per record. Fix: index-aware match + batched index drops.
3. **Bench gaps** — add benches for `bulk-delete` by **criteria** and delete-of-records-**with-indexes** (only delete-by-keys was ever benched).

---

### One-line summary of what's left
Review+merge **WS2** → run+review+merge **WS3** → **one prod deploy** (build+scp+restart, no migrate) → **verify** the 4 query shapes → fix the **test-isolation flake** → then the **delete/bench backlog**.
