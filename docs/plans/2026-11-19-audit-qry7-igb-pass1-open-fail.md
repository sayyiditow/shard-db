# fix/audit-igb-pass1-open-fail — AUD-QRY-7

Campaign: `docs/plans/2026-11-19-audit-campaign.md` Phase 4 (post-triad
queue). Finding: `AUD-QRY-7` in `docs/plans/2026-11-19-audit-index.md`
(major, fail-closed violation: btree open failures inside the indexed
group-by Pass-1 walk are treated as empty shards — those shards' groups
silently vanish from aggregate results). Branch name:
`fix/audit-igb-pass1-open-fail`. Branch base: harness working tree (or
`main` if landed); independent of QRY-5/-6, lands after them to keep
one branch in flight per reviewer.

## Brief (approval surface)

### Problem and root cause

The group-by Pass-1 walk exists as TWO routes in
`cmd_aggregate_do`'s IGB block, and BOTH fail open on btree open
failures:

1. **Parallel worker** — `igb_pass1_worker`
   (query_aggregate.c:4239): `btree_range_iter_open` failure →
   LOG_ERROR + `return NULL` (4251-4256) — no `w->aborted`, no errno
   capture, shard's groups silently dropped. The parallel route is
   gated at 6543-6546 (`run_serial = 1` unless `n_idx_g >= 4 &&
   live_for_pass1 >= 100000`) — so it serves only large objects.
2. **Serial fallback** — the `run_serial` path's inline Pass-1 loop
   (6799-6801): same open, `if (!it) continue;` — same silent drop,
   and it is the route for EVERY object under 100k records (the
   majority).

Both are fail-open outliers inside a block that otherwise fails
closed: `hbk_init` failure sets `w->aborted = 1` (4242-4246), the
deadline tick sets `aborted` (4262-4263), the bitmap branches
distinguish ENOENT-empty from hard errors and set `ctx.agg_error`
(6045-6051, 6135-6142).

**ENOENT semantics (settled by the repo's own precedent):** btree idx
shard files are created EAGERLY on the add-index/reindex build path
(`merge_spills_into_index`, index.c:2744+, publishes a valid empty
tree for every shard) but LAZILY for create-object-declared indexes
(`btree_idx_insert` index.c:105-113 → `bt_open_file` writer branch
`open(..., O_RDWR | O_CREAT)`, btree.c:618). `agg_walk_worker` already
codifies the correct handling (query_aggregate.c:2374-2392): **stat
first — ENOENT is an empty shard; an open failure on an existing shard
fails closed**. This plan mirrors that template.

Sibling fail-open sites discovered during review, in the same IGB
block, recorded but DESCOPED to their own finding (AUD-QRY-9, one
concern per branch): Pass-2's agg-field walk (`if (!it) continue;`,
6954-6956 — silently wrong sum/avg/min/max for that shard) and
`sec_map_build_worker` (4172-4177 — silent misses convert to record
drops via `multi_skip` at 4337/6882).

Found by the AUD-QRY-1 round-3 blind review (worker site) + the QRY-7
round-1 review (serial site, ENOENT truth, siblings).

### Logical flow

1. `IgbPass1Worker` gains `int open_errno;` (zero-init at worker setup).
2. Both Pass-1 routes adopt the stat-then-open template: `stat()` the
   shard's idx path first — `ENOENT` → empty shard (legitimate:
   lazy-created files), proceed as today; file exists but
   `btree_range_iter_open` fails → fail closed. (The stat-first shape
   is required because btree.c's failure path logs via
   `log_msg_sub`, and on an EMITTED message the log machinery calls
   time/localtime_r/strftime (config.c:237-240) which CLOBBER errno —
   the level filter itself is first (config.c:229), so errno survives
   only for suppressed lines; an emitted LOG_ERROR (btree.c:626, which
   also calls strerror) leaves it untrustworthy.)
3. Propagation normalizes to `EIO` (matching `agg_walk_worker`'s
   `a->err = EIO` choice): `agg_report_error` (2341-2353) maps only a
   fixed errno set — a raw EACCES would render as the misleading
   `{"error":"invalid typed record"}`; EIO renders as
   `{"error":"aggregate storage access failed"}` (7007-7010).
   Worker route: `w->open_errno = EIO; w->aborted = 1;` consumed
   post-join (6580-6583, alongside the existing `aborted` read —
   note `ctx.agg_error` is checked FIRST at 7007, so the error object
   carries the cause). Serial route: `ctx.agg_error = EIO; aborted =
   1; break;` — mirroring the in-loop precedent at 6854 — so the
   aborted-cleanup at 6906-6914 (which frees hbk/crit_ks/sec_maps)
   runs on the way out, and the emission goes through the existing
   7007-7010 error-first path. (Round-2: the block has NO `err` local
   in scope — `agg_walk_worker` signals via its `a->err` FIELD at
   2365, consumed by agg_walk_field's merge at 2418+, and the only
   `int err` in cmd_aggregate_do is block-scoped inside the earlier
   non-IGB walk branch at 4854/4861. A bare
   `ctx.agg_error = EIO; goto igb_skip;` also REJECTED: it skips the
   6906-6914 cleanup and leaks hbk/crit_ks/sec_maps whenever those
   are live — group_by=v + sum(t) via hbk_init at 6343 under
   `if (igb_needs_hbm)` 6329, multi-field sec_maps 6441, criteria
   crit_ks 6355.)
4. `ctx.agg_error = EIO` → `{"error":"aggregate storage access
   failed"}` reaches the client instead of silently-short groups.

### Decisions made

| decision | alternatives | why | tag |
|---|---|---|---|
| Fix BOTH Pass-1 routes (worker + serial fallback); stat-then-open ENOENT template; normalize to EIO | worker-only + big fixture; errno-based split; raw errno propagation | the serial route serves every object < 100k records — fixing only the worker would leave the defect live for the majority (round-1 blocker); the stat-first template is the repo's own precedent (agg_walk_worker 2374-2392) and is errno-clobber-immune; EIO gives the honest error text (round-1 major) | DEFAULT |
| Scope = Pass-1 only; Pass-2 (6954-6956) and sec_map (4172-4177) recorded as AUD-QRY-9, own branch | fix all four sites here | one concern per branch; the two siblings are distinct walks (agg-field values, secondary group maps) with their own consumer chains — they get their own plan | DEFAULT |
| Error out (fail closed) rather than `aborted`-only fallback to `agg_run_plan` | pure `aborted` (correct-if-slower rescan, no error) | `aborted` in this block means "fall back to the general plan" (6906-6914 → igb_skip → 7022) — a defensible alternative, but the block's bitmap precedent (6049-6050) errors on hard faults; consistency + the operator-visibility argument win. (Comment at the fix site notes the deliberate departure from the aborted-only pattern.) | DEFAULT |

### Blast radius

- Consumers: both Pass-1 routes in `cmd_aggregate_do`'s IGB block.
- Behavior change (flag in PR body): aggregate group_by over a
  btree-indexed group field now returns `{"error":"aggregate storage
  access failed"}` when a shard's EXISTING btree file fails to open,
  instead of silently-short groups — for objects of every size (the
  serial route included).
- Healthy objects: unchanged (stat hit + open success → identical
  behavior; the stat adds one syscall per shard per query — measured
  negligible at splits ≤ 4096; paste a before/after timing of a
  group_by on a splits=64 object if the executor wants certainty).
- No wire/protocol/on-disk change.

### Test strategy

- New product regression case `test-igb-pass1-open-fail` (Task 1):
  deterministic hard-fault — `chmod 000` an EXISTING shard's `.idx`
  file while the daemon is stopped, restart, aggregate group_by → on
  base: silently missing groups (RED); post-fix: the EIO error object
  (GREEN). chmod is deterministic and needs no fault interposer;
  warmup does not cache the file (server.c:3067-3072, open/read/close
  only) and bt_cache installs no negative entry on open failure
  (btree.c:911 returns -1 before install). Restore = chmod 644.
  NOT a delete: create-object-declared index files are lazy
  (btree.c:618 O_CREAT) — a missing file is ENOENT = legitimate empty
  shard under the stat-first template.
- Baseline sanity + oracle comparison as in every plan; full suite +
  sanitizer gate once each (worker-owned scalar + break-based
  propagations that route through the existing 6906-6914 cleanup —
  no locks/shared state, no early-exit that skips lifetimes; a shape
  that skipped the cleanup would be lifetime-touching and owe the
  three-consecutive-runs gate instead).

### Out of scope

- AUD-QRY-9 (Pass-2 agg-field walk 6954-6956 + `sec_map_build_worker`
  4172-4177 fail-opens — same class, distinct walks; own finding row
  and plan).
- The `rematch`/total family (AUD-QRY-4's scope).
- Repair tooling for corrupt idx files (operator runs `reindex`).
- Dead-state cleanup of `hbk_alloc_failed` (declared 4234, set 4243,
  never read — drive-by candidate for a cleanup branch, not this one).

### Review log

- Round 1 (blind reviewer, 2026-11-20): **2 blockers, 3 majors, 5
  minors, all folded in.** B1: the plan's 500-row fixture would take
  the SERIAL fallback (parallel gate 6543-6546 needs `n_idx_g >= 4 &&
  live >= 100000`), whose own fail-open at 6801 the plan never
  mentioned — fixed: serial Pass-1 joined to the fix; fixture stays
  500 rows. B2: Task 0a's eager/lazy truth is mixed (eager on
  add-index/reindex, index.c:2744+; LAZY on create-object-declared
  indexes, btree.c:618 O_CREAT), so a DELETED file yields ENOENT =
  legitimate empty under the correct template and the test could never
  turn green — fixed: stat-first template per the repo's own
  `agg_walk_worker` precedent (2374-2392) + chmod-000 fault on an
  existing file. M3: "Pass-1 is the one fail-open outlier" was false —
  Pass-2 (6954-6956) and sec_map_build_worker (4172-4177) enumerated
  and DESCOPED to the new AUD-QRY-9. M4: the errno-based ENOENT split
  is fragile (btree.c's logging clobbers errno via `log_msg_sub`,
  config.c:228-247) — stat-first instead. M5: raw errno propagation
  renders EACCES as the misleading "invalid typed record" — normalize
  to EIO. Minors: splits pinned (index_splits_for(8) = 2 — `003.idx`
  would not exist; splits=64 → 8 files); `hbk_alloc_failed` is dead
  state (no consumer to mirror; consumed signals are `aborted` 6581 /
  `budget_exceeded` 6582 / `sw[s].err` 6482); the `aborted`-means-
  fallback semantics documented and the error-vs-fallback choice made
  explicit; agg_error-check-first ordering noted; PR-body claim
  conditioned on the serial route. Reviewer independently verified:
  all contrast sites, the parallel gate, Task 0c/d fixture APIs
  (`test_env_stop_keep` fixtures.h:130 + `test_env_start_at` :72,
  product-case precedent test_crash_safety.c), no-startup-repair
  (embedded.c:1167-1197, 970/1209-1216; server.c:3067-3072; btree.c:911),
  `agg_report_error`'s errno map, and gate compliance.
- Round 2 (blind reviewer, 2026-11-20): **0 blockers, 1 major,
  3 minors, all folded in.** MAJOR: Task 2.3/Task 0b prescribed the
  serial-route propagation against a nonexistent local — the plan's
  `err = EIO` ("the same one agg_walk_worker uses") is false on both
  counts (agg_walk_worker signals via its `a->err` FIELD at 2365,
  consumed at 2418+; cmd_aggregate_do's IGB block has no `err` local
  — the only `int err` is block-scoped in the non-IGB walk branch at
  4854/4861), so the pin would have halted a faithful executor; and
  the plan's goto-igb_skip hedge, while compiling, skips the
  aborted-cleanup at 6906-6914 that frees hbk/crit_ks/sec_maps — a
  silent leak whenever those are live (invisible to this plan's
  count-only test shape, so LSan stays quiet while the leak ships).
  Fixed: serial route = `ctx.agg_error = EIO; aborted = 1; break;`
  per the in-loop precedent at 6854; hedge deleted; Task 0b
  re-pinned. Minors: errno-clobber mechanism corrected (log_msg_sub's
  level filter is FIRST at config.c:229; the clobbering
  time/localtime_r/strftime calls at 237-240 run only on EMITTED
  messages — operative conclusion, stat-first, unchanged); base-RED
  description corrected (buckets present-but-undercounted, not
  missing — 5 values over 8 idx shards put entries in 003.idx for all
  labels); sanitizer once-each justification now explicitly tied to
  the cleanup-covered break form. Reviewer re-verified fresh: both
  fail-open sites, the parallel gate, post-join consumption
  (6580-6583), error-first emission (7007-7011), the stat-first
  template, lazy/eager split, no-negative-entry restore, the
  bitmap-fast-path non-interaction for this fixture, and that the
  round-1 fixes were genuinely folded (both routes, chmod fault,
  splits=64, EIO text, AUD-QRY-9 descope, aborted semantics).

---

## Plan (executor-facing)

### Ground rules

Branch base per the header; the base must contain `src/test/audit/` for
the Task 4 differential spot-check (else PLAN_NOTES + halt). Work
uncommitted; path-limited stashes only; build/test per AGENTS;
`PLAN_NOTES.md` + halt on mismatch (Task 0's pins especially); one
heavy job at a time; `/tmp/shard-db-*` removed after every run.

### Task 0 — pin the facts (code-read, cheap; halt-on-mismatch)

a. Confirm the lazy/eager split: create-object-declared indexes create
   idx files lazily (index.c:105-113 → btree.c:618 O_CREAT);
   add-index/reindex builds eagerly (index.c:2744+). Confirm
   `agg_walk_worker`'s stat-then-open template at 2374-2392 — the
   fix mirrors it.
b. Confirm the post-join consumption sites: `workers[s].aborted`
   (6581), `workers[s].local.budget_exceeded` (6582), the serial
   loop's fail-open `if (!it) continue;` (6801), and `ctx.agg_error`
   checked first at 7007-7010. Confirm the serial block has NO `err`
   local in scope (round-2: the only `int err` is block-scoped in the
   non-IGB walk branch at 4854/4861; `agg_walk_worker` uses its
   `a->err` field) and that the correct serial-route propagation is
   the 6854-precedent form `ctx.agg_error = EIO; aborted = 1; break;`
   routing through the 6906-6914 cleanup.
c. Confirm the product-case restart API: `test_env_stop_keep`
   (fixtures.h:130) + `test_env_start_at` (fixtures.h:72) —
   product-case precedent test_crash_safety.c.
d. Confirm no startup path recreates a plain `.idx`
   (embedded.c:1167-1197 BTRI stamp-skipped, 970/1209-1216 bmfix
   bitmap-only; warmup server.c:3067-3072 read-only; bt_cache no
   negative entry btree.c:911) — and that chmod 000 denies the
   same-UID daemon (EACCES).

### Task 1 — regression test (test-first; expect RED on base)

New file `src/test/cases/test_igb_pass1_open_fail.c`, registered
`TEST_REGISTER("test-igb-pass1-open-fail", ...)`, wired into `build.sh`
next to the aggregate cases (build.sh:233 anchor). Fixture: objects
`pf_plain` (no indexes, oracle) and `pf_ix` — fields `v:varchar:16`
(5 values `cat_0..cat_4`), `t:long`, `u:int`; **`splits: 64`** on
`pf_ix` (index_splits_for(64) = 8 → files 000..007 exist; the faulted
file `003.idx` must exist), `indexes: ["v","t"]`; 500 rows
(`i % 5`).

Steps: start daemon, bulk-insert both, baseline no-criteria
`group_by=v` on `pf_ix` matches `pf_plain` (sanity); stop daemon
(keep root per Task 0c); `chmod 000` `<root>/default/pf_ix/indexes/v/003.idx`;
restart the daemon on the same root; then:

1. `aggregate group_by=v` no-criteria on `pf_ix` → assert the response
   is an ERROR object whose text matches the EIO emission (pin the
   exact substring — expected `aggregate storage access failed` — from
   the actual post-fix response). RED on base: response is a normal
   group list whose per-label totals are UNDERCOUNTED (shard 003's
   entries are skipped — with 5 values hash-routed over 8 idx shards,
   003.idx almost certainly holds entries for ALL 5 labels, so the
   buckets are present-but-short, not missing) — mismatch vs
   `pf_plain`.
2. Same request on `pf_plain` → unchanged correct (control; no index
   files to fail).
3. `count` on `pf_ix` == 500 (control; the defect is group_by-specific).
4. `chmod 644` the file, repeat the request → matches `pf_plain`
   again (control proving the fault, not the data, caused it; no
   restart needed — bt_cache installs no negative entry).

Test-first proof: assertion 1 RED (missing groups, no error), 2-4
GREEN. Paste.

### Task 2 — the fix

In `src/db/query_aggregate.c`:

1. `IgbPass1Worker` gains `int open_errno;` (zero-init at the worker
   array setup).
2. **Worker route** (`igb_pass1_worker`, 4248-4256): stat-then-open —
   ```c
   struct stat st;
   if (stat(idx_path, &st) != 0 && errno == ENOENT) {
       /* AUD-QRY-7: lazy-created index shards (btree.c:618) may not
          exist for shards never written — an empty shard, same as
          agg_walk_worker's template (2374-2392). */
       return NULL;
   }
   BtRangeIter *it = btree_range_iter_open(...);
   if (!it) {
       LOG_ERROR(LOG_SUB_QUERY, "igb_pass1_worker: btree_range_iter_open %s failed", idx_path);
       w->open_errno = EIO;   /* AUD-QRY-7: fail closed — see the plan;
                                 EIO renders as "aggregate storage access failed" */
       w->aborted = 1;        /* note: aborted alone would fall back to
                                 agg_run_plan; agg_error (checked first,
                                 7007) errors instead — deliberate */
       return NULL;
   }
   ```
   (errno cannot be trusted after the open failure — btree.c's logging
   clobbers it; hence stat-first + EIO normalization.)
3. **Serial route** (the `run_serial` Pass-1 loop, 6799-6801): same
   stat-first; on open failure of an existing file:
   ```c
   /* AUD-QRY-7: fail closed — EIO renders as "aggregate storage
      access failed". aborted = 1 routes through the block's
      aborted-cleanup (6906-6914, frees hbk/crit_ks/sec_maps) and
      ctx.agg_error (checked first, 7007) turns the fallback into
      the error object. Mirrors the in-loop precedent at 6854.
      A bare goto igb_skip here would SKIP that cleanup and leak. */
   ctx.agg_error = EIO;
   aborted = 1;
   break;
   ```
   (No `err` local exists in this block — round-2 verified; do not
   invent one.)
4. Post-join: no new consumer needed if `aborted` already routes to
   error via agg_error; if `ctx.agg_error` is only set from specific
   fields today, set it from `w->open_errno` first-non-zero-wins at
   the 6580-6583 site (mirror Task 0b's findings; cite AUD-QRY-7).
5. Update the comment above the open call in BOTH routes: stat-first
   rationale, the ENOENT-empty contract, the fail-closed-on-existing
   contract.

### Task 3 — prove and gate

1. Revert-prove-paste: `git stash push -- src/db/query_aggregate.c` →
   rebuild → Task 1 red → `git stash pop` → rebuild → greens. Paste.
2. Targeted case + full suite `run-all --jobs 8` fresh.
3. Sanitizer gate once each (asan `--jobs 6`, tsan `--jobs 3`); rebuild
   normal.
4. Differential spot-check — no delta expected (healthy objects only);
   record DIVSEC.
5. Changelog convention check + entry if per-fix (behavior change —
   likely warranted).
6. Index: AUD-QRY-7 → `fix-branch` with this branch name; add the
   AUD-QRY-9 row (Pass-2 6954-6956 + sec_map_build_worker 4172-4177
   fail-opens, same class, own plan) if not already present.

### Embedded execution rules

Tasks in order; both revert outputs pasted; no unrelated changes;
`PLAN_NOTES.md` + halt on mismatch (Task 0a-d surprises especially).
