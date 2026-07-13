# Auto-reshard — design

## Problem

`splits` (kf shard count) is fixed at `create-object` time and only
changeable via `vacuum --splits=N`, a manual, full-rehash rebuild. Most
users pick the default (`DEFAULT_SPLITS = 8`) or a value sized for the
object's expected record count at creation time, per
`docs/operations/tuning.md`'s sizing table. Objects that grow well past
what their `splits` was sized for silently degrade — each kf shard's
in-place auto-reshard only kicks in per-shard at 75% slot fill up to
`SLOTCASK_MAX_SLOTS_PER_SHARD = 16M` slots, so an under-split object just
grows its shard files instead of gaining more of them. An operator has to
know to compare their object's live record count against
`docs/operations/tuning.md`'s sizing table and remember to run
`vacuum --splits=N` by hand — nothing does that comparison automatically
today.

This feature automates that: a nightly maintenance job that inspects
every object's live record count against its current `splits`, and runs
`vacuum --splits=N` (jumping straight to the documented target) for any
object that has outgrown its shard count. Users who already size `splits`
correctly up front (known mass-insert use cases) see this job do nothing
— it never *lowers* splits and only acts once an object is genuinely
under-split for its current size.

## Scope

New nightly thread in the daemon (`src/db/server.c`), a config-driven
schedule gate, and reuse of the existing `cmd_vacuum(..., new_splits)` /
`rebuild_object` machinery — no changes to the rebuild/rename mechanism
itself (that's the kfcache/segcache staleness fix, already merged to
`main` as of `0731cdd`).

Out of scope: per-object opt-out (global `db.env` toggle only, v1),
shrinking splits, any change to `is_valid_splits`'s power-of-2 set, and
any change to `docs/operations/tuning.md`'s existing bands other than
inserting the new 1B–5B row described below.

## Trigger

**Single check, no per-shard signal.** This feature's job is "the
object's total volume outgrew what its `splits` was sized for" — that's
a total-live-count question, not a skew question. `shard-stats`'s
per-shard `total`-crossing-1M hint exists for a different purpose
(flagging *skewed* key distribution for an operator to investigate
manually); it's not reused here.

(A per-shard-max trigger was considered and dropped: while reviewing it,
we also found `cmd_shard_stats`'s existing hint text doesn't actually
match its own doc — the code checks an *average* records/shard, not a
per-shard max, despite `docs/operations/tuning.md` claiming otherwise.
Fixing that to check a per-shard max would still be the wrong signal,
though, since this feature's trigger is total-live-count-only — so
`cmd_shard_stats` is instead made to call this feature's own
`reshard_target_for_count()` lookup, giving both the manual diagnostic
and the automatic sweep the same answer. See the implementation plan's
Task 6.)

For each object, once nightly:

- `live = get_live_count(db_root, object)`.
- `target = reshard_target_for_count(live)` (see "Target splits" below).
- If `target > sch.splits`, the object is eligible for reshard — run
  `vacuum --splits=target`. Otherwise, no-op.

Trigger and target selection are the same lookup — there's no separate
"is this object eligible" check distinct from "what should it become."

## Target splits

Replace with a literal lookup table matching `docs/operations/tuning.md`,
keyed by the object's current live record count (`get_live_count`),
extended with a new 2048 band for the 1B–5B range (approved this session
— at its own 5B boundary, 2048 shards reaches ~2.4M records/shard
(12.8M / 2.44M ≈ 5.2×), the same ~5.2× headroom the existing table
already accepts at the 10B/4096 boundary (12.8M / 2.44M, since
10B/4096 = 2.44M too), and halving shard-file count vs. jumping straight
to 4096 measurably reduces `FCACHE_MAX`/kfcache pressure on other objects
sharing the cache budget):

| Live records       | Target `splits` |
|---------------------|-----------------|
| up to 1M             | 8   |
| 1M–10M               | 16  |
| 10M–50M              | 64  |
| 50M–200M             | 256 |
| 200M–1B              | 1024 |
| 1B–5B                 | 2048 (new) |
| 5B–10B                | 4096 |
| 10B+                  | 4096 (no further auto action — doc already recommends partitioning across objects at this point) |

Lookup returns the smallest band's target whose lower bound the live
count has crossed. If the looked-up target is `<= sch.splits` (object
already at or above its recommended band, including the intentional
1024→4096 gap and now 2048 sitting between them), it's a no-op — the
trigger above (`target > sch.splits`, from the same lookup) is what
actually gates whether we act, this table only decides *where to jump
to*. Every value in this
table is already accepted by `is_valid_splits` (`{16, 32, 64, 128, 256,
512, 1024, 2048, 4096}`) — confirmed by reading
`src/db/query_find.c`'s `rebuild_object`; no new validation needed.

**Doc update included in this feature**: `docs/operations/tuning.md`'s
"Recommended `splits` by record count" table gets the same new 1B–5B /
5B–10B split, so the shipped behavior and the published guidance stay in
sync.

## Growth step

Jump directly to the looked-up target in a single `vacuum --splits=N`
call — no incremental doubling. `rebuild_object`/`rebuild_object_v2`
already does a single full rehash regardless of how big the jump is, so
doubling repeatedly would just mean repeated full rebuilds for no benefit.

## Scheduling

No existing cron/hour-of-day precedent exists anywhere in this codebase
(confirmed by prior research) — `auto_vacuum_thread()` is a plain
interval/sleep-loop, not wall-clock-gated. This feature introduces the
first wall-clock-gated background job:

- New detached pthread `auto_reshard_thread()`, started alongside
  `auto_vacuum_thread()` near `src/db/server.c:~3409-3421`, structured
  the same way (1-second sleep-increment loop checking
  `server_running`).
- Each iteration: `localtime_r` the current time; if `tm_hour ==
  AUTO_RESHARD_HOUR` (config, default `3` — low-usage default, override
  in `db.env`) and today's date (`YYYY-MM-DD`) differs from an in-memory
  `last_run_date` guard, run the sweep once, then set `last_run_date` to
  today.
- The in-memory guard means a restart during the trigger hour can re-run
  the same night's sweep — acceptable, since re-checking an object
  already at its target `splits` is a cheap `get_live_count` + table
  lookup, not a rebuild.

## Sweep behavior

Strictly serial, one object at a time, no time-box — consistent with
`auto_vacuum_thread`'s own approach and the answer already given during
brainstorming (no evidence a low-usage window is meaningfully time-boxed
today, and serial execution avoids concurrent rebuilds competing for the
same `FCACHE_MAX` budget and I/O bandwidth). For each `(dir, object)`
pair from the existing tenant/object enumeration (the same walk
`auto_vacuum_thread` already does):

1. Load schema; `live = get_live_count(db_root, object)`.
2. `target = reshard_target_for_count(live)`. If `target <= sch.splits`:
   skip (already sized correctly, or the doc's table doesn't recommend
   moving yet — e.g. an object sitting at 4096 already, or one that
   jumped straight to a high `splits` at creation time).
3. Otherwise: log a `LOG_WARN(LOG_SUB_VACUUM, "auto-reshard %s/%s: starting %d -> %d splits (live=%lld) — object locked for the duration", dir, object, sch.splits, target, live)` **before** calling — `vacuum --splits` holds the object's exclusive objlock for the full rehash (unlike plain vacuum's cheap in-place flag-flip), so reads/writes to this object block until it completes. This is exactly why `auto_vacuum_thread` itself documents "NEVER auto-runs --compact or --splits" for its own polling cadence; auto-reshard is a deliberate, opt-in exception to that rule, gated to a low-usage hour, so the blocking window should be logged loudly rather than silently absorbed.
4. Call `cmd_vacuum(db_root, object, /*compact=*/0, target)`.
5. On success: `LOG_INFO(LOG_SUB_VACUUM, "auto-reshard %s/%s: %d -> %d splits done (live=%lld)", dir, object, sch.splits, target, live)` — reusing the existing `LOG_SUB_VACUUM` tag (`src/db/log.h:39`), the same one `cmd_vacuum`'s own call sites use.
6. On failure: `LOG_ERROR(LOG_SUB_VACUUM, "auto-reshard %s/%s: vacuum --splits=%d failed", dir, object, target)`, then
   continue to the next object — do not abort the sweep. A failed object
   is retried automatically the next night it still qualifies (state is
   derived fresh from disk each run, nothing persisted about failures).

## Configuration

New `db.env` keys, parsed in `src/db/config.c` alongside the existing
`AUTO_VACUUM*` keys:

- `AUTO_RESHARD_ENABLE` (default `0` — opt-in, matching `AUTO_VACUUM`'s
  own default-off precedent; this repo already ships one opt-in nightly
  maintenance toggle and there's no stated reason for this one to default
  differently).
- `AUTO_RESHARD_HOUR` (default `3`, valid range `0-23`; the hour of day,
  server-local time, the sweep is allowed to run in).

## Error handling

Same posture as `auto_vacuum_thread`: per-object failures are logged and
skipped, never crash the thread or block other objects. No retry within
the same night's sweep — the next qualifying night's sweep re-derives
eligibility from live disk state, so a transient failure (e.g. disk full,
object locked by a concurrent operation) self-heals on the next run
without any special-cased retry logic.

## Testing

`reshard_target_for_count(long long live)` is a pure function (the
lookup table, no I/O) — unit-tested directly against every band boundary
(0, 1M-1, 1M, 10M-1, 10M, ... 10B, 10B+1), no daemon required.

The scheduling/plumbing gets a `test_auto_vacuum.c`-style integration
test: spawn a real daemon with `AUTO_RESHARD_ENABLE=1` and
`AUTO_RESHARD_HOUR` set to the *current* server-local hour (computed by
the test itself at run time), so the thread's first wall-clock check
matches immediately once it runs — no multi-hour wait needed, unlike
`test_auto_vacuum.c`'s 90s interval-floor sleep. The thread itself waits
a fixed 5s after startup before that first check (see the implementation
plan's Task 3) — otherwise its once-per-calendar-day `last_run_date`
guard could let the very first tick land before the test (or a real
daemon's own startup sequence) has finished setting up, scan nothing
eligible, and then refuse to check again until the next day. `get_live_count` sums
each kf shard's `total - deleted` straight from the on-disk
`SlotcaskKfHeader` (`resolve_counts` in `storage.c`) — the same technique
`test_kfcache_staleness.c`/`test_segcache_staleness.c` already use to
fabricate cache state applies here: create a real (small, `splits=8`)
object via the normal JSON API, then directly `pwrite` fabricated
`total`/`deleted` values into its kf shard headers so `get_live_count`
reports a value inside the 1M–10M band (target=16) without doing a
single real insert. Assert `splits` changed from 8 to 16 (via
`describe-object`) after the sweep runs. A second object left at a live
count inside its already-correct band asserts *no* change (no-op path).

## Deliverables

1. This design doc.
2. Implementation plan at `docs/plans/2026-07-13-auto-reshard.md`, handed
   to the human for approval before any code changes. Execution happens
   on a fresh branch off `main`, by a non-Claude model, per this repo's
   standing execution-mode exception in `CLAUDE.md`.
3. `docs/operations/tuning.md` table update (1B–5B / 5B–10B split) as
   part of the same plan, not a separate follow-up.

## Out of scope

- Per-object opt-out (global `db.env` toggle only, v1).
- Shrinking `splits` (this feature only ever grows).
- Any incremental/doubling growth step (always a single jump to target).
- Enforcing a hard time-box on the nightly sweep.
- Changing `is_valid_splits`'s accepted set or `rebuild_object`'s
  rehash mechanism itself.
