# Storage durability & recovery — findings to investigate

Status: **notes only, not an approved plan**. Captured during an `/own-your-app`
walkthrough of `slotcask.c` (the core storage engine). Neither item has a
regression test or a root-cause confirmation yet — each needs its own
CORE-PROCESS plan (diagnostic task first, then fix task) before any code
changes land.

## Finding 1 — `rebuild-kf` can silently revert a key to a stale value

**Where**: `slotcask_rebuild_kf`, `src/db/slotcask.c:6569-6678`.

**Mechanism**: the function walks every stream's segment files in
**ascending** file-ID order (`qsort(fids, ..., cmp_fid_asc)`, line 6599),
then linearly by offset within each file. For every `flag == 1` (live)
record found, it repoints the matching kf entry to that location —
including overwriting a repoint made earlier in the *same pass* if a later
scan finds another live record with the same hash at a different location
(lines 6646-6657). There is no timestamp, sequence number, or generation
counter in the 24-byte segment record header (`hash[16] + klen[2] + flag[1]
+ reserved[1] + vlen[4]`) to break a tie between two live copies of the
same key — the scan order *is* the only tiebreaker, and it assumes
"higher file-ID/offset = newer," which free-slot reuse does not guarantee.

**How the bad state arises**: an update (`slotcask_upsert_with_hooks`,
upgrade-to-update branch, `slotcask.c:3240` onward) writes the new value
into a slot popped from the per-stream free pool — which can be *any*
previously-freed slot, including one in a **lower-numbered** file than the
one holding the current value. The order is: write new value → atomically
repoint kf (`kf_repoint_at_slot`) → tombstone old value. If the process is
killed in the window after repoint but before tombstone, the old copy is
left with `flag == 1` in a **higher-numbered** file than the new,
already-correct copy.

**Failure**: an operator later runs `rebuild-kf` (its stated purpose per
`embedded.c:170`: "repair kf entries corrupted by a prior buggy release").
Scanning low-to-high, it finds the new value first and repoints correctly
— then continues, finds the orphaned old value in the higher-numbered
file, and repoints *again* because the location "differs," silently
reverting the key to stale data. No error, no warning.

**Why it matters**: this is the disaster-recovery tool corrupting an
already-correct database — worse than doing nothing — in exactly the
layer that must not break.

**Proposed investigation** (reuses an existing test pattern —
`test_coverity_disk_corruption_segments.c` — daemon up, insert real data,
daemon down, hand-edit specific on-disk bytes with `pwrite`, daemon up,
assert on behavior):

1. Start a daemon, create an object, insert key `K` = `V1`.
2. Force `V1` into a segment file that will end up **higher-numbered**
   than wherever `V2` will land (e.g. insert/delete enough filler records
   first to advance the active append file, or pre-seed the free pool with
   a low-fid slot of the right size bucket).
3. Update `K` to `V2` (normal path — repoint + tombstone happens).
4. Stop the daemon. Directly `pwrite` the old `V1` slot's flag byte back
   to `1` (live) to simulate "crashed before tombstone."
5. Restart the daemon, invoke `rebuild-kf` on the object.
6. Assert `get(K)` still returns `V2`. **Expected on current code: fails,
   returns `V1`** — that's the regression-test proof CORE-PROCESS requires
   before touching the fix.

**Proposed fix direction** (smallest viable, no format change): scan
files/offsets in **descending** order instead of ascending, and change the
repoint condition from "repoint whenever location differs" to "repoint
only if this kf slot doesn't already have a valid entry" — first-match-wins
under descending order favors whichever copy sorts latest. Not a
mathematical guarantee (free-pool reuse could in principle still invert
it), but strictly better than the current ascending scan, which considers
recency not at all. A fully correct fix would need a generation/sequence
number in the on-disk record header — a real format change, only worth it
if step 6 above proves the failure mode is reachable in a realistic
free-pool sizing.

## Finding 2 — No periodic durability sync; writes ack before disk

**Where**: every regular-write `msync` call in `slotcask.c` uses
`MS_ASYNC` (non-blocking — just marks pages dirty, kernel writeback
handles the rest on its own schedule). `MS_SYNC` (blocking, waits for the
write to reach the device) only appears at structural moments: kf
resplit/rehash completion (`slotcask.c:1189`), object close/shutdown
(`storage.c:100`), b+tree page eviction under memory pressure
(`btree.c:397`), and post-rename+parent-dir-fsync during resharding
(`slotcask.c:1205`). There is no per-write or periodic sync knob today —
`db.env` has nothing sync-related.

**Consequence**: a client is told a write succeeded the moment the mmap
store completes in RAM, before the kernel has necessarily flushed it to
disk. On power loss / OOM-kill, the most recent acknowledged writes can be
gone — not corrupted, just as if they'd never happened (read-time hash+key
validation, per Chapter 2 of the walkthrough, prevents this from ever
surfacing as *wrong* data — only as "not found" — but data can still be
lost). Default Linux writeback interval bounds this to roughly ~30s
worst-case with no other pressure; `sync_file_range` nudges after bulk
operations can shrink it opportunistically but don't bound it.

**Direction agreed with the app owner**: not a per-write `MS_SYNC` (would
serialize every write on physical I/O completion — this engine has no
sequential-WAL/group-commit trick the way Postgres does, so a per-write
sync here is a blunter, more expensive instrument than Postgres's
`fsync`-per-commit). Instead: a **periodic sweep** (target: every ~1s,
configurable) that `msync(MS_SYNC)`s currently-dirty kf/segment cache
entries — bounding potential loss to "at most ~1s of writes" instead of
"at most ~30s," at a much smaller throughput cost than per-write sync.

**`MS_ASYNC` should go away entirely, not just move to the sweep**:
confirmed via `man 2 msync` — *"Since Linux 2.6.19, MS_ASYNC is in fact a
no-op, since the kernel properly tracks dirty pages and flushes them to
storage as necessary."* Every current `MS_ASYNC` call on the real
write-hot-path caches (`slotcask.c:209, 260` in `kfcache_drop_slot`/
eviction, `slotcask.c:683, 706` in the segcache equivalents) is therefore
a wasted syscall on Linux today — it does nothing beyond what the kernel
was already going to do on its own writeback schedule. It isn't purely
dead on every platform this engine targets, though — shard-db also ships
on macOS (Apple Silicon, 2026.05.4+, per this repo's CLAUDE.md), and
Darwin's `msync(2)` does not document the same no-op guarantee for
`MS_ASYNC`. That doesn't change the fix direction: the new periodic
`MS_SYNC` sweep supersedes `MS_ASYNC` on **both** platforms — on Linux
because `MS_ASYNC` was never doing anything, on macOS because a bounded
periodic *blocking* sync is a strictly stronger durability guarantee than
an unspecified async hint, whatever it does there. So the fix removes
every `MS_ASYNC` call on these caches outright rather than keeping it "for
portability."

**The per-entry dirty tracking this sweep needs doesn't exist yet, and it
targets a different cache than I first thought — checked, not assumed,
and self-corrected once**: my first pass here traced `UCacheEntry`
(`types.h:903-905`, `storage.c`) — it has exactly the shape this fix
wants (`slot_bits`, `max_dirty_slot`, `dirty`, plus a `storage.c:40`
comment reading "Bulk insert uses slot_bits/dirty for fast activation
pass"). But `UCacheEntry`'s owning subsystem (`ucache_ensure`,
`storage.c:190`, `static`) is never called from the live write path —
its only outside callers are `fcache_invalidate` from schema-mutation and
vacuum code (`query_schema.c:1163,1438`, `query_maint.c:640,899`), not
`slotcask_put`/`slotcask_get`. The actual write-hot-path caches are a
**separate** pair: `KfCacheEntry` and `SegCacheEntry`
(`shard_db_internal.h:31-57`), reached via `kfcache_acquire`/
`segcache_acquire` in `slotcask.c` — this is what `seg_write_record`
(`slotcask.c:1849`) and the kf write path actually use for every real
insert/update/delete. Checked both structs directly: **neither has a
dirty field of any kind** — not dead, not wired up, not present at all.
So `UCacheEntry.dirty` (confirmed still never set to a true value nor
read anywhere, only ever reset to `0`/`-1`) is dead scaffolding on a cache
that's beside the point for this fix, not the mechanism to reuse or wire
up. The real gap is bigger than "finish an unfinished field" — it's "add
a dirty field to the two structs that don't have one yet."

**Confirmed separately, since it matters for how safe the current gap
already is**: on a clean shutdown, both real caches already do the full,
unconditional sweep this fix generalizes — `kfcache_shutdown()`
(`slotcask.c:166-181`) and `segcache_shutdown()` (`slotcask.c:637-652`)
each walk every live entry and `msync(MS_SYNC)` before `munmap`/`close`,
regardless of a dirty flag (there isn't one to check). A graceful
shutdown already blocks until everything is flushed — the gap this
finding is about is strictly the in-between window while the process is
running, between writes and the next shutdown or structural sync.

**Proposed design** (matches what you described — periodic sweep, own
dirty-tracking struct, checks which entries actually changed, single
configurable interval in `db.env`, in milliseconds):

- Add `_Atomic int dirty;` to both `KfCacheEntry` and `SegCacheEntry`
  (`shard_db_internal.h`). Set it to `1` at each real write site under the
  rdlock the writer already holds — `seg_write_record`/
  `seg_write_record_varlen`/`seg_write_flag`/tombstone paths for segcache,
  the equivalent kf-write call sites for kfcache. Existing comment at
  `slotcask.c:1857-1860` confirms writers already take only `rdlock`
  ("each caller owns a unique reserved offset... concurrent writes don't
  race; the rwlock only serialises us against eviction, which takes
  wrlock") — a new sweep thread taking the same `rdlock` before `msync`
  fits that exact model: it neither blocks nor is blocked by concurrent
  writers, only mutually excludes with eviction/close.
- A new background thread, shaped like `auto_vacuum_thread`
  (`server.c:2973`): wakes every `X` (new `db.env` key, milliseconds —
  e.g. `DURABILITY_SYNC_MS`, exact name for the implementation plan),
  walks `g_kfcache`/`g_segcache`, and for every `used && dirty` entry:
  atomically test-and-clear `dirty` to `0` **before** calling
  `msync(MS_SYNC)` (not after) — so a write landing concurrently with the
  sync re-sets `dirty=1` and gets picked up on the *next* wake rather than
  being silently dropped by a post-sync clear that would race it.
- One interval does double duty as both the wake period and the
  staleness bound — no separate "how long has this been dirty" timestamp
  is needed. Every entry gets checked every wake, so worst-case exposure
  is already bounded by `X` on its own; a second age-threshold knob (the
  way Linux's `dirty_writeback_centisecs`/`dirty_expire_centisecs` pair
  works) only earns its complexity if there's a reason to *delay* syncing
  a freshly-dirtied entry for write-coalescing — no evidence that matters
  at this engine's write pattern, so it's left out unless a real case for
  it shows up later.
- Shutdown's existing unconditional sweep (`kfcache_shutdown`/
  `segcache_shutdown`) stays exactly as-is — a correctness backstop that
  doesn't depend on the dirty flag being right, for the one moment where
  the cost of checking first doesn't matter anyway.

**Considered alternative: delegate to kernel writeback tuning
(`vm.dirty_writeback_centisecs` / `vm.dirty_expire_centisecs`) instead of
building our own dirty-tracking + sweep** — rejected. These sysctls do
bound how long a dirty page can sit before the kernel's own flusher
writes it back, and they *would* apply to our mmap'd kf/segment/index
pages same as any other dirty page. But:

- **Scope is the whole machine, not this engine.** They're global
  `/proc/sys/vm/*` knobs — turning `dirty_expire_centisecs` down to bound
  *our* durability window also forces every other dirty page on the box
  (unrelated processes, the OS's own log/temp writes) to flush on the same
  aggressive schedule. On a shared or multi-tenant host that's a
  side-effect on workloads that have nothing to do with shard-db, for a
  guarantee we only need for our own files.
- **Requires root and lives outside the app entirely.** `sudo sysctl -w`
  is a host-level change, not something `db.env` can express or this
  binary can set for itself — it becomes a separate, easy-to-forget
  deployment step (and this repo's own deploy model is explicitly
  "ship built artifacts only, restart the daemon," never touching host
  config — see CLAUDE.md's Deployment section). It also silently
  reverts to distro defaults on any host that isn't provisioned to match,
  with no error from shard-db to say so.
- **No completion signal.** The kernel flusher runs on its own schedule
  with no callback the app can observe — shard-db has no way to know
  "did file X actually get flushed yet," only "the kernel promises to get
  to it eventually." `msync(MS_SYNC)` is a blocking call that returns
  only once data is confirmed on stable storage (or fails with a visible
  errno) — that positive confirmation is the actual property a durability
  fix needs, and no sysctl gives it.
- **Not portable.** These are Linux-only `/proc/sys/vm` knobs; this
  engine also ships on macOS (Apple Silicon, 2026.05.4+), which has no
  equivalent lever. Relying on them would leave macOS back on whatever
  `MS_ASYNC`'s unspecified behavior is there — a platform-inconsistent
  guarantee.
- **Doesn't remove the need for per-entry tracking anyway.** Even with
  the sysctls maxed out, the app still can't tell which cache entries are
  clean and skip syncing them — the kernel sweep is all-dirty-pages, not
  "only shard-db's touched files." The dirty flag's other job — letting
  the sweep skip idle entries instead of `msync`-ing everything every
  wake — is unaffected either way, so the app-level design does strictly
  more than the sysctl approach even where their scope overlaps.

Kernel writeback tuning is a reasonable *complementary* op-level knob for
someone running this on dedicated hardware who wants an extra safety net,
but it can't substitute for the app doing its own bounded, observable,
portable sync — so the design above keeps the explicit dirty flag +
sweep thread + blocking `msync(MS_SYNC)`.

**Scope widened to cover index/bitmap caches too**: `bt_cache`
(`BtCacheEntry`, `btree.c`) and `bm_cache` (`BmCacheEntry`, `bitmap.c`) —
the B+ tree index page cache and the bitmap-index cache — follow the
identical pattern: `MS_ASYNC` on eviction (`btree.c:481`, `bitmap.c:124`),
`MS_SYNC` only at shutdown (`btree.c:397`, `bitmap.c:88`), no dirty field
on either struct. Index data has the same exposure window as kf/segment
data, so the fix should add the same `dirty` field + sweep coverage to
all **four** write-hot-path caches (`KfCacheEntry`, `SegCacheEntry`,
`BtCacheEntry`, `BmCacheEntry`), not just the two the write path most
directly touches. `CountsCacheEntry` (storage.c) is in-memory-only
bookkeeping (live/deleted counters), not a `mmap`, so it has no durability
angle and is out of scope here.

**Aside — resolves what `UCacheEntry` actually is, and it's not just
dead code**: chasing why a seemingly-unused cache (`UCacheEntry`,
`storage.c`, `data/NNN.bin` files) still exists turned up that it isn't
fully dead — `server.c:1394-1420` routes single-key `get` **with a
`fields` projection** through it (`build_shard_path`, a different, older
on-disk layout than `kf_path_for`'s `data/kf/NNN.kf`). Nothing in the
current write path ever populates a `.bin` file (`ucache_get_write`, the
one function that would write one, has zero callers anywhere). Verified
live rather than trusting the static trace — reproduced with a throwaway
test object:

```
$ shard-db insert test widgets k1 '{"name":"hello","qty":5}'
{"status":"inserted","key":"k1"}
$ shard-db get test widgets k1
{"name":"hello","qty":5}
$ shard-db query '{"mode":"get","dir":"test","object":"widgets","key":"k1","fields":["name"]}'
{"error":"Not found"}
```

Single-key `get` with a `fields` projection returns "Not found" for a
record that unambiguously exists, every time. This is unrelated to
durability — it's a plain, reproducible correctness bug, not theoretical
— split out as **Finding 7** below rather than folded into this one,
since its fix (delete this branch, route through the same decode path
plain `get` already uses correctly, then filter fields post-decode) has
nothing to do with sync timing.

**Open scoping question, unresolved**: `auto_vacuum_thread` is only
started in `cmd_server`'s daemon startup path (`server.c:3641`) — **it
does not run in embedded mode.** The stated production deployment (HN
Explorer) runs embedded via the npm package. A periodic thread modeled on
`auto_vacuum_thread` would need separate wiring to also run under
`shard_db_open()` (embedded init, `embedded.c`) — or the design needs to
pick a different mechanism for embedded mode (e.g. an opt-in call the host
app's own event loop can drive periodically, matching how
`shard_db_set_log_handler` hands control back to the embedding process
rather than assuming a background thread). This must be settled before a
real implementation plan is written — it changes where the code goes.

## Finding 3 — stale comment in `objlock.c` (lower priority) [RESOLVED: comment fixed directly, see `src/db/objlock.c`]

**Where**: `src/db/objlock.c:6-8`, the file's top-of-file design comment:

```c
   - Reads (get/find/search/range): do NOT take this lock (MAP_PRIVATE
     gives snapshot isolation)
```

**Problem**: `MAP_PRIVATE` is not used anywhere in the read paths — every
mmap call in `slotcask.c`/`btree.c`/`storage.c` is `MAP_SHARED` (confirmed
by grep across all three files). The real reason reads need no objlock
isn't snapshot isolation from a private mapping — it's that `MAP_SHARED`
gives a live, coherent view, and the hash+key validation in the read
retry-loop (`slotcask_get`, `slotcask.c:2181-2229`) catches the rare case
where a concurrent write moved the value out from under the read, causing
a retry rather than requiring a lock.

**Fix**: comment-only correction, zero behavior change. E.g.:

```c
   - Reads (get/find/search/range): do NOT take this lock (MAP_SHARED
     gives a live view; the read-side retry loop validates hash+key after
     the fact and retries on a concurrent move, so no lock is needed for
     correctness)
```

**Timing**: `optional` — doc accuracy only, no runtime effect either way.

## Finding 4 — NQL dispatch takes an unconditional objlock the JSON path doesn't (high priority)

**Where**: `dispatch_nql_query`, `src/db/server.c:621`, vs. `dispatch_json_query`,
`src/db/server.c:1394-1398`.

**Evidence**: NQL takes `objlock_rdlock(db_root, cmd.obj)` once, unconditionally,
before the `switch (cmd.mode)` (line 621), and releases it after every case
(line 671). JSON instead gates the lock by command type:

```c
/* Per-object locking: wrlock for schema/rebuild, rdlock for writes, none for reads */
took_wrlock = mode_is_schema(mode);
took_rdlock = !took_wrlock && mode_is_write(mode);
```

**Why this is a real asymmetry, not just style**: `NqlMode` (`nql.h:43`) is
`{ NQL_FIND, NQL_COUNT, NQL_AGGREGATE }` — confirmed with the app owner that
NQL has no write modes at all (find/count/aggregate, plus join folded into
find via `cmd.joins`). Every single NQL command is a read. So the
`objlock_rdlock` at line 621 isn't protecting a mixed read/write switch the
way it might look at first glance — it's taking a lock unconditionally for
operations that, per JSON's own policy at the same choke point, should take
none.

**Consequence**: under the writer-preferring rwlock
(`PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP`, `objlock.c`), a pending
`vacuum`/`add-field`/schema wrlock request can make new rdlock acquisitions
queue behind it. An NQL `find` issued in that window blocks until the
pending exclusive lock is granted and released; the JSON-equivalent `find`
on the same object does not, because it never contends for the lock at
all. Same logical operation, different latency behavior depending only on
which wire protocol the client used.

**Verified this isn't a hidden safety requirement**: traced both NQL's and
JSON's read modes down to the same underlying functions — `cmd_count`
(JSON) parses its criteria JSON into a `CriteriaNode` tree and calls
`cmd_count_with_tree`; `cmd_count_tree` (NQL, `server.c:629`) is a thin
wrapper that calls the same `cmd_count_with_tree` directly
(`query.c:5357-5363`). Same convergence holds for find (`cmd_find` →
shared tree-walking core also reachable as `cmd_find_tree`) and aggregate.
NQL and JSON reads are two parsers over one execution core, with identical
`MAP_SHARED` + read-retry-loop safety properties — there is no NQL-specific
execution path relying on the objlock for anything. Removing NQL's
unconditional rdlock is safe.

**Confirmed fix direction**: matches the user's proposed approach, and
turns out to be even simpler than mirroring JSON's `mode_is_write`/
`mode_is_schema` gating — since 100% of NQL modes are reads, NQL doesn't
need per-mode gating logic at all. The fix is to delete the
`objlock_rdlock`/`objlock_rdunlock` pair at `server.c:621` and `server.c:671`
outright. (If NQL ever grows a write mode in the future, that mode would
need to add its own lock acquisition at that point — but that's a
non-issue today.)

**Timing**: `before production` — not an active corruption/security risk
(both paths are equally correct), but a real, currently-shipping latency
inconsistency between two client-facing protocols doing the same thing.
Small, mechanical fix; still wants its own CORE-PROCESS diagnostic/test
task (a test that starts a pending vacuum wrlock, issues a concurrent NQL
find, and asserts it doesn't block) before landing, per this repo's
process.

## Finding 5 — embedded mode has no per-end-user tenant isolation by default [RESOLVED: no code change — operational guidance only, see below]

**Where**: `shard_db_query`, `src/db/embedded.c:334` — every embedded call
dispatches as:

```c
dispatch_json_query(db->db_root, json, "127.0.0.1");
```

`client_ip` is hardcoded, not passed in by the caller. `is_ip_trusted()`
(`server.c:130-136`) trusts `127.0.0.1`/`::1` by default
(`!g_disable_localhost_trust`), so unless `DISABLE_LOCALHOST_TRUST=1` is
set, **every** embedded query — any `dir`, any `object`, any mode — skips
the token check entirely, regardless of whether the JSON payload includes
an `auth` field or what it contains.

**Why this matters for the stated use case**: this repo's HN Explorer
deploys shard-db embedded via the npm wrapper — single tenant today, which
is why the earlier "untrusted rwx in embedded mode is fine" call was
correct for that app. But the app owner is now considering 2-3 new
projects, and flagged the case where **a future embedded app has multiple
end-users, each of whom should be confined to one tenant `dir`**. As shipped,
the engine provides nothing to enforce that — the `shard_db_query(db, json,
out, out_len)` signature (`shard_db.h:21`) has no identity/caller parameter
at all. The only thing that ever decides "which dir can this call touch" is
whatever `dir` string the embedding app's own code happens to put in the
JSON text it builds — the engine will honor it either way.

**The isolation primitive already exists, just isn't default-on**: embedded
mode does load `db.env` from CWD (`embedded.c:78-91`), through the same
`load_db_root()` parser the daemon uses (`config.c:326`), which applies
`DISABLE_LOCALHOST_TRUST` (`config.c:402-403`) along with every other
`g_*`-guarded setting. So the existing scope×permission token model
(`$DB_ROOT/<dir>/tokens.conf`, per this repo's multi-tenancy docs) is
already wired up to work in embedded mode:

1. Set `DISABLE_LOCALHOST_TRUST=1` in the embedded process's `db.env`.
2. Create one token per end-user (or per session), each with a
   `$DB_ROOT/<their-dir>/tokens.conf` entry scoped to that one tenant dir.
3. The embedding app attaches the correct `"auth":"<token>"` field to every
   JSON string it builds for that end-user's requests.

With that in place, a query whose token doesn't match the requested `dir`'s
tokens.conf fails auth the same way it would over TCP — the enforcement
code path is identical, it's only the transport that differs.

**What this does *not* solve**: the embedding app itself is still the
entire trust boundary. `shard_db_query` has no way to independently verify
"this token really belongs to this end-user" — that binding lives entirely
in whatever code the embedding app writes to pick a token per request. A
bug there (wrong token forwarded, admin token hardcoded for convenience,
token omitted and `DISABLE_LOCALHOST_TRUST` left off) silently collapses
back to "every user can reach every dir," and shard-db has no independent
signal to detect that — same shape of risk as an app trusting a client-sent
`user_id` header without verifying a session first.

**Timing**: `keep-as-is` for the current single-tenant app (matches the
earlier "1 app, perms make sense when many apps are connecting" call).
`before production` for any of the 2-3 upcoming projects **if** it embeds
shard-db with multiple end-users who must not see each other's dir — at
that point, wire up `DISABLE_LOCALHOST_TRUST=1` + per-dir tokens deliberately
rather than defaulting to trusted-localhost, and audit the embedding app's
own token-selection code path (not shard-db's) as the real point of
failure.

**Verification, once a multi-tenant embedded project exists**: write a test
that opens two dirs with distinct per-dir tokens, sets
`DISABLE_LOCALHOST_TRUST=1`, and asserts a query carrying dir-A's token
against dir-B returns an auth error — proving the primitive works — then
separately review the embedding app's call sites to confirm the right
token is actually selected per end-user request (that half can't be tested
from inside shard-db; it's the embedding app's own logic).

**Confirmed: no shard-db code change required.** Checked the actual gate,
not just inferred it — `server.c:978-989`'s auth check
(`if (!is_ip_trusted(client_ip)) { ... is_authorized(auth, req_dir,
req_obj, mode) ... }`) runs unconditionally for every mode, and
`embedded.c:121-122` calls the identical `load_tokens_conf(db->db_root)` /
`load_allowed_ips_conf(db->db_root)` the daemon calls at
`server.c:3602-3603`. Nothing here is daemon-only; it's pure configuration
+ call-site discipline in the embedding app: (1) `DISABLE_LOCALHOST_TRUST=1`
in `db.env`, (2) one token per end-user scoped to their dir's
`tokens.conf`, (3) attach `"auth":"<token>"` to every JSON query the
embedding app builds for that user.

**Bootstrap-order caveat** (operational, not a code gap): flipping
`DISABLE_LOCALHOST_TRUST=1` removes the localhost bypass for *admin* calls
too, including `add-token` itself. So at least one admin token must already
exist in `$DB_ROOT/tokens.conf` on disk before `shard_db_open()` runs in
strict mode — either hand-provisioned once up front, or added while still
in trusted-localhost mode before ever setting the flag. After that, further
per-user tokens can be minted at runtime via `add-token` using that admin
token.

## Finding 6 — no test covers JSON escaping at the aggregate group-by call site [RESOLVED: already covered, see below]

**Where**: `query_aggregate.c:830, 852, 5816` (all three call `json_escape_const`
on group-by values).

**Original claim (incorrect)**: this finding, as first written, asserted
no test combined a JSON-metacharacter-bearing `varchar` group-by value
with an over-the-wire aggregate assertion. Re-checking while drafting
this finding's implementation plan found that claim was based on an
incomplete search — it only checked `test_agg_int_groupby_multi.c` and
missed a second test function already living in `test_json_escape.c`.

**What's actually there**: `test_json_escape_agg_file_run`
(`src/test/cases/test_json_escape.c:180-258`), registered as
`test-json-escape-agg-file`, already does exactly the proposed fix — it
seeds a `varchar` `category` field with a quote-bearing value
(`"He said \"hi\""`), then asserts the escaped form
(`\"hi\"`) appears in the wire response for **both** group-by execution
paths:
- the "standard bucket path" (`group_by:["category"]`, no `order_by`,
  hits the general hash-table path ending at `query_aggregate.c:5816`) —
  asserted at `test_json_escape.c:210-216`.
- the "top-N heap path" (`group_by:["category"]` + `order_by` + `limit`,
  hits `agg_run_topn_stream`'s drain loop ending at
  `query_aggregate.c:830/852`) — asserted at `test_json_escape.c:218-224`.

This test function is committed on `main` since commit `797aadd` ("fix:
JSON-escape varchar field values in find/fetch/aggregate/file responses",
2026-06-16) — a month before this audit — so the coverage gap this
finding described does not exist today. No code or test change needed
for the JSON-escaping half of this finding.

**One genuinely-residual, narrower gap**: neither of those two existing
assertions exercises `format:"csv"`. That half of the original proposed
fix (assert CSV output is correctly escaped too) is still open — but only
meaningfully testable for the general hash-table path today, since the
top-N streaming path doesn't support CSV output at all (see Finding 10,
discovered while investigating this finding: `format:"csv"` on a
top-N-eligible aggregate silently returns JSON, unrelated to escaping).
Rather than a separate plan, this residual CSV-escaping assertion is
folded into Finding 10's plan (Task 2's regression test already builds a
CSV-format aggregate request against the same code paths, so it adds the
quote-bearing group-by value there rather than duplicating fixture setup
in a second plan).

**Timing**: n/a — no fix required; doc correction only.

## Finding 7 — single-key `get` with a `fields` projection always returns "Not found"

**Where**: `server.c:1394-1420`, the `mode == "get"` branch when a single
`key` (not `keys`) is given along with a non-empty `fields` array.

**What**: this branch bypasses the normal record-fetch path entirely and
reads directly from a `.bin` shard file (`build_shard_path`,
`storage.c`) using the `ucache`/`UCacheEntry` cache — a different,
older on-disk layout than `kf_path_for`'s `data/kf/NNN.kf`, which is what
every current insert/update/delete actually writes to. Confirmed via
grep that the one function capable of writing a `.bin` file
(`ucache_get_write`) has zero callers anywhere in the codebase — nothing
populates this format today.

**Reproduced live** (not just traced statically — ran the real binary
against a throwaway test object):

```
$ shard-db insert test widgets k1 '{"name":"hello","qty":5}'
{"status":"inserted","key":"k1"}
$ shard-db get test widgets k1
{"name":"hello","qty":5}
$ shard-db query '{"mode":"get","dir":"test","object":"widgets","key":"k1","fields":["name"]}'
{"error":"Not found"}
```

Plain `get` on the same key, same object, same server process, correctly
returns the record. Adding `"fields":["name"]` to the exact same lookup
makes it report the key doesn't exist. This reproduces on every call,
not intermittently — it isn't a race, the code path is structurally
reading from a file that's never written.

**Impact**: any client that uses single-key `get` with a `fields`
projection gets a false "not found" for every existing record, 100% of
the time. Multi-key `get` (the `keys` array form, `cmd_get_multi`) does
not take this branch and is unaffected — confirmed its signature has no
`fields` parameter at all, so it doesn't project either way. `find`/
`fetch` with `fields` go through the normal query-engine path and are
unaffected — only the single-key-`get`-plus-`fields` combination hits
this branch.

**Root cause, confirmed by the app owner**: `UCacheEntry`/`ucache` is the
old **v1** (pre-slotcask, probe-into-slot) data-format cache. It should
have been deleted when v2/slotcask replaced it, and wasn't. That explains
everything found while investigating this: it's not reachable from any
live v2 write, and its one live v2 read caller (`scan_shards`, the
`query.c:5322` fallback) only fires when `slotcask_registry_get` fails to
open the object as v2 — at which point it scans for `.bin` files that
can't exist for a v2 object and silently reports zero matches instead of
surfacing the real failure. The single-key-`get`-plus-`fields` branch is
the one place this dead format still gets *exercised* by a normal
request, which is why it's the one place the bug is visible — the rest
of the v1 cache is just unreachable weight, not actively wrong, because
nothing calls into it under normal v2 operation.

**Decision: remove the v1 cache entirely, not patch around it.** Per the
app owner: this shouldn't be special-cased or left in place for
"compatibility" — v1 objects are already refused at load by this binary,
so there is nothing left for this code to serve. Smallest viable fix
becomes: delete the whole subsystem, and fix the one caller that used it
for something real.

**Full removal surface** (every call site touching `UCacheEntry`/`ucache`,
so an implementation plan doesn't have to rediscover this):

- **Struct**: `UCacheEntry` (`types.h`).
- **`storage.c`** — the entire ucache subsystem: `ucache_shutdown`,
  `ucache_probe`, `ucache_ensure`, `ucache_get_write`,
  `ucache_write_release`, `ucache_nudge_writeback`,
  `ucache_bump_record_count`, `grow_rehash_worker`, `ucache_grow_to`,
  `ucache_grow_shard`, `ucache_peek_slots`, `ucache_slot_count`,
  `ucache_stats`, `ucache_entry`, `fcache_get_read`, `fcache_release`,
  `fcache_invalidate`, `build_shard_filename`, `build_shard_path`.
  (`ucache_get_write` already confirmed to have zero callers — pure dead
  weight even before this decision.)
- **`query_find.c:17-127`** — `scan_one_shard`, `scan_worker`,
  `ScanWorkerArg`, `scan_shards`: the whole v1 mmap-scan path.
- **`query.c:5322`** — the `scan_shards(...)` fallback call in the count
  path. Removing it means the `slotcask_registry_get` failure case needs
  a real error response instead of falling through to a scan that used
  to (harmlessly, if uselessly) return zero — **this is the one place
  removal changes externally-visible behavior**: today a v2-open failure
  here is masked as "0 matches"; after removal it should surface as an
  actual error. Worth deciding deliberately rather than as a side effect.
- **`server.c:1394-1420`** — the broken `get`+`fields` branch itself;
  replaced per the fix below, not merely deleted.
- **`server.c:1024, 1097`** — `ucache_stats(&uc_used, &uc_total,
  &uc_bytes)`, surfaced in the `stats` and `stats-prom` JSON/Prometheus
  output. This is a **documented external protocol surface**
  ([diagnostics.md](docs/query-protocol/diagnostics.md) /
  [stats-prom]) — removing the underlying cache means deciding whether
  those fields disappear from the response (a breaking change for any
  monitoring that scrapes them) or get zeroed/stubbed. Flag for the
  implementation plan rather than deciding here.
- **`query_schema.c:1163, 1438`** and **`query_maint.c:640, 899`** —
  `fcache_invalidate(...)` calls made during schema mutations and
  vacuum. These currently invalidate a cache that (per the above) holds
  nothing live under v2 — confirm each call site is *only* clearing this
  dead cache and not also expected to do something for kfcache/segcache
  under a shared name, before deleting the calls.

**Proposed fix for the `get`+`fields` branch specifically**: replace it
with the same decode path plain `get` already uses correctly (the one
that reads via `kf_path_for` → kfcache → segcache), then filter the
decoded record down to the requested `fields` before serializing —
mirroring whatever `find`/`fetch` already do for their `fields`
parameter, since those are confirmed working.

**Timing**: `fix now` for the `get`+`fields` bug itself — reachable,
reproducible, silent wrong-answer bug on a documented read path. The
full v1-cache removal can ride along in the same change (it's the root
cause, not a separate risk), but the `stats`/`stats-prom` field question
and the `scan_shards` fallback's new error-surfacing behavior are both
small externally-visible decisions that belong in the implementation
plan, not folded in silently.

**Verification**: the repro above, turned into a regression test
(`test-get-single-key-fields-projection` or similar) — insert a record,
`get` with `fields`, assert the actual field values come back rather than
an error. Plus: confirm the full test suite still passes with the v1
cache subsystem gone (nothing else should reference it, per the call-site
inventory above, but the test run is the actual proof).

**Traced lead, resolved — no further v1 code found**: three comments in
`query_maint.c` (lines 203, 782, 858) mention "v1 only" / "v1 still has
the full text-counts file write path below," raising the question of
whether more live v1 branches exist beyond the ucache surface above.
Traced each: `reset_deleted_count` (`storage.c:977-979`, called from
`query_maint.c:203` and `query_find.c:1232`) is an **unconditional
no-op for both v1 and v2** — the comment describing it as "v1 only" is
stale, not a sign of live v1 logic (its sibling mutators
`update_count`/`update_deleted_count`/`set_count`, `storage.c:965-976`,
are the same — kept only "so existing callers... keep their bookkeeping
shape," per the class comment at `storage.c:960-964`). The `cmd_recount`
function below the "v1 still has the full text-counts file write path
below" comment (`query_maint.c:874-887`) has zero v1 branching — pure
v2 kf-header arithmetic. Conclusion: no more v1 storage code to remove
beyond what's listed above — but the three stale comments themselves are
now in scope for this same removal pass, since leaving "v1 only" /
"v1 still has the full text-counts file write path below" in the source
is exactly the kind of misleading trail that made this lead worth
tracing in the first place. Added to the removal surface below:
`query_maint.c:203`'s "v1 only; no-op for v2" trailing comment,
`query_maint.c:782-784`'s "v1 uses INITIAL_SLOTS=256..." comment, and
`query_maint.c:853-858`'s "recount... v1 still has the full text-counts
file write path below" comment — rewrite each to describe only the v2
behavior that's actually there, or delete the v1 clause if the
surrounding sentence doesn't need it.

## Finding 8 — `vacuum` and `recount` don't validate the schema before
use, so both return fake success on a nonexistent or unopenable object

**Where**: `cmd_vacuum` (`query_maint.c:161-206`) and `cmd_recount`
(`query_maint.c:874-887`).

**What**: both call `load_schema(db_root, object)` and use the result
without checking whether it actually succeeded. `load_schema`
(`config.c:696-...`) returns a zeroed `Schema` (`splits=0`) both when the
object doesn't exist and when it's a legacy v1 object it refuses to load
(`config.c:795-807`) — in the latter case it also prints an error to
stderr, but that's invisible to a JSON client and doesn't stop the
caller from proceeding. Confirmed `slotcask_registry_get` safely returns
`NULL` for `info->splits <= 0` (`slotcask.c:5127-5129`) rather than
crashing, so neither function crashes — they just silently continue with
a zeroed schema and a NULL `sdb`.

**Impact**: for a nonexistent or legacy-v1 object,
`cmd_vacuum` returns `{"status":"vacuumed","cleaned":0}` (a **fake
success**, indistinguishable from vacuuming a real, empty object), and
`cmd_recount` returns `{"count":0}` (indistinguishable from a real object
with zero live records). Contrast with `cmd_rebuild_kf`
(`query_maint.c:859-872`), which does check `if (!sch.splits) {
OUT("{\"error\":\"object not found\"}\n"); return 1; }` right after the
same `load_schema` call — the fix pattern already exists in the same
file, it's just not applied consistently to its two neighbors.

**Root cause**: same shape as Finding 7 and the `scan_shards` fallback —
a schema-load failure that isn't surfaced as an error propagates as a
default/zero value instead, and the caller can't tell "genuinely zero"
from "never opened."

**Proposed fix**: add the same `if (!sch.splits) { OUT("{\"error\":...}\n");
return 1; }` guard `cmd_rebuild_kf` already uses, to `cmd_vacuum` and
`cmd_recount`, immediately after their `load_schema` calls.

**Timing**: `before scale` — not a data-loss or security issue (both
operations are otherwise no-ops on an unopenable object), but a
misleading API response that could mask an operator's typo'd object name
or a stuck v1→v2 migration as "nothing to do" rather than "this didn't
run."

**Verification**: call `vacuum`/`recount` against a deliberately
nonexistent object name and confirm the response becomes
`{"error":"object not found"}` instead of a fake zero/success.

## Finding 9 — embedded mode runs none of the daemon's background
maintenance threads (auto-vacuum, auto-reshard, warmup), and there's no
shared owner either mode inherits from

**Where**: `shard_db_open` (`embedded.c:267` onward) vs. `cmd_server`
(`server.c:3630-3681`).

**What**: confirmed via `grep -n "db_thread_create(&" src/db/server.c`
that `cmd_server` spawns exactly four threads/pools, all inline in its own
daemon-startup body, each with its own opt-in gate and its own
locally-scoped arg struct (`AutoVacuumArg` `server.c:2561`, `AutoReshardArg`
`server.c:3058`, `WarmupArg` `server.c:2600`):

- `worker_thread` pool (`server.c:3625`) — daemon-specific by nature
  (services TCP connections), not applicable to embedded's synchronous
  call/response model. Out of scope for this finding.
- `auto_vacuum_thread` (`server.c:3641`, gated `g_auto_vacuum_enable`) —
  periodic space reclamation (Finding 9's original scope, kept above).
- `auto_reshard_thread` (`server.c:3655`, gated `g_auto_reshard_enable`) —
  nightly (once-per-calendar-day, configurable hour) check that grows
  `splits` for objects that have outgrown their current shard count, via
  `vacuum --splits=target`.
- `warmup_thread` (`server.c:3672`-`3681`, gated `g_warmup_mode != "off"`)
  — primes the OS page cache for every kf header + index shard at startup;
  async mode detaches it, sync mode blocks `cmd_server` until it finishes.

Direct grep across `embedded.c` confirms zero `pthread_create`/
`db_thread_create` calls anywhere in the file — none of the three
opt-in threads (vacuum, reshard, warmup) ever run under `shard_db_open`,
regardless of what `db.env` says. Tellingly, `embedded.c:62`'s own
comment reads "(those are started by the caller — cmd_server **or
shard_db_open**)" — the embedded path was evidently meant to start them
too, and structurally never does. `db_defaults_set` (`embedded.c:38-56`)
still initializes every relevant config field
(`auto_vacuum_interval_sec`, `vacuum_recommend_pct`,
`vacuum_recommend_min_deleted`, `auto_reshard_hour`,
`auto_reshard_throttle_ms`, `warmup_mode`) — config for three sweeps that
never run in this mode.

**Impact**: any embedded deployment — stated production case: HN
Explorer, via the npm wrapper — gets, regardless of `db.env` settings:
no automatic space reclamation (unbounded on-disk growth under
delete/update churn, manual `vacuum` only), no automatic reshard-on-growth
(an object that outgrows its `splits` stays there until a manual `vacuum
--splits=N`), and a cold page cache on every embedded-process start (first
queries pay full disk-read latency instead of racing a warmup thread).
None of these are data-loss or correctness bugs — all three degrade
silently, with no error or log line, since each looks configured and
active from `db.env` alone.

**Root cause**: all three threads are wired up exclusively inside
`cmd_server`'s body — there is no shared "background maintenance"
component either startup path calls into; `cmd_server` *is* the only
owner. Embedded mode was built around `shard_db_open()`/
`shard_db_query()`/`shard_db_close()` as a synchronous, thread-free
call/response API, so nothing analogous ever got added when these three
features shipped for the daemon.

**Proposed fix direction (per app owner discussion): centralize, don't
duplicate.** Rather than reimplementing three thread-spawns a second time
inside `embedded.c` (the copy-paste that would let the two paths drift
again the next time a fourth background task is added — e.g. Finding 2's
proposed durability sweep), factor `cmd_server`'s four spawn blocks into
one shared function — e.g. `bg_threads_start(ShardDb *db)` /
`bg_threads_stop(ShardDb *db)` in a new or existing shared TU (`server.c`
itself, or split into its own file if that reads better once Finding 2's
sweep thread is added as a fifth) — that both `cmd_server` and
`shard_db_open`/`shard_db_close` call. Each individual thread keeps its
own existing opt-in gate (`g_auto_vacuum_enable`, `g_auto_reshard_enable`,
`g_warmup_mode`, and whatever Finding 2's sweep uses) so behavior for a
config that leaves them off is unchanged in either mode — centralizing
the *spawn plumbing* doesn't force any thread to become mandatory.

**Two things the plan needs to settle, not left to the executor**:
1. **Does embedded mode get a real background thread, or a host-driven
   call?** A real `pthread_create` inside `shard_db_open` matches the
   daemon's model most directly and needs no embedding-app changes beyond
   config — but a background thread inside a library the host process
   didn't ask for is a bigger behavioral change for existing embedders
   than a periodic sweep is (this is the same fork Finding 2 already
   named: real thread vs. opt-in `shard_db_run_periodic_tasks(db)` driver
   call). Whichever is chosen should be the **same** answer for Finding 2's
   durability sweep and Finding 9's three threads — one mechanism, not
   one per feature.
2. **Shutdown/join semantics.** `cmd_server`'s shutdown sequence joins
   (not detaches) `auto_vac_tid`/`auto_reshard_tid` deliberately (per the
   existing code comments) — `shard_db_close()` needs the equivalent join
   so a centralized thread doesn't outlive the `ShardDb*` it captured a
   pointer/`db_root` string for.

**Relationship to Finding 2**: identical missing infrastructure. Plan
Findings 2 and 9 together as one implementation plan — a single
"embedded background-task infrastructure" change that lands the
centralized `bg_threads_start/stop` mechanism once, then registers
vacuum, reshard, warmup, and the new durability sweep against it, rather
than four uncoordinated one-off fixes.

**Timing**: `before production` for any embedded deployment expected to
run long enough / with enough write churn or growth for reshard/vacuum to
matter (HN Explorer's actual growth profile wasn't measured as part of
this finding — flag for the app owner to assess urgency for that specific
app). Structural gap either way, and it will recur for every future
background feature until the centralization lands.

**Verification**: once `bg_threads_start/stop` exists, a test that opens
an embedded `ShardDb` with auto-vacuum/auto-reshard enabled in its
`db.env`, drives each past its trigger condition (tombstone threshold for
vacuum, `splits` undersizing for reshard), and asserts the same effect a
daemon-mode integration test would assert (disk usage drop, `splits`
increase) — plus a `shard_db_close()` test confirming no thread is left
running/leaked after close. No such test exists today because none of
the three features run in this mode yet.

## Finding 10 — `aggregate` with `format:"csv"` silently returns JSON when
the top-N streaming path is eligible

**Where**: `cmd_aggregate_do` (`query_aggregate.c:3315-3323`) computes
`csv_delim` from the caller's `format`/`delimiter`, but the top-N
streaming dispatch block right below it (`query_aggregate.c:3643-3679`)
calls `eligible_for_topn_stream` (`query_aggregate.c:478-516`) and
`agg_run_topn_stream` (`query_aggregate.c:663-` through the drain loop at
`~815-870`) with no `format`/`delimiter` argument at all — neither
function's signature carries one. The drain loop unconditionally emits
JSON (`OUT("{\"cat\":\"%s\", ...")`-style calls throughout).

**How this surfaced**: while investigating Finding 6 (JSON-escaping test
coverage at the aggregate group-by call sites), traced every call site of
`json_escape_const` in `query_aggregate.c`, which led to comparing the
top-N streaming path against the general hash-table group-by path
(`query_aggregate.c:~5737-5820`, which *does* correctly branch between
`csv_emit_cell` for CSV and `json_escape_const` for JSON at lines 5749 vs
5816). The asymmetry between the two group-by execution paths — one
format-aware, one not — prompted checking whether the top-N path is ever
reachable with `format:"csv"` set.

**Reproduced live** (daemon mode, single indexed varchar `group_by`
field, `count()` aggregate, `order_by` on that count's alias, `limit:10`,
no `having` — exactly `eligible_for_topn_stream`'s admission criteria):

```
--- JSON topn-eligible aggregate ---
[{"cat":"grp0","c":2},{"cat":"grp1","c":3}]
--- CSV topn-eligible aggregate (same shape, "format":"csv" added) ---
[{"cat":"grp0","c":2},{"cat":"grp1","c":3}]
```

Identical output — the `"format":"csv"` request silently got JSON back
instead of CSV rows. No error, no warning; a client parsing the response
as CSV text would either crash on malformed input or (worse, if its CSV
parser is lenient enough to choke silently) misinterpret JSON syntax
characters as CSV field content.

**Why this is a real (not hypothetical) exposure**: `eligible_for_topn_stream`'s
admission bar is deliberately narrow but not rare — "top N groups by
count, ordered by count" (or by an aggregate on the group-by field
itself) is one of the most common analytical query shapes there is
(`docs/query-protocol/recipes.md`'s own cookbook style). Any caller using
`format:"csv"` for that exact shape today gets silently wrong output.

**Root cause**: the streaming top-N executor was added (Phase 1 per its
own comments) as a targeted optimization for the JSON find/aggregate
response path and was never extended to know about `format`/`delimiter`
— the eligibility gate that decides whether to take this shortcut
doesn't include "and the caller didn't ask for CSV" as a disqualifying
condition, and the shortcut's own output routine has no CSV branch to
fall into even if it did check.

**Two possible fix shapes** (plan must pick one, not leave it open):
1. **Disqualify CSV from top-N eligibility** — add a `format` parameter
   to `eligible_for_topn_stream` and return 0 whenever `format &&
   strcmp(format, "csv") == 0`, so a CSV request always falls through to
   the general hash-table path (`rc == -2` branch), which already emits
   correct CSV. Minimal, low-risk: doesn't touch the streaming executor's
   internals at all, just narrows when it's chosen. Cost: CSV+top-N-shape
   requests lose the streaming optimization's performance benefit
   (fall back to full scan+hashmap+sort), same as any other
   currently-disqualifying condition (multi-field group_by, `having`,
   etc.) already does.
2. **Teach the streaming executor CSV output** — thread `format`/`delimiter`
   through `agg_run_topn_stream`'s signature and add a `csv_emit_cell`
   branch to its drain loop, mirroring the general path's dual JSON/CSV
   branching. Preserves the streaming optimization for CSV callers too.
   Cost: larger diff, more surface to keep in sync with the general
   path's CSV formatting choices (delimiter escaping rules, trailing
   newline, etc.) going forward.

**Timing**: `before production` — this is a confirmed, reproducible,
silent wire-format bug on a common query shape, not a coverage gap.

## Finding 11 — `rebuild_object_v2` crash window silently discards object
data, and the next rebuild attempt permanently deletes the only surviving
copy

Discovered while researching exact anchors for a planned crash-injection
test suite (the "vacuum/rebuild `.new`/`.old` rename steps" hook
category) — confirmed by reading `rebuild_object_v2` and `slotcask_open`
in full, not hypothesized.

**Location**: `src/db/query_find.c:1026-1251` (`rebuild_object_v2`,
reached by every `vacuum`, `add-field`, `remove-field`, `edit-field`,
manual reshard, and `auto_reshard_sweep_one`/`auto_reshard_thread` call —
i.e. essentially every schema-changing or resharding operation) and
`src/db/slotcask.c:2708-2721` (`slotcask_open`).

**Mechanism**: `rebuild_object_v2` stages a rebuild with two renames, a
walk, and a cleanup, none of which are atomic with each other:

1. `rename(data_dir, legacy_dir)` — `data/` → `data.legacy/`
   (`query_find.c:1049`).
2. `rename(legacy_dir, legacy_data_under_root)` — `data.legacy/` →
   `<obj>/.rebuild_legacy_root/data/` (`query_find.c:1074`).
3. `slotcask_open(&new_db, obj_dir, ...)` (`query_find.c:1086`) —
   re-creates a brand-new, empty `data/` for the rebuild target.
4. `slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx)`
   (`query_find.c:1156`) — walks the staged legacy copy, re-inserting
   every live record into the new `data/`.
5. `rmrf(legacy_root); slotcask_registry_invalidate(...)`
   (`query_find.c:1237-1238`) — only on full success, discards the
   staged legacy copy.

`slotcask_open` (`slotcask.c:2708-2721`) unconditionally
`mkdirp_local(data_dir)`s (line 2721) — it has no way to tell "this
object never had a `data/` dir" apart from "this object's `data/` is
mid-rebuild, staged elsewhere." So if the daemon crashes (or is killed)
at any point between step 1 and step 5:

- The very next access to the object (from any client, or from the
  daemon's own startup path) calls `slotcask_open` against `obj_dir`,
  finds no `data/`, and silently fabricates a fresh, **empty** one. The
  object now appears to exist with zero records — no error, no log line
  a client would see, count returns 0.
- The real data is still intact, sitting in `data.legacy/` (if the crash
  landed between step 1 and step 2) or `.rebuild_legacy_root/data/` (if
  after step 2) — but nothing restores it. `rebuild_recovery()`
  (`objlock.c:110-168`, the daemon's only startup crash-recovery sweep)
  only knows about the suffixes `*.new`/`*.old`/generic-`.new`
  (`storage.c`'s `grow_recovery`) and per-shard `kf.new`
  (`slotcask.c:1316-1332`, `2782-2796`) — it has no knowledge of
  `data.legacy` or `.rebuild_legacy_root` at all.
- Worse: `rebuild_object_v2` itself only guards against **one** of the
  two staging names on its *own* next invocation —
  `rmrf(legacy_dir)` at `query_find.c:1040` ("Clean any stale
  data.legacy from a prior crashed rebuild") runs unconditionally at the
  top of every future call, on the assumption that any `data.legacy` left
  over is garbage from an aborted attempt. If the crash left the real,
  only-surviving copy of the object's data in `data.legacy/` (crash
  between steps 1 and 2), the very next `vacuum`/`add-field`/reshard
  attempt on that object **permanently deletes it**.
- If the crash instead landed after step 2 (data staged in
  `.rebuild_legacy_root/data/`), there is no cleanup path for that
  directory at all — not the top-of-function stale check (which only
  looks at `data.legacy`), not `rebuild_recovery()`. It leaks forever,
  silently, while the object itself now looks empty (or partially
  repopulated by post-crash writes into the freshly-fabricated empty
  `data/`).

**Why this matters**: this is not a narrow edge case — `vacuum` and
`auto_reshard_sweep_one`/`auto_reshard_thread` both route through this
exact path routinely (auto-reshard is a background, unattended trigger
per `docs/plans/2026-07-17-durability-sync-and-embedded-bg-threads.md`'s
Finding 9 scope), so any process interruption (OOM kill, `SIGKILL`, or an
ordinary process crash) landing in this window during ordinary automated
maintenance can silently zero out an object's data and then have the
next maintenance pass finish the job by deleting the one recoverable
copy. This finding does not claim recovery from sudden power loss; bounded
power-loss durability remains owned by Finding 2.

**Root cause**: the multi-step rebuild-in-place sequence has no
crash-safe staging convention that `slotcask_open` (or a startup sweep)
can recognize and prefer over "just create an empty `data/`." Every other
rebuild-adjacent artifact in this codebase (`data.new`/`.old`,
`indexes.new`/`.old`, `fields.conf.new`/`.old`, `schema.conf.new`/`.old`,
per-shard `kf.new`) has exactly this recognize-and-restore-or-discard
convention; `data.legacy` and `.rebuild_legacy_root` were added later (by
the v2 rebuild path) without a matching recovery entry.

**Timing**: completed 2026-07-19, before production.

**Disposition**: implemented by
`docs/plans/2026-07-18-rebuild-legacy-crash-recovery.md`. Rebuild now uses
an on-disk transaction with `.preparing`, `.active`, and `.done` states;
the atomic `.active` to `.done` rename is the commit point. Active state
always rolls data, fields metadata, schema metadata, and affected indexes
back together, while done state is cleanup-only. Startup recovery runs
under the DB-root flock and fails closed for ambiguous legacy layouts.
Edit-field finalization is inside the transaction, and auto-vacuum holds
the per-object write lock before it can upgrade to a rebuild.

Recorded verification: the isolated pre-fix legacy restore regression was
red (`5 passed, 2 failed`: restart returned zero records and retained
`data.legacy`). After implementation, the release rebuild filter passed
`384/384` assertions across 11 cases, auto-vacuum passed `17/17`, embedded
locking passed `5/5`, and the full release suite passed `10428/10428`
assertions across 300 cases. The corresponding ThreadSanitizer rebuild and
auto-vacuum filters passed `384/384` and `17/17` with no sanitizer report.

## Next steps

Finding 11 is fixed and test-confirmed; the remaining unannotated findings
still require their recorded follow-up work. Finding 7's bug is confirmed
and reproduced, but its regression test and fix still need writing. A trace
prompted by Finding 7 (checking whether
stale "v1 only" comments in `query_maint.c` meant more live v1 code
existed) turned up no further v1 storage code — those comments are just
outdated — but did surface Finding 8, a smaller, separate, confirmed bug
(`vacuum`/`recount` returning fake success/zero instead of an error for
an unopenable object). Confirming Finding 2's embedded-vs-daemon scoping
question also surfaced Finding 9 — embedded mode has no periodic-thread
mechanism at all, so auto-vacuum (not just the proposed durability sweep)
never runs there regardless of config; the two findings likely share one
fix (a single embedded-mode periodic-task driver both features register
against) rather than being solved independently. Per CORE-PROCESS: Finding 1 needs a diagnostic task
(the reproduction above) to confirm root cause before any fix task is
written into a plan; Finding 2 needs the embedded-vs-daemon scoping
question answered, plus a decision on the `dirty`-field-and-sweep design
now scoped to all four write-hot-path caches (kf/seg/bt/bm), before a plan
can specify where the periodic-sync code lives; Finding 3 is a trivial
comment fix with no test needed; Finding 4 needs a concurrency regression
test (pending wrlock + concurrent NQL read shouldn't block) before the
one-line removal lands; Finding 5 needs no engine change today — it's a
deployment-configuration decision to revisit only when/if a specific
upcoming project actually needs per-end-user dir isolation in embedded
mode; Finding 6 needs one small new/extended test case, no production
code change; Finding 7 needs the regression test above, then two fixes
together: remove the entire v1 `UCacheEntry`/ucache subsystem (full
call-site inventory above) and reroute the single-key `get`+`fields`
branch through the working kf/segment decode path — plus deliberate
(not incidental) decisions on the `stats`/`stats-prom` field removal and
the `scan_shards` fallback's new error-surfacing behavior; Finding 8
needs a regression test (vacuum/recount against a nonexistent object,
asserting an error instead of fake success/zero) then the same
`if (!sch.splits) { ... }` guard `cmd_rebuild_kf` already uses, copied
into `cmd_vacuum` and `cmd_recount`; Finding 9 needs the same embedded-mode
periodic-task design decision as Finding 2 before a plan can specify where
its code lives — planned together with Finding 2 (see below); Finding 10
(discovered mid-investigation of Finding 6, see that finding's entry)
needs a signature change to `eligible_for_topn_stream` to disqualify
`format:"csv"` requests from the JSON-only streaming path, plus a
regression test at both the unit level (`test_agg_topn_stream.c`) and the
wire level (`test_json_escape.c`, which also closes Finding 6's one
residual CSV-escaping gap).

**Status: all eleven findings now have a disposition.** Ten required a
dedicated implementation plan under `docs/plans/`; three (3, 5, 6) were
resolved directly (a one-line comment fix, a documented operational
decision needing no code change, and a pre-existing test that already
covered the claimed gap, respectively) rather than warranting a full plan,
per each finding's `[RESOLVED: ...]` annotation above. Plan documents:

- Finding 1 → `docs/plans/2026-07-17-rebuild-kf-ambiguous-duplicate-detection.md`
- Finding 2 + Finding 9 (shared design/fix) → `docs/plans/2026-07-17-durability-sync-and-embedded-bg-threads.md`
- Finding 3 → resolved directly (see annotation above), no plan
- Finding 4 → `docs/plans/2026-07-17-nql-drop-unconditional-objlock.md`
- Finding 5 → resolved directly (see annotation above), no plan
- Finding 6 → resolved directly (see annotation above), no plan
- Finding 7 → `docs/plans/2026-07-17-fix-get-fields-remove-dead-ucache.md`
- Finding 8 → `docs/plans/2026-07-17-vacuum-recount-validate-schema.md`
- Finding 10 → `docs/plans/2026-07-17-agg-csv-topn-format-bug.md`
- Finding 11 → `docs/plans/2026-07-18-rebuild-legacy-crash-recovery.md`

Finding 11's plan was executed on its dedicated branch on 2026-07-19. The
other plans remain subject to this repo's standing execution-mode exception:
execute on separate branches, one plan at a time, after human review.

A separate, not-yet-written, follow-on plan
(`docs/plans/2026-07-18-crash-hook-matrix.md`) adds a general
crash-injection test harness (named crash hooks in the write/rebuild
paths + kill/restart + invariant checks) proposed by the human
independently of this audit. Its rebuild/reshard hook category is a
regression test for Finding 11's fix and is written to fail against
current code until that fix lands — the two plans should land in that
order (Finding 11 first) or the crash-matrix plan should note the
expected-red state explicitly if written first.
