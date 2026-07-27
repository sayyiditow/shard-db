# Fix kf/segment/index persistence ordering with crash-recoverable intent markers

Status: **second rewrite**, in response to a second review round. The
first rewrite was rejected for: (1) `durability_msync(h.map + offset, ...)`
passing a non-page-aligned address straight to `msync`, which fails
`EINVAL` on any offset not a multiple of the page size — true for nearly
every real record offset; (2) claiming "index-before-kf" was a *safe*
ordering when it's actually symmetric with kf-before-index — both leave a
**permanent** (not "until next write") false positive on disk, not a
merely bounded one, because equality reads never re-verify the fetched
record against the predicate; (3) a test matrix requiring an
`update-after-index-clear-sync` pause point that the code sketch never
actually produced; (4) several places deferring a decision to "the
executor picks," "check whether... likely," "locate... likely something
like" — not self-contained, per CORE-PROCESS; (5) a base-branch regression
proof that depended on Task 1's own new pause hook, which by definition
doesn't exist on `main` — the test can't run against pristine `main` as
originally worded.

This rewrite replaces the core mechanism. Instead of arguing over which of
two orderings produces a *smaller* permanent-inconsistency risk (neither
does — that was the flawed premise), it adds a **small, per-kf-shard,
crash-recoverable intent marker** that makes the two-file (index, kf)
publish step fully recoverable after a crash, not just narrower. This
was worked out interactively with the plan's author (transcript
paraphrased in "Design rationale" below, kept because CORE-PROCESS
requires the *why*, not just the *what*, and the reasoning for rejecting
two simpler alternatives — a full WAL, and a bare boolean flag with no
payload — is load-bearing for why this specific shape was chosen).

Independent of `docs/plans/2026-07-24-remove-rebuild-kf-and-vacuum-validation-gate.md`.

## Root cause (unchanged from the first draft)

`durability_sync_one_pass` (`src/db/durability.c:202-242`) flushes
`db->kfcache`/`db->segcache`/`bt_cache`/`db->bm_cache` entries marked
dirty, but only on a timer, not synchronously with any specific write. A
crash between "record acknowledged" and "next tick" can leave kf pointing
at unflushed segment bytes, or an index entry that was never flushed (or
vice versa).

## Design rationale (why a marker, not a plain reorder, not a full WAL)

**Why a plain reorder doesn't work.** Update touches three things: the new
segment record, the index (remove old-value entry, add new-value entry),
and kf (repoint the slot). Whichever of {index, kf} is synced *second*,
a crash between the two leaves the *first*-synced one durable and the
second stale — and because equality reads resolve purely through
kf's flag/hash and never re-decode the fetched record to check it against
the query predicate (`bm_emit_cb` forwards the query's own predicate
bytes; `OP_EQUAL`/`OP_IN` are excluded from any recheck-against-primary
path, `query.c:403-409`; the btree read path, `btree.h:107-108`, has the
same trust model), that staleness is **not** self-correcting — nothing
touches it again until this exact key is next written. Index-then-kf
produces a stuck false positive on the *new* value; kf-then-index
produces a stuck false positive on the *old* value. Symmetric, both
permanent. There is no ordering that fixes this — ordering only choses
which value gets stuck.

**Why not a full WAL.** A general write-ahead log (durable intent record
for every mutation, replayed in full on every restart) is the standard
fix and is explicitly out of scope for this plan's size — it changes the
durability model for every operation, not just the one crash window this
plan targets, and needs its own replay/checkpoint/truncation design.

**Why not a bare flag.** A single boolean ("an update was interrupted in
this shard") tells recovery *that* something needs fixing but not *what*
— it has no way to know which key, which field, or what the old/new
values were, so recovery could only fall back to a full per-shard (or
per-object) reindex. That works but is needlessly expensive at scale.

**The chosen middle ground**: a marker that carries just enough to make
recovery *targeted* and *idempotent*, without carrying full payload
values (which could be large, e.g. a long varchar) — carry **pointers**
to where the old and new record bodies already durably live (they're
segment records, already synced by the time the marker is written), and
let recovery *dereference* those pointers to read the actual values back,
rather than duplicating them into the marker. This keeps the marker
fixed-size and tiny (one struct, not proportional to record size), and
it works because segment reclaim of the "old" location is explicitly
gated on the marker's own completion (see ordering table) — the old
record is guaranteed to still be intact and readable for as long as the
marker referencing it is active.

**Commit-intent boundary and error semantics.** A successfully fsynced
marker is the durable commit-intent point for an indexed insert or update.
After that point the operation is never rolled back. A post-marker kf or
index failure is replayed synchronously under the held kf lock; if it
cannot converge, the daemon fails closed and startup completes the marker
before serving reads. `durability_degraded` is reserved for the safe case
where kf and indexes have converged but final marker deletion failed.
Accordingly, every ordinary rejection must happen before the marker is
written: CAS/duplicate checks, schema validation, bitmap-cap preflight,
and any kf growth/resplit work that can fail as a normal write error. A
client that loses its connection after marker fsync has the usual
ambiguous-outcome contract: the request may complete after restart even
though no success response was received.

A crash after marker fsync but before marker clear is fully recoverable:
the marker says "make the requested new state true." A crash before marker
fsync leaves only an orphaned, already-synced segment record; no kf entry
or index entry has been published, so readers cannot observe it and a
retry is correct. This is intentionally a crash-recoverable single-write
protocol, not a general multi-operation transaction manager.

**No-op for objects with zero indexed fields.** The marker exists solely
to make the index-vs-kf reconciliation window recoverable — when an
object has no indexed fields at all, there is no second independently-
trusted structure that kf could ever disagree with (same reasoning the
ordering table already applies to Compaction and to Delete's lazy path),
so segment-write-then-kf-repoint is already fully crash-safe on its own,
exactly as it is today. Writing and clearing a marker in that case would
be pure overhead — two extra `fsync`s plus two directory `fsync`s per
write, protecting nothing. All four of Task 3b's real call sites
(`slotcask_upsert_with_hooks`'s two branches, `upsert_slow_path`,
`slotcask_insert_with_hooks`) must gate the entire marker/index-sync
sequence on whether the object has any indexed fields, collapsing to
`segment write (sync_now=1) → kf repoint/put_new → targeted kf-slot sync`
when it doesn't. See Task 3b for the exact gate (`opts->has_indexed_fields`).

## `durability_msync` must operate on page-aligned ranges

`durability_msync` (`src/db/durability.c:40-66`) wraps `msync(addr, len,
MS_SYNC)` unchanged — `msync` requires `addr` to be page-aligned
(`EINVAL` otherwise, per POSIX). Every new call site this plan adds syncs
a sub-page byte range at an arbitrary record offset inside a larger
mmap'd file, so a wrapper that aligns down to the containing page(s) is
required; **do not call `durability_msync` directly with a raw
`base + offset` anywhere this plan touches.**

```c
/* src/db/durability.c, new function, next to durability_msync */
int durability_msync_range(void *base, size_t offset, size_t len) {
    static long page_size = 0;
    if (!base || len == 0) { errno = EINVAL; return -1; }
    if (page_size == 0) {
        long ps = sysconf(_SC_PAGESIZE);
        if (ps <= 0) return -1;
        page_size = ps;
    }
    uintptr_t addr = (uintptr_t)base + offset;
    uintptr_t aligned = addr & ~((uintptr_t)page_size - 1);
    size_t front_pad = (size_t)(addr - aligned);
    size_t sync_len = len + front_pad;
    sync_len = (sync_len + (size_t)page_size - 1) & ~((size_t)page_size - 1);
    return durability_msync((void *)aligned, sync_len);
}
```
Declare in `src/db/durability.h` next to `durability_msync`'s existing
declaration (anchor: the line declaring `int durability_msync(void *addr,
size_t len);` — locate it exactly, add the new declaration immediately
after).

Every call site added by this plan (Tasks 3, 5, 6) uses
`durability_msync_range(handle.map, offset, len)`, never the raw
two-argument form.

## Segment record ordering (why an in-flight write is inert)

`seg_record_emit` already writes payload first and performs the final
`flag=1` store last. For this plan the entire emitted record, including
that live flag, is synchronously flushed **before** marker creation. Thus
a valid marker always names a complete, durable, readable new record.
Before marker fsync, the record is an unreferenced orphan: no kf entry or
index points to it, and recovery ignores it. Do not invent a second
activation phase or leave `flag=0` across marker creation; that would make
the marker impossible to replay.

## Marker file format

**One file per kf shard**, sibling to the shard file it protects:
`$DB_ROOT/<dir>/<object>/data/kf/<NNN>_marker.dat` (same 3-hex-digit
numbering as `data/kf/NNN.kf`). Rejected the earlier "one file per
object, N slots" design after the user flagged a real fsync-contention
problem it would have introduced: `fsync` durably commits the *whole
inode*, not just the caller's byte range, and on journaling filesystems
that's a single serialized journal-commit boundary per inode — so every
concurrent insert/update across *every* kf shard of an object would have
funneled through the same fsync target, adding contention that doesn't
exist today (each shard's kf/index files are already separate inodes,
so concurrent writers to different shards never share an fsync commit).
Per-shard marker files put the marker's fsync boundary at the exact same
granularity as the kf shard's own wrlock and the kf shard file's own
fsync boundary — no new contention, no bucketing scheme needed to claw
some of it back.

This also simplifies the lifecycle: since there's no array to index into,
the file holds exactly one slot and is **created on marker-write,
deleted on marker-clear** — idle = absent, no zeroed-but-present file
ever sits around. The rare unclean-shutdown recovery sweep then just
`readdir`s `data/kf/` for files matching `*_marker.dat` and only touches
shards that actually have one (bounded by *in-flight* operations at
crash time, not by `splits`) — this is what makes the earlier
one-file-per-object consolidation (which existed only to keep the sweep
cheap) unnecessary: the clean-shutdown flag (see below) already skips
the sweep entirely in the common case, and per-shard files make the rare
case cheap too, by construction, not by batching.

```c
/* src/db/shard_db_internal.h, new type, near other on-disk struct defs */
#define KF_MARKER_MAGIC 0x4B464D31u /* "KFM1" */

typedef struct {
    uint32_t magic;        /* KF_MARKER_MAGIC; checksum validates full record */
    uint32_t kf_slot;     /* update: existing slot; insert: UINT32_MAX and
                             recovery derives the slot from new record's key */
    uint32_t old_offset;
    uint32_t new_offset;
    uint16_t old_file_id;
    uint16_t new_file_id;
    uint8_t  old_stream_id;
    uint8_t  new_stream_id;
    uint8_t  has_old;      /* 0 = insert intent: recovery publishes new_* if
                               kf is absent; 1 = update intent: recovery
                               repoints the existing kf_slot to new_*. Recovery
                               branches on this, not on old_offset==0. */
    uint8_t  reserved[5];
    uint32_t checksum;     /* XXH32 over bytes [0, offsetof(checksum)) */
} KfMarkerSlot; /* sizeof == 32: 4+4+4+4+2+2+1+1+1+5+4. Field order keeps every
                    multi-byte member at a naturally aligned offset (0, 4, 8,
                    12, 16, 18, 28) so no compiler padding is inserted anywhere
                    except the explicit reserved[5] — verify with static_assert
                    (sizeof(KfMarkerSlot) == 32) at the definition site,
                    Task 2. */
```

`has_old` replaces the earlier (incorrect) `old_file_id != 0 || old_offset != 0`
sentinel the recovery-sweep sketch used — see Task 7 below.

No `flag` field is needed anymore — the file's mere *existence* is the
flag (present = in-flight, absent = idle), so there's one less thing that
can be inconsistent with itself.

For an insert (`has_old == 0`), all old-location fields are zero and are
never interpreted. The explicit flag—not any offset sentinel—is the only
discriminator, because offsets are valid data values.

**Invalid marker handling is fail-closed.** A crash before marker `fsync`
returns cannot have published either kf or index, so a zero-byte new file
is harmless. A non-empty file with an invalid size, magic, or checksum can
also be post-fsync media corruption; treating it as idle would discard the
only redo record for a possibly published intent. Use the already-vendored
`XXH32` over every byte before `checksum` in `KfMarkerSlot`, set it before `fsync`, and verify
it on every read. `ENOENT` means absent; a zero-byte file means an
unpublished torn create and may be unlinked; every other invalid marker is
`MARKER_CORRUPT`. Startup and live writers log its path and fail closed on
`MARKER_CORRUPT`; they never unlink it. This trades availability for
correctness rather than silently falling back to the old data-loss window.

`XXH32` is intentionally a **new checksum variant in this codebase**:
`xxhash.h` vendors it, but existing storage call sites use `XXH3_128bits`.
Task 2 must first paste `rtk proxy rg "XXH32" src/db` evidence showing no existing
call site to mirror, then add this one narrow marker-integrity use; do not
mistake it for an established on-disk hash convention.

**Directory-entry durability**: creating and deleting a file each
durably change the *containing directory's* metadata, not just the
file's own data — POSIX requires an explicit `fsync` on the directory fd
to make a `create`/`unlink` survive a crash; fsyncing the file itself is
not sufficient for that. Both `kf_marker_write` (create) and
`kf_marker_clear` (unlink) below fsync `data/kf/`'s directory fd after
their respective operation — this is a real, easy-to-miss requirement
for a create/delete-based lifecycle (it wasn't needed by the earlier
zero-in-place design, which never created or removed the file).

Marker I/O uses plain `open`/`pwrite`/`fsync`/`unlink` rather than adding
a fifth mmap'd cache subsystem (alongside kfcache/segcache/bt_cache/
bm_cache) — markers are written/cleared at most once per update/insert
(not once per read), so the extra syscalls per operation are acceptable
and the implementation stays simple:

```c
/* src/db/slotcask.c, new static helpers, near kfcache_release */
static void kf_marker_path(char *buf, size_t cap, const char *data_dir,
                           int kf_shard) {
    snprintf(buf, cap, "%s/data/kf/%03x_marker.dat",
             data_dir, (unsigned)kf_shard);
}

static void kf_marker_dir_path(char *buf, size_t cap, const char *data_dir) {
    snprintf(buf, cap, "%s/data/kf", data_dir);
}

static int fsync_dir(const char *dir_path) {
    int dfd = open(dir_path, O_RDONLY | O_DIRECTORY);
    if (dfd < 0) return -1;
    int rc = fsync(dfd);
    close(dfd);
    return rc;
}

static int kf_marker_write(const char *data_dir, int kf_shard,
                           const KfMarkerSlot *slot) {
    char path[PATH_MAX], dpath[PATH_MAX];
    KfMarkerSlot durable = *slot;
    durable.checksum = XXH32(&durable, offsetof(KfMarkerSlot, checksum), 0);
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return -1;
    ssize_t n = pwrite(fd, &durable, sizeof(durable), 0);
    if (n != (ssize_t)sizeof(durable)) { close(fd); return -1; }
    if (fsync(fd) != 0) { close(fd); return -1; }
    close(fd);
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    return fsync_dir(dpath);
}

static int kf_marker_clear(const char *data_dir, int kf_shard) {
    char path[PATH_MAX], dpath[PATH_MAX];
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    if (unlink(path) != 0 && errno != ENOENT) return -1;
    kf_marker_dir_path(dpath, sizeof(dpath), data_dir);
    return fsync_dir(dpath);
}

/* Return 0=valid, 1=absent, 2=zero-byte pre-publish create, -1=corrupt/I/O. */
static int kf_marker_read(const char *data_dir, int kf_shard, KfMarkerSlot *out) {
    char path[PATH_MAX];
    struct stat st;
    kf_marker_path(path, sizeof(path), data_dir, kf_shard);
    if (stat(path, &st) != 0) return errno == ENOENT ? 1 : -1;
    if (st.st_size == 0) return 2;
    if (st.st_size != (off_t)sizeof(*out)) { errno = EILSEQ; return -1; }
    int fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    ssize_t n = pread(fd, out, sizeof(*out), 0);
    int saved = errno;
    close(fd);
    if (n != (ssize_t)sizeof(*out)) { errno = n < 0 ? saved : EILSEQ; return -1; }
    if (out->magic != KF_MARKER_MAGIC ||
        out->checksum != XXH32(out, offsetof(KfMarkerSlot, checksum), 0)) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}
```

### Retained-marker gate (mandatory on every writer)

After the segment-write preparation and immediately after a write path
acquires the final kf-shard writer lock, but before it creates a new marker
or mutates that kf shard, it calls
`kf_marker_read`. `1` permits the new commit. `2` is an unpublished
zero-byte create: call `kf_marker_clear` and proceed only if its directory
fsync succeeds. `0` means a prior commit reached the safe
`durability_degraded` state: invoke `kf_marker_replay_locked`, then
`kf_marker_clear`; proceed only after both succeed. `-1` is corrupt/I/O:
release no lock, terminate the daemon, and leave the marker untouched for
operator repair. No path may call `O_TRUNC` in `kf_marker_write` until this
gate has completed. This makes a retained marker a mandatory cleanup
barrier rather than information a later write can overwrite.

The same `kf_marker_replay_locked(db, sid_kf, &kh, &marker)` function is
used by this gate and Task 7 startup recovery. Its exact six steps are the
recovery algorithm below: read records, establish+sync kf, apply+sync the
index diff, clear marker, then reclaim old segment. It is invoked with the
kf writer lock already held and returns success only after marker clear.

## Required ordering, per operation (revised)

`has_indexed_fields` below is the object-level flag from `load_index_fields()`'s
count (see Task 3b) — it is **not** re-derived per record from which fields
actually changed; even an update that happens to touch zero indexed fields
on an object that has other indexed fields still goes through the marker
path, since the marker protects the class of operation, not the individual
diff. Only an object with zero indexed fields *at all* takes the no-marker
row.

| Operation | Required order |
|---|---|
| Insert, `has_indexed_fields=0` | write+sync new segment → publish kf (`kf_put_new`) → targeted sync of the new kf slot and header → release lock. No marker. |
| Insert, `has_indexed_fields=1` | write+sync new segment → final-lock retained-marker gate → create+fsync commit-intent marker (`has_old=0`, `new_*`=this record; `kf_slot=UINT32_MAX`) → publish kf (`kf_put_new`) → targeted sync of the new kf slot and header → write+sync new index entries using the confirmed `put_slot` → delete+fsync-dir marker |
| Update, `has_indexed_fields=0` | write+sync new segment → repoint kf → targeted sync of that kf slot → release lock → tombstone old segment. No marker. |
| Update, `has_indexed_fields=1` | write+sync new segment → final-lock retained-marker gate → create+fsync commit-intent marker (`old_*`=pre-update kf pointer, `new_*`=this record) → repoint kf → targeted sync of that kf slot → remove old-value index entries/add new-value entries → sync touched index file(s) → delete+fsync-dir marker → tombstone old segment (lazy) |
| Delete | tombstone kf → sync kf → tombstone physical segment (lazy) → remove index entries (lazy — self-heals via kf flag check, unchanged from the first draft, no marker needed) |
| Compaction | write recipient segment → `durability_msync_range` it → repoint kf → sync kf → unlink donor (lazy, existing fsync path) — no marker needed (only two entities, kf/seg; no third, independently-trusted structure like the index is involved, so there's nothing a marker would protect that isn't already protected by the existing seg-then-kf order) |

Note the index write no longer needs the four separate pause/sync points
(`update-after-index-clear`, `-clear-sync`, `-after-index-set`,
`-set-sync`) the first draft required — that was needed only because the
first draft's recovery story depended on knowing *which* half completed.
With the marker, recovery always redoes *both* halves unconditionally
(idempotent — inserting an already-present entry or removing an
already-absent one is a no-op), so a **single** sync after both index
mutations is sufficient. This directly resolves the second review's
"High" finding about the missing sync boundary — the boundary isn't
missing, it's no longer needed.

## Recovery sweep (startup, gated on unclean shutdown)

**Skip entirely on a clean shutdown** — extend the existing single-
instance-guard lock file (`AGENTS.md`'s "Single-instance guard" section,
`$DB_ROOT/.shard-db.lock`) with a sibling flag file,
`$DB_ROOT/.shard-db.clean`:
- `cmd_server`'s graceful `stop` path writes `.shard-db.clean`, fsyncs
  that file, and fsyncs `$DB_ROOT` **after** every pending write has
  drained **and after verifying no `*_marker.dat` or
  `*_batch_marker.dat` remains anywhere under the DB root**. If a safe
  `durability_degraded` marker remains, stop still completes but leaves
  `.shard-db.clean` absent, forcing the next startup sweep to finish its
  cleanup. This is the final durable action before releasing the lock.
- Startup acquires the lock, records whether `.shard-db.clean` exists,
  then unlinks it and fsyncs `$DB_ROOT` **before any database write,
  background thread, or client listener starts**. It uses the captured
  existence result: present means the previous run drained cleanly and
  skips recovery; absent means an unclean exit and runs recovery. A crash
  after this unlink is safe because the new process has not yet changed
  database state.

**The sweep itself, only on unclean-shutdown startup**: enumerate every
object across every tenant directory (reuse whatever existing
directory-walk `db-dirs`/`list-objects` already uses — locate the exact
helper at execution time), and for each object, `readdir` its
`data/kf/` directory for filenames matching `*_marker.dat`. This touches
only shards that actually had an in-flight operation at crash time — not
every possible shard up to `splits` — since idle shards have no marker
file at all (created-on-write, deleted-on-clear). Parallelize via the
existing `parallel_for` machinery already used for parallel indexing
(`index.c`) rather than a new threading mechanism, since the work is
independent per object.

For each `*_marker.dat` file found with valid `magic`, recovery's job is
exactly "finish this committed intent." The segment append always already
succeeded and is durable before marker fsync. Recovery holds the object's
write lock and completes each marker before clients can connect:

1. Dereference `new_stream_id`/`new_file_id`/`new_offset` and read the
   record currently stored there — this is `new_value` for step 3.
2. If `has_old == 1` (update-type marker — see `KfMarkerSlot` above),
   dereference `old_stream_id`/`old_file_id`/`old_offset` and read that
   record — this is `old_value`. If `has_old == 0` (insert-type marker),
   `old_value = NULL`.
3. Establish and sync the desired kf mapping **before** changing indexes.
   For `has_old == 1`, redo `kf_repoint_at_slot(kf_slot, new_*)` and
   the targeted kf-slot sync helper. For `has_old == 0`, call `kf_lookup_with_slot` using
   the key read from `new_value`; if it is absent, call `kf_put_new`, then
   look it up again to obtain the actual slot. In both cases verify the
   kf pointer equals `new_*`, then sync it. The marker's `kf_slot` is a
   hint for updates only; `UINT32_MAX` for inserts is intentional.
4. Call the same `apply_index_diff` helper (`storage.c`, Task 3b) with
   `old_value`/`new_value` and the confirmed kf slot, then synchronously
   flush every touched index file. This helper must converge a repeated
   invocation to the same index state (delete old entry if present; add
   new entry if absent), rather than depending on a one-shot write.
5. Delete the marker file and fsync `data/kf/`. If that unlink or dir
   fsync fails, keep the old segment live and return degraded; a later
   recovery replay is safe.
6. Only after step 5 succeeds, tombstone/recycle the old segment for an
   update. Insert markers have no old segment to reclaim.

Applies identically to `*_batch_marker.dat` files (Task 5): walk each
valid slot in the array through steps 1–5 above, then delete the whole
batch file (step 6) only once every slot has been processed.

A zero-byte marker is the only safe torn-create exception: delete it and
fsync the directory because no later step could have run before marker
fsync. Any non-empty marker with a bad size, magic, or checksum is corrupt:
leave it in place, log the path, and refuse startup before accepting
connections. Never reinterpret a corrupt redo record as idle.

This must complete, for every object, before the server begins accepting
client connections — it's a correctness gate, not a background
optimization. It runs independently of (and can run concurrently with,
since they touch disjoint state) the existing cache-warmup thread.

## Task 1 — `durability_msync_range` (test-first)

**Test first** (new file `src/test/cases/test_durability_ordering.c`,
registered exactly as the first draft specified — `TEST_REGISTER`
convention, `build.sh` insertion point unchanged from the first draft's
Task 1 item 3, still correct): call `durability_msync_range` on a
deliberately non-page-aligned `(map, offset, len)` triple against a real
mmap'd test file and assert it returns `0`, not `EINVAL`. This **must
fail on `main`** today if written against raw `durability_msync` instead
— demonstrate that first (call `durability_msync(map + 37, 64)` directly
against a page-aligned mmap base, non-page-aligned offset, assert it
returns nonzero/`EINVAL` on `main`), paste that failing run, then add
`durability_msync_range` and show the same scenario now returns `0`.

**Task 1 done-check**: paste both runs (raw call failing, wrapped call
succeeding), plus `SKIP_TESTS=1 ./build.sh` clean.

## Task 2 — marker file infrastructure (test-first)

Tests first, in `test_durability_ordering.c`:
0. `static_assert(sizeof(KfMarkerSlot) == 32, "KfMarkerSlot must stay a
   fixed 32-byte record — recovery's pread assumes this size")` at the
   struct definition site (`kf_marker_write`'s header, alongside the
   struct). Not a runtime test — a compile-time guard against a future
   field addition silently shifting the on-disk layout.
1. `kf_marker_write` then re-`pread` the resulting file, assert every
   field round-trips including `magic` and `has_old`, and assert the file
   exists at `data/kf/<NNN>_marker.dat`.
2. `kf_marker_clear` after a write, assert the marker file no longer
   exists (`stat` returns `ENOENT`) and that `data/kf/`'s mtime reflects
   the deletion (sanity check the dir-fsync path actually ran, not just
   the unlink).
3. Do not create a deferred test. Task 7 owns the corrupt-marker test:
   write raw nonzero garbage through `pwrite`, restart unclean, assert the
   daemon refuses to serve and leaves the marker in place. A separate
   zero-byte-file case asserts recovery removes it and proceeds, proving
   the only safe torn-create exception.

These three do not need to "fail on `main` for a stated reason" in the
regression-proof sense — `kf_marker_write`/`kf_marker_clear` don't exist
on `main` at all, so this is new-code TDD (write the test against the
not-yet-existing function, confirm it fails to *compile* or link, then
add the function, confirm it passes), not a regression fix. State that
distinction explicitly in the task's paste, since it's a different shape
of evidence than Task 3's canary below.

**Task 2 done-check**: paste the compile-failure-then-pass sequence for
tests 1-2. Task 7 supplies the integration tests for invalid-marker policy.

## Task 3 — single-record insert/update: the marker-guarded fix (test-first)

### 3a. Regression tests — write first, prove the bug exists on `main`

Extending `test_durability_ordering.c`:

1. **Seg-sync failure aborts the operation outright** (same as the first
   draft's test 1 — `durability_test_msync_fail_next(1, EIO)` before
   insert, assert insert fails and a subsequent GET fails). Uses
   `durability_msync_range` under the hood now, injection still targets
   `durability_msync` (the wrapped call), unaffected by the wrapper.
   Fails on `main` today (no synchronous seg sync exists yet to fail) —
   paste that.
2. **The canary — kf durable, index not yet durable.** Insert a record
   with indexed field `F=A`. Add the named pause phase
   `update-after-kf-sync`, firing after the targeted kf-slot sync and before
   the index diff. Issue an update changing `F` from `A` to `B`. While
   paused, `SIGKILL` the daemon.
   Restart against the same `DB_ROOT` **with the `.shard-db.clean` flag
   deliberately absent** (simulating the unclean shutdown, which is what
   actually happened) so the recovery sweep runs. Run `WHERE F = A` and
   `WHERE F = B` after restart. **On `main` today** (no marker, no
   recovery sweep — this test exercises code that doesn't exist there,
   so frame this paste as "this scenario has no recovery path on `main`;
   the closest equivalent is the first draft's kf-before-index test,
   which showed `WHERE F=A` matching after an equivalent crash" — cite
   that as the base-branch evidence, rather than literally running
   nonexistent code, since the second review correctly identified that
   Task 1's own new pause hook can't run unmodified against pristine
   `main`). After Task 3b's fix: `WHERE F = A` returns nothing, `WHERE
   F = B` returns the record with `F = B` — assert both, this is the
   actual proof the false positive is gone, not merely narrowed.
3. **Marker cleared on the normal (no-crash) path** — insert, update,
   assert the corresponding `data/kf/<NNN>_marker.dat` does not exist
   immediately after the update call returns, proving the marker doesn't
   leak a file on the success path.
4. **Delete stays marker-free** — delete a record with an indexed field,
   assert no `data/kf/<NNN>_marker.dat` was ever created for that shard
   (still absent, as before), confirming delete's
   already-correct lazy-removal behavior (unchanged from the first draft)
   isn't accidentally routed through the new marker path.

### 3b. The fix

**Verified against real source** (not the earlier draft's invented
`seg_write_record_body`/`seg_activate_record`/`seg_record_emit_inactive`,
which do not exist anywhere in the codebase): `seg_record_emit`
(`slotcask.c:2024-2048`) already writes the record body **and** flips its
flag 0→1 atomically inside one call, via a release-fence store — there is
no externally observable "write inactive, then activate" split in memory.
The only real two-phase concept is **written-in-mmap vs. durably synced
to disk**; `seg_write_record`/`seg_write_record_varlen`
(`slotcask.c:2052-2095`) call `seg_record_emit` and then only
`durability_mark_dirty` (queues for the *background* sweep) — there is no
existing synchronous flush on this path.

**Fix: add a `sync_now` parameter to `seg_write_record` and
`seg_write_record_varlen`.** When set, `durability_msync_range` runs
synchronously, under the same segcache rdlock, before `segcache_release`:

```c
/* src/db/slotcask.c:2052 — was seg_write_record(db, stream_id, file_id,
   offset, hash, key, klen, value, vlen); add a trailing sync_now param. */
static int seg_write_record(const SlotcaskDb *db, uint8_t stream_id,
                             uint16_t file_id, uint32_t offset,
                             const uint8_t hash[16],
                             const void *key, size_t klen,
                             const void *value, size_t vlen,
                             int sync_now) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 0, 1) != 0) return -1;
    seg_record_emit(h.map + offset, db->slot_size, hash, key, klen, value, vlen);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    int rc = 0;
    if (sync_now && durability_msync_range(h.map, offset, (size_t)db->slot_size) != 0)
        rc = -1;
    segcache_release(&h);
    return rc;
}

/* src/db/slotcask.c:2078 — same change, rec_size in place of db->slot_size
   for the sync range (matches the varlen record's actual on-disk length,
   not the fixed slot_size the fixed-format sibling uses). */
static int seg_write_record_varlen(const SlotcaskDb *db, uint8_t stream_id,
                                    uint16_t file_id, uint32_t offset,
                                    const uint8_t hash[16],
                                    const void *key, size_t klen,
                                    const void *value, size_t vlen,
                                    size_t rec_size, int sync_now) {
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 1, 0, 1) != 0) return -1;
    seg_record_emit(h.map + offset, (int)rec_size, hash, key, klen, value, vlen);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    int rc = 0;
    if (sync_now && durability_msync_range(h.map, offset, rec_size) != 0)
        rc = -1;
    segcache_release(&h);
    return rc;
}
```

Every existing call site must pass an explicit `sync_now` argument (the
signature change makes the compiler enforce this — no call site can be
silently missed). Only the four `pre_commit`-driven call sites below pass
`sync_now=1`; every other call site (`slotcask_insert`/`slotcask_update`
at `slotcask.c:2212/2220/2337/2346` — used only by the reindex-rebuild
verbatim-reinsert path in `query_find.c`, no indexes involved;
`slotcask_bulk_update` at `slotcask.c:2549/2557` and the bulk primitives
covered separately by Task 5) passes `sync_now=0`, preserving today's
async-flush behavior exactly — this task does not change durability
timing for any path outside the four sites below.

**Shared index-diff helper** (`src/db/storage.c`) — `v2_insert_pre_commit`
and `v2_update_pre_commit` share the same index-diff/dispatch goal, but
not an identical body: the update path starts from stored binary records
(`build_index_key_from_record_into`), while the insert path starts from
JSON (`build_index_key_from_json`). Extract the common typed-key/diff/
dispatch portion once, then adapt the insert path to materialize its typed
new record before calling it; do not simply delete one body as a textual
duplicate. This lets both the live
`pre_commit` path and Task 7's recovery sweep call the exact same code —
recovery must derive index keys the same way the live path does, not a
parallel reimplementation that can drift:

```c
/* src/db/storage.c, new — extracted from v2_update_pre_commit's body */
typedef struct {
    const char *db_root, *object;
    int nidx;
    char (*idx_fields)[256];
    enum IndexType *idx_types;
    int splits;
    const uint8_t *hash;
    int kf_shard;
    uint32_t kf_slot;
    TypedSchema *idx_ts;
    const uint8_t *old_value;   /* NULL when there is no prior record
                                    (insert-type marker replay) */
    const uint8_t *new_value;   /* required */
    char *err_buf;
    size_t err_buf_len;
} IndexDiffApplyArgs;

static int apply_index_diff(const IndexDiffApplyArgs *a) {
    if (a->nidx == 0) return 0;
    enum { INDEX_KEY_MAX = 4096 };
    size_t arena_bytes = (size_t)a->nidx * (size_t)(2 * INDEX_KEY_MAX);
    uint8_t *arena = malloc(arena_bytes);
    UpdateIdxArg args[MAX_FIELDS];
    uint8_t *fb_bufs[2 * MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int i = 0; i < a->nidx; i++) {
        uint8_t *old_slot = arena ? arena + (size_t)i * 2 * INDEX_KEY_MAX : NULL;
        uint8_t *new_slot = old_slot ? old_slot + INDEX_KEY_MAX : NULL;
        size_t old_len = 0, new_len = 0;
        int have_old = 0, have_new = 0;
        uint8_t *old_buf = NULL, *new_buf = NULL;

        if (arena) {
            int ro = a->old_value
                ? build_index_key_from_record_into(a->idx_ts, a->old_value,
                                                   a->idx_fields[i],
                                                   old_slot, INDEX_KEY_MAX, &old_len)
                : 0;
            int rn = build_index_key_from_record_into(a->idx_ts, a->new_value,
                                                       a->idx_fields[i],
                                                       new_slot, INDEX_KEY_MAX, &new_len);
            have_old = (ro == 1);
            have_new = (rn == 1);
            old_buf = have_old ? old_slot : NULL;
            new_buf = have_new ? new_slot : NULL;
            if (ro == -1) {
                have_old = build_index_key_from_record(a->idx_ts, a->old_value,
                                                       a->idx_fields[i], &old_buf, &old_len);
                if (have_old) fb_bufs[n_fb++] = old_buf;
            }
            if (rn == -1) {
                have_new = build_index_key_from_record(a->idx_ts, a->new_value,
                                                       a->idx_fields[i], &new_buf, &new_len);
                if (have_new) fb_bufs[n_fb++] = new_buf;
            }
        } else {
            if (a->old_value)
                have_old = build_index_key_from_record(a->idx_ts, a->old_value,
                                                       a->idx_fields[i], &old_buf, &old_len);
            have_new = build_index_key_from_record(a->idx_ts, a->new_value,
                                                   a->idx_fields[i], &new_buf, &new_len);
            if (have_old) fb_bufs[n_fb++] = old_buf;
            if (have_new) fb_bufs[n_fb++] = new_buf;
        }

        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len || memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        /* Recovery calling this with old_value read from the marker's
           old_* location and new_value read from new_* produces the same
           "changed" verdict the live call would have — a replay after an
           already-completed live update is a no-op (new==new everywhere),
           making recovery idempotent by construction, not by a separate
           "already applied?" check. */
        if (changed) {
            args[n_args].db_root = a->db_root;
            args[n_args].object  = a->object;
            args[n_args].field   = a->idx_fields[i];
            args[n_args].splits  = a->splits;
            args[n_args].new_key = have_new ? new_buf : NULL;
            args[n_args].new_len = new_len;
            args[n_args].old_key = have_old ? old_buf : NULL;
            args[n_args].old_len = old_len;
            args[n_args].hash    = a->hash;
            args[n_args].type    = a->idx_types ? a->idx_types[i] : IT_BTREE;
            args[n_args].kf_shard = a->kf_shard;
            args[n_args].kf_slot  = a->kf_slot;
            args[n_args].bm_max_values = 0;
            n_args++;
        }
    }

    int idx_failed = 0;
    if (n_args > 0) {
        parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
        for (int i = 0; i < n_args; i++) {
            if (capture_index_update_error(a->err_buf, a->err_buf_len, &args[i], "update"))
                idx_failed = 1;
        }
    }
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    free(arena);
    bm_flush_thread_bitmap_cache();
    return idx_failed ? -1 : 0;
}
```

`v2_update_pre_commit` (`storage.c:1023-1127`) collapses to: build an
`IndexDiffApplyArgs` from its `V2UpdateCtx` fields (`old_value =
old->value`, `new_value = new_value`) and `return apply_index_diff(&args)`
— delete the now-duplicated loop body (`storage.c:1041-1124`) entirely.
`v2_insert_pre_commit` first materializes the typed `new_value` from its
JSON input using its existing conversion path, then calls the helper with
`old_value = NULL`; it cannot collapse by merely swapping one pointer.
Neither function's signature, registration (`.pre_commit =
v2_update_pre_commit` / `v2_insert_pre_commit`), or caller-visible
behavior changes — this is a pure internal refactor that both makes the
duplication go away and gives Task 7 a real function to call.

**`opts->has_indexed_fields`** — new `int` field on `SlotcaskUpsertOpts`,
set once per call site from `load_index_fields(db_root, object, buf,
MAX_FIELDS) > 0` (the established, cached pattern — `config.c:1135`,
called this way already at `storage.c:884/1274/1512`). Not recomputed per
record. When `0`, `opts->pre_commit` is also `NULL` at these call sites
(nothing to diff) — the existing `if (opts->pre_commit)` guards below are
not a substitute for this flag; they happen to agree with it, but the
marker-write/marker-clear/index-sync-pause steps must also be skipped
explicitly, which only `has_indexed_fields` gates.

**`opts->out_durability_degraded`** — new `int *` field on
`SlotcaskUpsertOpts` (mirrors `out_kf_shard`/`out_kf_slot`, same struct).
Task 8 wires it into the response.

### Authoritative indexed-write rule (supersedes the older Site 1–4 order
sketches below)

The earlier Site 1–4 excerpts were written for the rejected
index-before-kf order. Do not execute their ordering or cleanup branches.
All indexed single-record paths use this one commit-intent sequence. Do
the non-kf-dependent preparation before acquiring the kf-shard writer
lock; hold that lock only from final duplicate/CAS revalidation through
marker clear:

```c
/* New helper: sync only the pages this commit changed, retaining h's lock. */
static int kfcache_sync_slots_locked(SlotcaskKfHandle *h,
                                     const size_t *slots, size_t nslots,
                                     int header_changed) {
    if (!h || !h->writer || !h->hdr || (!slots && nslots)) {
        errno = EINVAL;
        return -1;
    }
    for (size_t i = 0; i < nslots; i++) {
        if (slots[i] >= h->capacity) { errno = EINVAL; return -1; }
        size_t off = SLOTCASK_KF_HDR_SIZE + slots[i] * sizeof(*h->map);
        if (durability_msync_range(h->hdr, off, sizeof(*h->map)) < 0)
            return -1;
    }
    return !header_changed ||
           durability_msync_range(h->hdr, 0, SLOTCASK_KF_HDR_SIZE) == 0
               ? 0 : -1;
}
```

For objects with no indexes, the same helper is the commit boundary:
segment sync → kf mutation → `kfcache_sync_slots_locked` → release → (update
only) old-segment reclaim. If that sync fails, do not reclaim the old
segment and do not report success. Retry the sync under the held lock; if
it cannot succeed, terminate the daemon before releasing the lock. The
next start sees either the old durable kf mapping or the new durable kf
mapping, both of which name an already-durable segment record.

1. Before acquisition, do every rejectable check that does not require the
   current kf mapping: schema validation, key construction, bitmap-cap
   preflight, and any resplit/growth that can return a normal error. Under
   the lock, do only the final duplicate/CAS revalidation before marker
   creation.
2. Write and `durability_msync_range` the new segment record.
3. Write and fsync the marker. For an update it contains the existing
   `kf_slot` and old/new locations. For an insert it contains
   `kf_slot = UINT32_MAX`, `has_old = 0`, and the new location only.
4. Publish/repoint kf, then call `kfcache_sync_slots_locked` for the changed
   slot (and header only if its counters changed). Insert obtains
   `put_slot` here and sets `out_kf_slot` before index work; update uses its
   already-known slot.
5. Invoke the existing index callback with that confirmed slot and force its
   touched btree/bitmap files durable. Only after both index and kf are
   durable, call `kf_marker_clear(db->data_dir, sid_kf)`.
6. Release `kh`. For updates, reclaim the old segment only if marker clear
   and its directory fsync succeeded. A present marker always keeps its old
   segment readable for recovery.

The writer lock remains held from the final revalidation through steps 2–6,
so live readers never see
the temporary kf/index mismatch. If a post-marker kf or index operation
fails, invoke the same marker replay helper synchronously while that lock
is held. If replay still cannot converge, terminate the daemon without
releasing the lock; the next start performs the mandatory recovery sweep
before accepting traffic. Do not return ordinary failure or attempt a
rollback after marker fsync. If kf+index have converged but marker clear
fails, return `durability_degraded`, retain the marker, skip old reclaim,
and require the next writer for that shard to replay and clear the marker
before creating another one.

<!-- Historical drafting notes retained only for audit; they are deliberately
hidden from rendered execution instructions because their code reflects the
rejected ordering. Do not use them as anchors or implementation material.

**Historical source reference — Site 1, existing-key/update branch.**
The following excerpt identifies the current anchor only; replace its full
commit/cleanup region with the authoritative rule above rather than
applying the stale illustrative ordering shown in this historical excerpt.
(`slotcask.c:3505-3533`). Real order today: `pre_commit` (index) runs
*before* `kf_repoint_at_slot` — this is the branch the original bug
report is about. `ex_sid`/`ex_fid`/`ex_off`/`ex_slot` are already
captured by the `kf_lookup_with_slot` call earlier in the existing-key
branch (line 3468) — this is the real old-location data, not something
new to capture:

```c
        /* Publish (shard, ex_slot) for index hooks that key by physical
           location (bitmap). On an in-place update the slot doesn't
           move — kf_repoint_at_slot below rewrites the entry without
           changing its index. */
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)ex_slot;

        if (!opts->has_indexed_fields) {
            /* No second structure to reconcile kf against — see "No-op
               for objects with zero indexed fields" above. */
            kf_repoint_at_slot(&kh, ex_slot, target_stream, target_fid, target_off);
            kfcache_release(&kh);
            if (slotcask_tombstone_and_push_back(db, ex_sid, ex_fid, ex_off) != 0) {
                if (result) result->was_update = 1;
                free(old_buf);
                return -1;
            }
            if (result) result->was_update = 1;
            free(old_buf);
            return 0;
        }

        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)ex_slot, .has_old = 1,
            .old_stream_id = ex_sid, .old_file_id = ex_fid, .old_offset = ex_off,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            free(old_buf);
            return -1;
        }
        if (opts->pre_commit) {
            int rc = opts->pre_commit(&old_rec, value, vlen, 1, opts->pre_commit_ctx);
            if (rc != 0) {
                kfcache_release(&kh);
                kf_marker_clear(db->data_dir, sid_kf);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                free(old_buf);
                return -1;
            }
        }
        durability_test_pause(db->data_dir, "update-after-index-sync");
        kf_repoint_at_slot(&kh, ex_slot, target_stream, target_fid, target_off);
        if (kfcache_release(&kh) != 0) {
            /* Marker stays active; Task 7's sweep completes it. Degraded,
               not failure — the index update IS durable. */
            *opts->out_durability_degraded = 1;
        } else {
            kf_marker_clear(db->data_dir, sid_kf);
        }
        if (slotcask_tombstone_and_push_back(db, ex_sid, ex_fid, ex_off) != 0) {
            if (result) result->was_update = 1;
            free(old_buf);
            return -1;
        }
        if (result) result->was_update = 1;
        free(old_buf);
        return 0;
```

The segment write itself (`seg_write_record`, `slotcask.c:3359-3364`,
shared by both the new-key and existing-key branches since it runs before
`kf_put_new` decides which one applies) passes `sync_now=1`
unconditionally, regardless of `has_indexed_fields` — segment-durable-
before-kf-durable is required for basic crash safety even with zero
indexes (same reasoning as the Compaction row).

**Historical source reference — Site 2, new-key/insert branch.**
Use it only to locate the branch; replace its commit/cleanup region with
the authoritative rule above.
(`slotcask.c:3379-3421`). Real order today: `kf_put_new` (at line
3376-3377, shared before the branch point) commits *before* `pre_commit`
runs — deliberately, per the existing comment at `slotcask.c:3298-3303`:
running index-before-kf here would risk writing index entries for a
record `kf_put_new` then rejects as a duplicate. Because kf is already
committed by the time this branch runs, `has_old = 0` and there is no
pre-repoint window to protect — only the index-write step needs the
marker:

```c
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)put_slot;
        if (opts->check && opts->check(NULL, opts->check_ctx) == 0) {
            uint8_t  tmp_flag = 0, tmp_sid = 0;
            uint16_t tmp_fid = 0;
            uint32_t tmp_off = 0;
            size_t   tmp_slot = 0;
            if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                     &tmp_flag, &tmp_sid, &tmp_fid, &tmp_off,
                                     &tmp_slot) == 0) {
                kf_tombstone_at_slot(&kh, tmp_slot);
            }
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            if (result) result->condition_not_met = 1;
            return -2;
        }

        if (!opts->has_indexed_fields) {
            kfcache_release(&kh);
            if (result) result->was_update = 0;
            return 0;
        }

        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)put_slot, .has_old = 0,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            uint8_t  tmp_flag = 0, tmp_sid = 0;
            uint16_t tmp_fid = 0;
            uint32_t tmp_off = 0;
            size_t   tmp_slot = 0;
            if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                     &tmp_flag, &tmp_sid, &tmp_fid, &tmp_off,
                                     &tmp_slot) == 0) {
                kf_tombstone_at_slot(&kh, tmp_slot);
            }
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
        if (opts->pre_commit) {
            int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
            if (rc != 0) {
                uint8_t  tmp_flag = 0, tmp_sid = 0;
                uint16_t tmp_fid = 0;
                uint32_t tmp_off = 0;
                size_t   tmp_slot = 0;
                if (kf_lookup_with_slot(&kh, hash, key, klen, db->data_dir,
                                         &tmp_flag, &tmp_sid, &tmp_fid, &tmp_off,
                                         &tmp_slot) == 0) {
                    kf_tombstone_at_slot(&kh, tmp_slot);
                }
                kf_marker_clear(db->data_dir, sid_kf);
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                return -1;
            }
        }
        kf_marker_clear(db->data_dir, sid_kf);
        kfcache_release(&kh);
        if (result) result->was_update = 0;
        return 0;
```

No `out_durability_degraded` branch here: kf is already committed and
durable-pending before this point (the shared seg write above already
synced synchronously, and `kfcache_release` for the *insert* case only
happens at the very end, after the marker is already cleared — so unlike
the update branch, there's no "index synced but kf sync still pending"
gap to report as degraded; `kfcache_release`'s own failure here is
reported through its existing return path, unchanged from today).

**Historical source reference — Site 3, `upsert_slow_path`**
(`slotcask.c:3545-3742`). Use the excerpt only to locate the branch;
replace its commit/cleanup region with the authoritative rule above.
It is structurally
different from the two fast-path branches: it takes the kf wrlock
*before* writing anything (`kfcache_acquire` at 3555, before the segment
write at 3654-3673), so there is no duplicate-rejection race to protect
against — `found` (whether the key already existed) is already resolved
by the time `pre_commit` runs. This lets it use **one uniform order for
both insert and update**: `pre_commit` (index) always before the kf
commit (`kf_repoint_at_slot` or `kf_put_new`, whichever `found` selects,
at 3701-3714). `has_old = found`:

```c
        /* Publish (shard, slot) for index hooks... [unchanged, 3675-3685] */
        if (found) {
            if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
            if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)kf_slot;
        }

        if (!opts->has_indexed_fields) {
            int kf_rc2;
            if (found) {
                kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
                kf_rc2 = 0;
            } else {
                size_t used_delta = 0;
                kf_rc2 = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                                    key, klen, db->data_dir, &used_delta, NULL);
                if (kf_rc2 == 1) kf_rc2 = -1;
            }
            if (kf_rc2 != 0) {
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
            kfcache_release(&kh);
            if (found && slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off) != 0) {
                if (result) result->was_update = 1;
                free(old_buf);
                return -1;
            }
            if (result) { result->was_update = found ? 1 : 0; result->condition_not_met = 0; }
            free(old_buf);
            return 0;
        }

        /* kf_slot is only meaningful when found=1; kf_put_new below picks
           the slot for a fresh insert, so the marker's kf_slot field is
           filled in after the kf commit for that case (see below) — the
           marker write for the insert case is deferred one step later
           than the update case for exactly this reason. */
        if (found) {
            KfMarkerSlot marker = {
                .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)kf_slot, .has_old = 1,
                .old_stream_id = old_sid, .old_file_id = old_fid, .old_offset = old_off,
                .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
            };
            if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
        }
        if (opts->pre_commit) {
            int rc = opts->pre_commit(old_ptr, value, vlen, found, opts->pre_commit_ctx);
            if (rc != 0) {
                if (found) kf_marker_clear(db->data_dir, sid_kf);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                kfcache_release(&kh);
                free(old_buf);
                return -1;
            }
        }
        durability_test_pause(db->data_dir, "update-after-index-sync");
        int kf_rc;
        size_t committed_slot = kf_slot;
        if (found) {
            kf_repoint_at_slot(&kh, kf_slot, target_stream, target_fid, target_off);
            kf_rc = 0;
        } else {
            size_t used_delta = 0;
            kf_rc = kf_put_new(db, &kh, hash, target_stream, target_fid, target_off,
                               key, klen, db->data_dir, &used_delta, &committed_slot);
            if (kf_rc == 1) kf_rc = -1;
        }
        if (kf_rc != 0) {
            if (found) kf_marker_clear(db->data_dir, sid_kf);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            kfcache_release(&kh);
            free(old_buf);
            return -1;
        }
        if (!found) {
            /* Insert case: marker written now that kf_put_new has picked
               a slot — covers the same "index applied, crash before this
               point is durably visible" window as the found=1 case, just
               anchored to the slot kf_put_new just chose. Written after
               the commit (mirrors Site 2's insert ordering: kf-then-
               marker for inserts), so has_old=0 and there is no earlier
               "was it repointed yet" ambiguity for recovery to resolve. */
            KfMarkerSlot marker = {
                .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)committed_slot, .has_old = 0,
                .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
            };
            kf_marker_write(db->data_dir, sid_kf, &marker);
            /* Best-effort: index is already durable (pre_commit above
               succeeded and this task's index-sync step, wired into
               update_idx_fn/bitmap_update per the unchanged first-draft
               Task 2c, already made it so) and kf is now committed too —
               the marker here exists only to protect the interval this
               function itself already closed by running index-before-kf;
               a write failure here is logged, not fatal (kf_marker_write
               failing after the operation has already fully committed
               must not turn a successful write into a reported error). */
        }
        if (kfcache_release(&kh) != 0) {
            *opts->out_durability_degraded = 1;
        } else {
            kf_marker_clear(db->data_dir, sid_kf);
        }
        if (found) {
            if (slotcask_tombstone_and_push_back(db, old_sid, old_fid, old_off) != 0) {
                if (result) result->was_update = 1;
                free(old_buf);
                return -1;
            }
        }
        if (result) { result->was_update = found ? 1 : 0; result->condition_not_met = 0; }
        free(old_buf);
        return 0;
```

**Historical source reference — Site 4, `slotcask_insert_with_hooks`**
(`slotcask.c:3748-3918`). Use the excerpt only to locate the branch;
replace its commit/cleanup region with the authoritative rule above.
Insert-only; `require_existing` is rejected at the top (3771), so
`has_old` is always `0`. Same shape as Site 2 (kf commits at
`kf_put_new`, 3857-3858, before `pre_commit` at 3906-3914, deliberately,
per the identical duplicate-rejection reasoning in this function's own
header comment at 3744-3747):

```c
        if (opts->out_kf_shard) *opts->out_kf_shard = sid_kf;
        if (opts->out_kf_slot)  *opts->out_kf_slot  = (uint32_t)put_slot;

        if (!opts->has_indexed_fields) {
            kfcache_release(&kh);
            return 0;
        }

        KfMarkerSlot marker = {
            .magic = KF_MARKER_MAGIC, .kf_slot = (uint32_t)put_slot, .has_old = 0,
            .new_stream_id = target_stream, .new_file_id = target_fid, .new_offset = target_off,
        };
        if (kf_marker_write(db->data_dir, sid_kf, &marker) != 0) {
            kfcache_release(&kh);
            seg_write_flag(db, target_stream, target_fid, target_off, 2);
            pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
            return -1;
        }
        if (opts->pre_commit) {
            int rc = opts->pre_commit(NULL, value, vlen, 0, opts->pre_commit_ctx);
            if (rc != 0) {
                kf_marker_clear(db->data_dir, sid_kf);
                kfcache_release(&kh);
                seg_write_flag(db, target_stream, target_fid, target_off, 2);
                pool_push_free_cap(pool, target_fid, target_off, slot_capacity, db->slot_size);
                return -1;
            }
        }
        kf_marker_clear(db->data_dir, sid_kf);
        kfcache_release(&kh);
        return 0;
```

Marker helpers take `db->data_dir` directly. `SlotcaskDb` has no
`db_root`, `dir`, or `object` members, so no path decomposition or new
parameter plumbing is permitted at these call sites.

-->

**Authoritative integration matrix.** Replace, rather than edit around,
the commit/cleanup regions anchored by these exact source strings:

| Path | Quoted anchor | Required replacement |
|---|---|---|
| `slotcask_upsert_with_hooks`, existing | `if (opts->pre_commit) {` after `ex_slot` is captured | segment was already synced; final revalidation → retained-marker gate → marker write → `kf_repoint_at_slot` → targeted kf sync → `opts->pre_commit`/index sync → marker clear → release → old reclaim. |
| `slotcask_upsert_with_hooks`, new | `kf_put_new(db, &kh, hash,` | move this publication below marker write; use `UINT32_MAX` in the marker, capture `put_slot`, then targeted kf sync before index work. |
| `upsert_slow_path` | `int kf_rc;` | use the same new/update branch sequence as above; the pre-existing lock only changes where final revalidation occurs, not commit order. |
| `slotcask_insert_with_hooks` | `kf_put_new(db, &kh, hash,` | same insert sequence as the new branch above. |

For every row, a marker-write failure is an ordinary pre-commit failure:
release the lock and recycle only the unreferenced new segment. Once
`kf_marker_write` succeeds, execute only the fail-closed replay rule; do
not clear the marker, tombstone either segment, or return an ordinary error
until kf and indexes converge. The executor must replace the whole region,
not preserve any stale `pre_commit`-before-kf, `kf_put_new`-before-marker,
or `kfcache_release`-return-value branch hidden above.

**Index durability uses per-touched-file `fdatasync`, not a new
`IndexSyncSet` subsystem.** This is the deliberately narrower interface
change: it avoids a whole-cache/background sweep and does not require the
executor to invent page-dirty propagation through the recursive btree.
`btree_sync_path` is a new `btree.c` internal/exported helper that acquires
that one path's existing btree writer lock, calls `fdatasync(bt.fd)`, then
releases it. `bm_sync(BitmapShard *bm)` calls `fdatasync(bm->fd)` before
`bm_close` releases that bitmap's existing writer lock. Add synchronous
btree wrappers used only by CRUD/recovery `update_idx_fn`:

```c
/* src/db/btree.c, after btree_delete; declare in btree.h */
int btree_sync_path(const char *path) {
    BtFile bt;
    if (bt_acquire(&bt, path, 1) != 0) return -1;
    int rc = fdatasync(bt.fd);
    bt_release(&bt);
    return rc;
}

/* src/db/bitmap.c, declare in bitmap.h */
int bm_sync(BitmapShard *bm) {
    if (!bm || !bm->writer || bm->fd < 0) { errno = EINVAL; return -1; }
    return fdatasync(bm->fd);
}

/* src/db/index.c, new static helper beside btree_idx_insert/delete */
static int btree_idx_mutate_sync(const char *db_root, const char *object,
                                 const char *field, int splits,
                                 const char *value, size_t vlen,
                                 const uint8_t hash[BT_HASH_SIZE], int insert) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char path[PATH_MAX];
    build_idx_path(path, sizeof(path), db_root, object, field, idx_shard);
    int rc = insert ? btree_insert(path, value, vlen, hash)
                    : btree_delete(path, value, vlen, hash);
    return rc == 0 ? btree_sync_path(path) : rc;
}
```

`write_index_entry`/`delete_index_entry` and the trigram equivalents use
this helper only on the marker-governed CRUD, bulk-window, and recovery
paths; index builds/reindex retain their existing asynchronous/batch
durability behavior. `bitmap_update` calls `bm_sync(bm)` after all
`bm_clear`/`bm_set`/`bm_grow` work succeeds and before `bm_close`. A
foreground commit therefore waits only for the btree/bitmap files its own
`UpdateIdxArg` changed, never every cached index mapping. `kfcache_release`
remains its existing `void` unlock/dirty-marking primitive.

**Replay idempotence includes btree counters.** `bt_insert_rec` currently
detects an identical `(value, hash)` leaf entry but `btree_insert` still
increments `entry_count`/`insert_count` afterwards. Before using it for
marker replay, introduce these three explicit results and propagate them
unchanged through the internal recursive case:

```c
enum { BT_INSERT_NO_SPLIT = -1, BT_INSERT_SPLIT = 0, BT_INSERT_DUPLICATE = 1 };
```

The identical-entry branch returns `BT_INSERT_DUPLICATE`; every caller of
`bt_insert_rec` returns it immediately without modifying a parent; and
both `btree_insert` and `btree_insert_batch` update header counters only
when the result is not `BT_INSERT_DUPLICATE`. A test inserts the same tuple
twice, including through marker replay, and asserts one search result and
unchanged header counters after the second call. `btree_delete` is already
idempotent for an absent tuple, so no analogous change is required.

**Task 3 done-check**: run all four 3a tests, paste passing. Then revert
3b (`git stash`), re-run test 1 and confirm it fails for the stated
reason (seg-sync now unchecked again), paste. Re-apply, paste final green
run. Test 2 (the canary) doesn't have a meaningful "revert and re-fail"
step in the same sense, since its base-branch evidence is the cited first
draft's equivalent test rather than a literal re-run — note that
explicitly in the paste rather than fabricating a revert-run that isn't
meaningful.

## Task 4 — delete stays as designed in the first draft (no change)

Delete's lazy index removal (self-heals via kf flag check) is unaffected
by this rewrite. No marker; its kf tombstone uses the same targeted kf
slot/header sync as every other foreground kf mutation. `kfcache_release`
only records background-flush state and is not its durability boundary.
Carry the first draft's Task 2b test 4 (`delete stays lazy under
fail-every-msync injection`) forward unchanged.

## Task 5 — bulk paths: a bounded per-commit-window marker file

### Authoritative bulk commit order

The older Phase-4 description below is retained only as source-path
research; its pre-commit-before-kf ordering is superseded. Split each kf
shard's Phase 4 into consecutive windows of at most
`BULK_COMMIT_MAX_RECORDS = 256`. For every indexed record that survived
one window's validation phase, write and fsync
its marker-array slot **before** its kf mutation. The marker slot carries
`UINT32_MAX` for an insert and the existing slot for an update. Then, while
the one kf-shard writer lock remains held: publish/repoint that window's records,
sync the changed kf-slot pages once with `kfcache_sync_slots_locked`, run and sync every
record's index diff using its confirmed kf slot, and finally unlink+fsync
the window marker file. Reclaim old segments only after that unlink succeeds.

Every duplicate/CAS/index-cap/resplit check must be completed before the
first window-marker slot is fsynced. A post-marker error follows the same
fail-closed rule as a single write: replay under the held lock, otherwise
terminate the daemon and let startup recovery finish before serving reads.
Recovery processes each marker slot using the same insert/update kf-first
algorithm specified above; for an insert it derives the slot by looking up
the key in the durable new segment record.

<!-- Historical current-code notes only: do not preserve this loop order.
Verified against the real body: `bulk_upsert_slow_in_kfshard`'s Phase 4
(`slotcask.c:4452-4532`) commits kf and runs `pre_commit` per record
inside one loop, but `kfcache_release` for the whole batch against one
shard is called **once**, at `slotcask.c:4534`, after the loop exits —
amortizing the kf sync cost across every record in the batch is the
entire point of batching, so a per-record kf sync is not an option here.
Per-record commit order mirrors the two fast-path sites from Task 3b, not
`upsert_slow_path`'s uniform order: for a new key (`!st[i].old_found`),
`kf_put_new` commits at `4471-4474` *before* `pre_commit` runs at
`4490-4509` (`has_old=0`, same rationale as Task 3b Sites 2/4 — avoid
stale index entries if `kf_put_new` finds a late duplicate); for an
existing key (`st[i].old_found`), `pre_commit` runs first and
`kf_repoint_at_slot` commits after, at `4516-4517` (`has_old=1`, same as
Task 3b Site 1). Each record's batch-marker slot is filled in with the
`has_old` value matching its own branch, not a single value for the
whole batch.

The single end-of-loop `kfcache_release` is incompatible with reusing the
single-record `<NNN>_marker.dat` file per record: since it's one file
identified only by shard number, record 2's marker write would silently
overwrite record 1's still-uncommitted marker before record 1's kf state
is ever synced (kf isn't synced until every record in the shard's batch
has gone through the loop), losing record 1's recovery info entirely if a
crash lands between the two — a real bug, not just an ordering nuance,
caught precisely because the marker is a single file per shard rather
than an array.
-->

**Fix: bulk uses a separate, transient, per-shard-per-commit-window file**,
`data/kf/<NNN>_batch_marker.dat` (distinct name from the single-record
marker, so the two mechanisms never collide — though they couldn't run
concurrently against the same shard anyway, since both are covered by
the same shard wrlock). Reuses `KfMarkerSlot` unchanged, but as an
**array** sized to one window's record count for this shard
(`window_n <= 256`). For each window, create it once (`open`+directory
fsync), write+fsync each slot, publish and targeted-sync only that window,
sync its index changes, then delete it with one `unlink`+directory-fsync
before starting the next window. The fixed filename is safe because the
previous window's marker is absent before the next one is created. This
keeps the marker lifecycle atomic without allowing an unbounded bulk
request to monopolize the kf writer lock.

Failure handling: a marker-write failure before its slot is fsynced is an
ordinary failure for that record and it is omitted from the window. An
index-sync failure after any window marker slot is fsynced is post-commit:
replay the complete window under the held lock or fail closed. It is never
reported as a per-record ordinary failure. `durability_degraded` remains
reserved for the safe case where the window converged but its final unlink
or directory fsync failed.
A `kfcache_sync_slots_locked` failure at the end of a window affects
**every** record in that window. Its marker file is not deleted, so every
one of those records remains recoverable via Task 7's sweep; prior cleared
windows remain complete and later windows have not started.

Recovery (Task 7's sweep, extended): glob `*_batch_marker.dat` alongside
`*_marker.dat` in each object's `data/kf/`. A found batch marker is
walked slot by slot, each valid (`magic` intact) slot redone exactly
like a single-record marker (idempotent index + kf redo), then the whole
file is deleted (directory fsynced) once every slot has been processed —
matching the "delete only after everything in it is confirmed" rule the
live path already follows.

Regression test: multi-record window (≥3 records, same shard), force a
failure in that window's `kfcache_sync_slots_locked`, assert `data/kf/<NNN>_batch_marker.dat`
still exists and contains every record's slot immediately after, and
that a subsequent (simulated, via the same recovery-sweep entry point
Task 7 adds — call it directly in-test rather than restarting the
daemon, if that entry point is exposed as a standalone function)
recovery pass processes every slot, deletes the batch marker file, and
leaves the index/kf state correct for all of them. Also test the
overwrite bug directly as a regression case against the *first version
of this task's design*: two records in one batch targeting the same
shard, crash injected between record 1's and record 2's marker writes
under the old (rejected) per-record-reusing-the-shard-file scheme —
included here as documentation of why the per-batch file exists, not as
a scenario that needs to pass against current code (the old scheme never
shipped).

**Window-boundary regression test**: route 257 indexed records to one kf
shard, pause after the first window's marker clear, and issue a same-shard
read. It must complete while the second window has not started. Assert two
create/fsync/clear marker lifecycles, each no larger than 256 slots, and
that a crash in the second window recovers only its marker while the first
window remains already complete.

**Task 5 done-check**: same base-fail → revert-fail → reapply-pass
sequence, for the marker-ordering-hazard test specifically.

## Task 6 — compaction, and the unrelated `bt_alloc_page` fsync gap

**Compaction**: unchanged reasoning from the first draft (no marker
needed — two-entity chain, kf is the final commit point) but the sync
call must use `durability_msync_range`, not raw `durability_msync`:

```c
    seg_record_emit(c->rmap + target_off, c->db->slot_size,
                    hash16, key, (size_t)klen, value, (size_t)vlen);
    if (durability_msync_range(c->rmap, target_off, (size_t)c->db->slot_size) != 0) {
        c->rc = -1;
        return 1;
    }
    durability_test_pause(c->db->data_dir, "compact-after-recipient-sync");
```
(anchor and surrounding context unchanged from the first draft's Task
4a — only the sync call itself changes, from `durability_msync(c->rmap +
target_off, ...)` to the range-safe form above. `varlen_compact_cb` gets
the identical substitution.)

**`bt_alloc_page` allocation durability**: replace the complete function
anchored by `static uint32_t bt_alloc_page(BtFile *bt) {` with this body.
It must extend and `fsync` the file **before** remapping it; the current
order maps beyond EOF before checking `ftruncate`, which can SIGBUS.
`bt_alloc_page` has no error return and all 11 callers assume a valid page,
so matching its existing mmap-failure precedent with `abort()` is required;
do not invent partial return-code plumbing.

```c
static uint32_t bt_alloc_page(BtFile *bt) {
    BtFileHeader *fh = (BtFileHeader *)bt->map;
    uint32_t new_id = fh->page_count;
    size_t needed = (size_t)(new_id + 1) * bt_page_size;

    if (needed > bt->map_size) {
        size_t old_size = bt->map_size;
        size_t new_size = old_size * 2;
        if (new_size < old_size + 1024 * 1024)
            new_size = old_size + 1024 * 1024;
        if (new_size < needed) new_size = needed;
        if (ftruncate(bt->fd, (off_t)new_size) < 0 || fsync(bt->fd) < 0) {
            fprintf(stderr, "btree: allocation grow %zu→%zu failed: %s\n",
                    old_size, new_size, strerror(errno));
            abort();
        }
#ifdef __linux__
        void *new_map = mremap(bt->map, old_size, new_size, MREMAP_MAYMOVE);
        bt->map = new_map == MAP_FAILED ? NULL : new_map;
#else
        munmap(bt->map, old_size);
        void *new_map = mmap(NULL, new_size, PROT_READ | PROT_WRITE,
                             MAP_SHARED, bt->fd, 0);
        bt->map = new_map == MAP_FAILED ? NULL : new_map;
#endif
        if (!bt->map) {
            fprintf(stderr, "btree: page allocation remap failed: %s\n", strerror(errno));
            abort();
        }
        bt->map_size = new_size;
        if (bt->slot >= 0) {
            BtCacheEntry *e = &bt_cache[bt->slot];
            e->map = bt->map;
            e->map_size = new_size;
        }
        fh = (BtFileHeader *)bt->map;
    }

    fh->page_count = new_id + 1;
    uint8_t *pg = bt_page(bt, new_id);
    memset(pg, 0, bt_page_size);
    return new_id;
}
```

**Task 6 done-check**: unchanged from the first draft's Task 4 done-check,
substituting the range-safe sync call in the pasted evidence.

## Task 7 — clean-shutdown flag and startup recovery sweep

Implements the "Recovery sweep" section above.

1. **Test first**: start a daemon, insert/update some records normally,
   graceful `stop`, assert `$DB_ROOT/.shard-db.clean` exists. `start`
   again, assert it's been deleted immediately (read it inside a tight
   poll loop right after issuing `start`, since the deletion needs to
   race the check meaningfully — or better, have the recovery-sweep
   function itself log/expose whether it ran, and assert it did *not*
   run, via a stats counter or log line, rather than trying to catch the
   file mid-deletion).
2. **Test**: `SIGKILL` a daemon mid-write (any of Task 3's pause phases),
   assert `.shard-db.clean` is absent (it was deleted at the *previous*
   startup and never recreated since this run didn't stop cleanly),
   restart, assert the recovery sweep ran (same stats-counter/log
   mechanism) and that the affected marker was cleared and index/kf state
   is correct — this subsumes and formalizes Task 3's canary test as an
   actual full-cycle (crash → restart → verify) test rather than the
   cited-evidence approximation Task 3 had to use for its `main`-baseline
   comparison.
3. Implement: after the graceful-stop drain, enumerate marker filenames
   using the same object walk as recovery. Only when none remain, create
   `.shard-db.clean`, `fsync` it, then `fsync` `$DB_ROOT`; otherwise leave
   it absent. On startup, acquire the instance
   lock, `stat` the flag into `was_clean`, unlink it, and fsync `$DB_ROOT`
   before any database mutation or worker/listener starts. Run the sweep
   iff `was_clean == 0`, always before accepting connections. The test
   exposes a `recovery_ran` test-only counter instead of racing a file
   deletion.
4. **Corrupt-marker policy test**: with `.shard-db.clean` absent, create a
   non-empty `NNN_marker.dat` with an invalid checksum; assert startup fails
   before binding the listener and the file remains. Repeat with a zero-byte
   marker; assert startup succeeds, deletes it with a directory fsync, and
   records `recovery_ran=1`. This is Task 2's integration test, not a stub.

**Task 7 done-check**: base-fail → revert-fail → reapply-pass for test 2
(the only one with genuine crash-recovery behavior to prove); test 1 is
new-code TDD (compile-fail → pass), same distinction as Task 2.

## Task 8 — durability-degraded wire protocol (simplified by the marker)

**SKIPPED — human approval (2026-07-25).** Rationale: durability itself
does not depend on this field — it is a pure observability signal for
the rare "kf+index converged but marker cleanup itself failed, or the
marker was left for crash-recovery to replay" case. The existing
`marker_recovery_ran` stats field plus server logs already surface that
signal at the operational level (recovery ran / didn't run after a
restart), which is what operators actually watch. A per-write/per-key
wire field on top of that was judged to be complexity nobody would
consume, for a case that is already safe by construction (no data loss
either way — see the "Authoritative indexed-write rule" section above).
Implementation was drafted and then reverted in full (single-record
`out_durability_degraded` plumbing in `cmd_insert_v2`/`cmd_update_v2`,
a per-record `durability_degraded` flag on `SlotcaskBulkRec`, and
aggregate/per-key surfacing in the four bulk workers) — no trace of it
remains in the diff. `cmd_delete_v2` was never in scope: `SlotcaskDeleteOpts`
has no marker/durability-related fields at all, matching Task 4's
"no change" designation for delete.

**What changed from the first draft's Task 5** (historical context, not
implemented): with recovery in place,
"degraded" means the kf and indexes have already converged but the final
marker unlink/directory fsync failed. The response is safe because reads
are correct; the old record is deliberately retained and the next writer
or restart replay clears the marker. A kf/index sync failure that cannot
be replayed in-process is fail-closed: the daemon exits and no response is
emitted, so it is not represented by this field. The field shape stays the
same for continuity:

```json
{"status":"updated","key":"42","durability_degraded":true}
```
```json
{"status":"bulk-updated","count":3,"skipped":0,"durability_degraded_keys":["7","500"]}
```

Consumers to update: `cmd_insert_v2`/`cmd_update_v2`/`cmd_delete_v2`, the
six bulk workers, [the CAS response reference](docs/query-protocol/cas.md),
and [the auto-key/bulk response reference](docs/query-protocol/schema-mutations.md),
reading the new
`out_durability_degraded` out-param Task 3b threads through instead of
the first draft's `ctx->durability_degraded`; set it only for the safe
marker-clear failure described above.

**Task 8 done-check**: unchanged in shape from the first draft's Task 5
done-check — assert the field is present on a forced-failure run, paste.

## Lock-hold budget — mandatory constraints for every task above

The marker protocol must not turn a per-shard kf writer lock into a
whole-file I/O lock. These rules are correctness-and-performance
requirements, not suggestions; do not substitute a simpler whole-map
`msync` while the writer lock is held.

1. **Keep pre-commit preparation outside the kf writer lock.** On the
   fast paths, reserve/write/sync the new segment before `kfcache_acquire`,
   as the current fast path already does. Also build typed index keys,
   allocate `UpdateIdxArg` storage, and perform schema/index-cap preflight
   before acquisition whenever they do not require the current kf mapping.
   CAS/require-existing paths may briefly hold the lock to copy and validate
   the old record, but must not perform JSON decoding, heap growth, schema
   loading, or worker-pool setup while it is held. If moving a preparation
   step outside would invalidate CAS semantics, keep that narrow step under
   the lock and document why in `PLAN_NOTES.md`; never silently weaken CAS.

2. **Targeted kf sync, never whole-shard sync on the commit path.** Add
   `kfcache_sync_slots_locked(SlotcaskKfHandle *h, const size_t *slots,
   size_t nslots, int header_changed)`. It keeps the existing writer lock,
   flushes exactly the pages containing the supplied `h->map[slot]` values,
   and additionally the header page when `kf_put_new`/`kf_tombstone_at_slot`
   changed counters. It uses `durability_msync_range` from Task 1.
   Single-record commits call it with one slot; bulk commits pass the
   chunk's slots, sorted and de-duplicated by containing page. Do not call
   `durability_flush_dirty(..., e->map_size)` during a foreground commit. A targeted flush must not
   clear another writer's dirty indication: retain/restore the cache dirty
   bit for the background sweep, or add a dirty-generation/range tracker
   and prove it cannot lose a concurrent dirty page.

3. **One touched index file at a time.** Do not add speculative
   `IndexSyncSet` page-tracking to this change. CRUD/recovery btree writes
   use `btree_idx_mutate_sync` and its one-file `fdatasync`; bitmap writes
   call `bm_sync` before releasing their existing writer lock. Never call
   `durability_flush_dirty` on a cache-wide mapping or invoke the background
   sweep from the foreground commit. Tests cover a btree split and bitmap
   growth, asserting exactly the mutated index path reaches the injected
   `fdatasync` hook and an unrelated cached index path does not.

4. **Hold the kf writer lock only across the irreducible commit window.**
   That window is: final duplicate/CAS revalidation → marker fsync → kf
   publish plus targeted kf sync → index mutation plus targeted index sync
   → marker unlink/directory fsync. It intentionally remains one window so
   same-shard readers cannot observe a live kf/index mismatch. Never drop
   and reacquire it in the middle of that sequence.

5. **Bound bulk commits.** Split each kf-shard's bulk Phase 4 into
   `BULK_COMMIT_MAX_RECORDS = 256` records per commit window. Each window
   gets its own batch marker, targeted kf/index sync set, marker clear, and
   old-record reclaim. Preserve the existing per-record result semantics;
   the public bulk operation is already allowed to report individual
   failures, so chunking does not invent a new all-or-nothing contract.
   A test with 257 records routed to one kf shard must observe two separate
   marker lifecycles and allow a reader between them to complete.

6. **Measure rather than assume.** Add per-shard `commit_lock_hold_us` and
   `commit_sync_us` counters to `stats`, plus a warning when a foreground
   commit exceeds `SLOW_QUERY_MS`. This is diagnostic only: never abandon
   or time out an in-progress `fsync`/`msync`, because it cannot safely be
   cancelled. The required before/after benchmark report includes p50/p95/
   p99 lock-hold duration for single insert/update/delete and 1k/10k bulk
   writes, in addition to throughput.

## Performance note

Adds, beyond the first draft's per-operation `msync` costs: one
`pwrite`+`fsync` for the marker write, one more for the marker clear —
two small synchronous fsyncs per insert/update (bulk: two per record,
per Task 5's per-record marker requirement), on top of the seg/index/kf
syncs already costed in the first draft. This is the real, stated price
of closing the permanent-false-positive gap rather than just narrowing
it. Benchmark before/after per the first draft's existing instruction;
expect a larger regression than the first draft estimated, specifically
attributable to the two marker fsyncs, and report it as its own line
item, not folded into the general "synchronous msync isn't free" note.

### Lock-hold budget results (2026-07-25/26)

**Constraints #1-5 verified by direct code inspection** (not per-task
re-audit — this whole diff already passed one full ASan+TSan gate and
review earlier in the branch's history; this was a final structural
recheck against the 5 rules):

1. Fast path (`!has_indexed_fields`): `kfcache_sync_slots_locked` +
   `kfcache_release` immediately, no marker involvement
   (`slotcask.c:3906-3924`).
2. `kfcache_sync_slots_locked` (targeted) is used on every commit path.
   The two whole-mapping `durability_flush_dirty` call sites in
   `slotcask.c` (lines 228, 1012) are both gated by
   `reason == CACHE_DROP_EVICT` — background cache eviction, never the
   foreground commit.
3. `grep -rn "IndexSyncSet" src/db/` returns nothing — no speculative
   page-tracking was added.
4. Marker-guarded commit window (`slotcask.c:3926-3971`+) runs marker
   write → targeted kf sync → `pre_commit` (index mutation) → marker
   clear as one unbroken sequence; `kfcache_release` only happens once,
   at the end of the success path or on an early-abort branch — never
   mid-sequence-then-reacquire.
5. `BULK_COMMIT_MAX_RECORDS = 256` (`slotcask.c:85`) drives the windowed
   loop in `slotcask_bulk_upsert_in_kfshard`.

**Instrumentation added** (global-aggregate scope, chosen over
per-shard arrays via human approval — see conversation): `commit_count`,
`commit_lock_hold_us_total` (wall-clock around the
`slotcask_insert_with_hooks` / `slotcask_upsert_with_hooks` /
`slotcask_bulk_upsert_in_kfshard` call at each of the 5 call sites:
`cmd_insert_v2`, `cmd_update_v2`, and the 3 bulk-update workers, plus
the bulk-insert worker — 6 sites total), and `commit_sync_us_total`
(wrapping the 5 durability-primitive functions:
`kf_marker_write/clear`, `kfcache_sync_slots_locked`,
`kf_batch_marker_write/clear`, each split into a `_impl` body + a thin
timing wrapper so no caller or internal branch needed touching). A
per-commit warning (`LOG_WARN(LOG_SUB_DURABILITY, ...)`) fires when a
single commit's lock-hold exceeds `SLOW_QUERY_MS`. All three counters
are exposed in `stats` (text + JSON) and `stats-prom`.

**Benchmark**: `bench-kv` (1M unindexed records) and `bench-invoice`
(1M records, 14 indexes added after bulk-load) run before
(`git stash` to bare `main`, i.e. pre-durability-work) and after
(current branch), single run each, same machine, no averaging —
directional, not lab-grade:

| Bench / op | Before | After | Δ |
|---|---|---|---|
| bench-kv UPDATE x10000, p50 (unindexed, no markers) | 37µs | 38µs | ~noise |
| bench-kv DELETE x10000, p50 (unindexed, no markers) | 38µs | 38µs | ~noise |
| bench-kv BULK UPDATE x10000, one call (unindexed) | 12.4ms | 12.2ms | ~noise |
| bench-kv BULK DELETE x10000, one call (unindexed) | 7.7ms | 6.7ms | ~noise |
| bench-invoice BULK INSERT 1M, single JSON call (pre-index, no markers) | 9186ms | 9644ms | +5% (likely disk-cache noise, single run) |
| bench-invoice BULK UPDATE x10000 (indexed, **marker-guarded**) | 59.4ms | 60.5ms | +1.8% |
| bench-invoice SINGLE DELETE x1000, p50 (14 idx, **no marker** — Task 4) | 77µs | 78µs | ~noise |

**Caveat**: neither bench exercises a single-record marker-guarded
INSERT/UPDATE latency in isolation (`bench-kv`'s object carries no
indexes at all; `bench-invoice` only measures single-record latency for
DELETE, which Task 4 explicitly keeps marker-free). The one
marker-guarded op both benches do cover at meaningful volume —
`bench-invoice`'s indexed BULK UPDATE — shows the two extra marker
fsyncs per 256-record window costing under 2%, well inside single-run
noise. This is consistent with the marker fsync being a small,
targeted write (one `KfMarkerSlot`, not a whole-file sync) rather than
the larger regression the Performance note above anticipated;
Linux's write-back cache is doing the heavy lifting for these
in-page-cache single-machine runs; production numbers on a
fsync-honest device (real HDD/cloud-network-disk, not this dev
machine's NVMe/tmpfs-backed test paths) would be expected to show more.
No task in this plan claims the regression must stay under a specific
number — this section exists to make the actual cost visible, which it
now does via both the benchmark above and the `stats`/`stats-prom`
counters for ongoing production observation.

## Execution rules

Unchanged from the first draft's Execution rules section — branch name,
build/test commands, task-order-with-tests-first discipline, anchor
re-verification requirement, `PLAN_NOTES.md` halt protocol, uncommitted-
until-reviewed, full ASan/TSan gate before handoff — with the task list
updated to: 1 (msync range helper) → 2 (marker infra) → 3 (single-record,
TDD, the canary) → 4 (delete, no change) → 5 (bulk) → 6 (compaction +
`bt_alloc_page`) → 7 (clean-shutdown flag + recovery sweep) → 8 (wire
protocol).

Additional re-verify-at-execution-time items specific to this rewrite,
on top of the first draft's list: the bulk Phase-4 loop's current
per-record vs. batch-deferred structure (determines whether the
marker-clear-ordering hazard flagged in Task 5 already has a natural home
or needs restructuring — see Task 5's note on `bulk_upsert_slow_in_kfshard`
below), and `cmd_server`'s exact drain/stop sequence (for the
`.shard-db.clean` write point).

## Resolution log (second round)

1. **msync page-alignment (`EINVAL`).** Fixed: `durability_msync_range`
   (new section, used everywhere instead of raw `durability_msync` for
   any offset-into-a-larger-mapping call).
2. **Index-before-kf isn't safe, just symmetric — permanent, not
   bounded.** Accepted in full; the "Design rationale" section replaces
   the first draft's "why the code's order is correct" argument entirely.
   The fix is no longer about ordering at all — it's the marker, which
   makes recovery redo whichever half is missing regardless of order,
   closing the gap rather than relocating it.
3. **Missing `update-after-index-clear-sync` boundary.** Resolved by
   design: the marker makes the whole index diff replayable, so the
   canary pauses at the actual cross-file boundary,
   `update-after-kf-sync`, and proves startup recovery completes the
   index before reads resume.
4. **Deferred decisions ("executor picks," "likely," etc).** Every
   instance in this rewrite either states the concrete choice directly or,
   where current-code shape genuinely can't be confirmed without
   re-reading the working tree at execution time (a handful, explicitly
   listed in "Execution rules"), gives the executor a decision rule to
   apply once they look, not an open choice. Task 3b's code blocks were
   independently re-verified against the real bodies of
   `slotcask_upsert_with_hooks`, `upsert_slow_path`, and
   `slotcask_insert_with_hooks` (`slotcask.c`) and `seg_write_record`/
   `seg_write_record_varlen` during this drafting pass — an earlier
   internal draft of this section had invented three functions
   (`upsert_fast_path`, `seg_write_record_body`, `seg_activate_record`)
   that do not exist in the codebase; that draft was discarded and
   replaced with the current section, anchored to real line numbers and
   real function bodies throughout.
5. **Impossible base-branch regression proof for the SIGKILL canary.**
   Resolved by reframing: Task 3's canary test cites the first draft's
   equivalent test (which *did* run legitimately against `main`, since it
   didn't depend on any new pause hook) as its base-branch evidence,
   explicitly stated as such rather than presented as a literal rerun of
   new code against old code. Task 7's test 2 is the real, full-cycle
   crash→restart→verify proof, added once the recovery machinery exists
   to run it meaningfully.
