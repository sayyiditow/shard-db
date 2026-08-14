# Fix: free-pool slot-capacity/header mismatch corrupts variable-length record stride

Branch: `refactor/variable-only-segments` (continue on current branch — not a
new feature branch, this is a bugfix on work already in flight; execution
stays uncommitted per this repo's standing exception).

**CORRECTION (post-implementation, 2026-08-14):** The "Startup recovery
correctness" claim below (originally around line 606, "recover_one_stream's
reserve_off reconstruction... becomes correct by construction") is also
wrong — it does not account for `recover_one_stream`'s active-file walk
having no resync logic for the mid-file zeroed gaps this fix's own
`pool_split_leftover` deliberately creates. See
`docs/plans/2026-08-14-recover-one-stream-midfile-gap-corruption.md` for the
full diagnosis, repro, and suggested fix direction; that bug is pre-existing
on `main` (this function is untouched by this fix) and not introduced here.

**CORRECTION (post-implementation, 2026-08-13):** This plan's root-cause
claim below overstated its explanatory power. The fix as implemented here
(new `pool_split_leftover`, 7 call sites) is real and worth keeping — it
fixes a genuine bug where two bulk-write sites left non-zeroed stale
garbage on disk (worse than a zeroed gap, since a scanner hitting garbage
there can hit the *unrecoverable* desync path instead of the benign
padding-tail path) and a free-pool capacity leak. **It does not fix the
`keys`-latency regression this plan set out to explain.** Verified via the
regression test written under this plan (`test-varlen-pool-donation-stride`)
staying red on its latency assertion after the fix landed (3.97s at 8,000
ops, materially unchanged from pre-fix), then confirmed via direct
byte-level inspection: `seg_record_emit` always zero-pads a record out to
whatever capacity value it's given, before and after this fix — so the
*physical* on-disk gap between an undersized record and the next real
record is identical either way; this fix only changes whether that
leftover span is tracked as reusable free-pool space (post-fix) or leaked
(pre-fix), not whether the gap exists. The gap itself, and the scanner
having to resync across it, is an inherent consequence of the free pool's
coarse-bucket donation policy under a workload with varying record sizes —
not a bug in what gets written to the header. The actual latency driver is
`do_resync` in `io_direct.c` paying a full teardown-and-rebuild of the
double-buffered O_DIRECT prefetch context on every gap, however small; see
`docs/plans/2026-08-13-od-resync-inbuffer-fastpath.md` for that fix. The
regression test's latency assertion is expected to stay red until that
follow-up plan lands too — left as-is (not weakened) per this repo's rule
against hiding a real failure.

## Root cause

Every variable-length segment record's on-disk header (24 bytes: 16B hash +
2B `klen` + 1B flag + 1B reserved + 4B `vlen`) is the **only** source of
truth a sequential reader has for that record's footprint — readers compute
`rec_size = round_up_8(24 + klen + vlen)` (`slotcask_record_size_varlen()` /
`od_varlen_rec_size()`) and advance by that many bytes to reach the next
record. There is no separate on-disk "capacity" field.

The free-slot pool (`pool_try_pop_for_size()`, `src/db/slotcask.c:3662`)
intentionally does *coarse* bucket matching — "Bucket membership is a coarse
capacity range ... not a single fixed size ... a bucket can hold entries
smaller than needed_size" (existing comment at `slotcask.c:3667-3672`) — and
accepts **any** free slot whose `capacity >= needed_size`, not an exact
match. When a donated slot's capacity exceeds what the new record actually
needs (e.g. a slot freed by tombstoning a `{v:int,writer:int}` insert,
`vlen=8`, gets reused by a smaller write, e.g. a partial-field `update` that
only sends `{"v":...}`, `vlen=4`), five of the seven write call sites pass
the **donor's full capacity** as the padding target to `seg_record_emit()`:

```c
// src/db/slotcask.c:3884-3898 (slotcask_insert, and identically shaped at
// slotcask_update:3975-3990, slotcask_upsert_with_hooks fast path:4942-4955,
// upsert_slow_path:5276-5293, slotcask_insert_with_hooks CAS path:5647-5660)
int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
if (got_pool) {
    target_fid = fs.file_id;
    target_off = fs.offset;
    slot_capacity = fs.capacity;          // <-- donor's capacity, can be > needed
} else {
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    ...
    slot_capacity = (uint32_t)rec_size;
}
...
seg_write_record_varlen(..., slot_capacity, 1);   // zero-pads out to slot_capacity
```

`seg_record_emit()` (`slotcask.c:3736-3760`) zero-pads from `24+klen+vlen`
up to whatever `slot_size` it's given, so this is memory-safe — the excess
bytes *are* zeroed, not leaked as garbage — but the record's **header**
still encodes only the true, smaller `vlen`. The record's real on-disk
footprint (`fs.capacity`, e.g. 40 bytes) now exceeds what any reader can
compute from the header (`round_up_8(24+klen+vlen)`, e.g. 32 bytes). Every
sequential walker that trusts the header-computed size under-advances and
lands inside the previous record's own zero-padding tail, which reads back
as `flag==0` — a legitimate "padding" signal today, but occurring **every
~1 record** instead of only at genuine end-of-live-data, because the
donation-vs-need gap recurs on essentially every reuse under a mixed
record-size workload (any object where the same key's value size varies
between writes — trivially true for this schema's `insert` vs. partial
`update`).

Three independent consumers share this same header-only stride assumption
and are all affected:

1. **`seg_scan_o_direct()`** (`src/db/io_direct.c:626`, stride computed at
   line 729) — the O_DIRECT double-buffered scanner backing `cmd_keys` and
   the `scan_shards_v2_o_direct` family. Each spurious `flag==0` hit trips
   `do_resync`, which **tears down and rebuilds the entire double-buffered
   prefetch context** (joins the prefetch thread, re-inits, re-kicks-off) —
   empirically 386-1172 times per segment file in the concurrent stress
   test, at ~28ms/resync, producing the observed 14-33s per-file scan
   latency (files scan in parallel via `parallel_for_io`, so end-to-end
   latency ≈ the slowest file — matches the ~33s `keys` latency and the
   test's 30s client-side timeout, i.e. this is the actual root cause of
   `test-slotcask-v2-concurrent`'s hang/timeout).
2. **`recover_one_stream()`** (`slotcask.c:4442-4478`, active-segment walk)
   — used at **every daemon startup** to reconstruct `reserve_off` (the
   append frontier) and repopulate the free pool from `flag==2` records in
   the currently-active segment. A misaligned walk here doesn't just cost
   time — it can compute the wrong `reserve_off`, risking a subsequent
   append overwriting still-live data, or corrupt the free pool with wrong
   offsets.
3. **`recover_scan_tombstones_od()`** (`slotcask.c:4300-4399`, non-active
   segment startup scan) — same header-only stride assumption, populates
   the free pool for older segments.

Separately, two **bulk** write sites have a related but distinct defect —
they already pad only to the record's own `rec_size` (not the donor's
larger capacity), which is the *correct* padding target, but never zero or
reclaim the leftover `fs.capacity - rec_size` bytes at all, leaving genuine
**stale, non-zeroed garbage** in the gap (worse than the zeroed-but-
misaligned case above, since a reader that lands there sees an
unpredictable non-`flag∈{0,1,2}` byte, not a clean padding signal) and
permanently leaking that capacity out of the free pool (it's never
recomputed, since `slotcask_tombstone_and_push_back` only ever recomputes
capacity from a record's own header at the time *that* record is
tombstoned):

- `slotcask_bulk_update()` (`slotcask.c:4110`, pool-reserve at
  `4142-4155`, write at `4160-4167` — passes `rec_size`, not
  `infos[i].target.capacity`).
- `bulk_phase3_seg_writes()` (`slotcask.c:5977`, pool-reserve at
  `5991-6008`, `seg_record_emit` call at `6019-6021` — passes `rec_size`,
  not `r->slot_capacity`).

This fully explains both symptoms seen in `test-slotcask-v2-concurrent`:
the severe `keys` latency (confirmed via instrumentation: hundreds of
benign-but-expensive resyncs per file) and the separately-observed empty
`keys` result (`count=21`, `keys=[]`) — plausible when a gap occurs early
enough in a file that `do_resync`'s bounded window search
(`od_varlen_resync_find`, window = `max_slot_size` bytes) either doesn't
extend far enough to find the true next record, or a chain of consecutive
undersized-donation gaps compounds beyond the window, causing that file's
scan to terminate early via the benign "reached the sparse tail" path
(`padding_desync=1`, `ret=0`) while genuinely live records past that point
are silently dropped from the result. `count` is unaffected because it
reads authoritative per-shard kf headers (`slotcask_sum_kf_totals`), never
walking segments; `fetch`/`get` are unaffected because they resolve a
specific (file_id, offset) directly from the kf hash table and read
exactly `vlen` bytes at that known offset — they never need to compute a
next-record stride.

**This is confirmed independent of concurrency.** A purely sequential
(single-connection, no threads) reproduction with the identical
insert/update/delete mix used by `test_slotcask_v2_concurrent.c`
(`KEY_RANGE=50`, `insert`/`update`(partial `{"v":...}`)/`delete` at 1/3
odds each) reproduces the same `keys` latency blowup deterministically:

| sequential ops | build time | `keys` latency | `keys` len (all correct) |
|---:|---:|---:|---:|
| 5,000  | 0.18s | 2.51s | 28 |
| 10,000 | 0.37s | 4.74s | 24 |
| 15,000 | 0.55s | 6.86s | 26 |
| 40,000 | 1.43s | 17.46s | 30 |

(Reproduced against a scratch `DB_ROOT` at `/tmp/repro2/db`, object
`fields:["v:int","writer:int"]`, `splits:8` → `streams:8`, PRNG seed fixed.
Byte-level inspection of the resulting `.dat` file confirmed the mechanism
directly: consecutive tombstoned records spaced exactly 40 bytes apart on
disk while each one's own header encodes `klen=3,vlen=4` →
`round_up_8(24+3+4)=32`, an 8-byte-per-record undercount matching
`max_value=8` (the schema's full `v+writer` size) vs. the `vlen=4` actually
written by the partial `update`.)

This confirms concurrency was never structurally required to trigger the
bug — it only made `test_slotcask_v2_concurrent.c`'s existing failure mode
visible sooner/more reliably at its 3-second scale. The fix and its
regression test below are both purely sequential/deterministic.

## Fix

Never let a written record's true on-disk footprint exceed its own
`slotcask_record_size_varlen(klen, vlen)`. When a popped free-pool slot's
capacity exceeds what the record being written needs, split the excess off
immediately as an independent, explicitly-zeroed, separately-tracked free
entry — restoring the invariant `SlotcaskFreeSlot.capacity` already claims
in its own doc-comment (`slotcask.h:217`: `/* actual slot size in bytes (24
+ klen + vlen) */`) and that every sequential reader depends on.

### New helper (`src/db/slotcask.c`, immediately after `pool_try_pop_for_size`,
anchor: the closing brace + comment before `/* ============================================================ Append path */`)

Quoted anchor (exact, currently at `slotcask.c:3686-3688`):

```c
    pthread_mutex_unlock(&p->pool_lock);
    return 2;
}

/* ============================================================ Append path */
```

Insert the new helper between the closing `}` of `pool_try_pop_for_size`
and the `/* ==== Append path ==== */` banner:

```c
    pthread_mutex_unlock(&p->pool_lock);
    return 2;
}

/* A free-pool slot popped by pool_try_pop_for_size() may be larger than
   the record about to be written into it (coarse bucket matching, see the
   comment above) — never let the excess be silently folded into that
   record's zero-padding, or every reader that recomputes stride from the
   record's own header (24+klen+vlen) will under-advance and misalign
   against genuinely live data past it. Zero the excess in place and
   return it to the pool as its own independent, correctly-capacitied
   entry so the invariant "on-disk footprint == header-computed size"
   holds for every record unconditionally. */
static int pool_split_leftover(SlotcaskDb *db, uint8_t stream_id,
                                uint16_t file_id, uint32_t offset,
                                uint32_t len) {
    if (len == 0) return 0;
    char path[PATH_MAX];
    seg_path_for(path, db->data_dir, stream_id, file_id);
    SlotcaskSegHandle h;
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    memset(h.map + offset, 0, len);
    if (h.slot >= 0) {
        SegCacheEntry *e = &g_segcache[h.slot];
        durability_mark_dirty(&e->dirty, &e->dirty_since_ms);
    }
    segcache_release(&h);
    return pool_push_free_cap(&db->streams[stream_id], file_id, offset, len,
                              db->slot_size);
}

/* ============================================================ Append path */
```

### Site 1 — `slotcask_insert` (`slotcask.c:3884-3898`)

Quoted anchor (exact):

```c
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = fs.capacity;
    } else {
        size_t rec_size = slotcask_record_size_varlen(klen, vlen);
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

Replace with:

```c
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

(This introduces one duplicate local — the function has no pre-existing
`rec_size` at this scope, confirmed by reading `slotcask_insert` in full;
the removed inner-`else`'s `size_t rec_size` declaration is what's being
hoisted, not a new addition alongside an existing one.)

### Site 2 — `slotcask_update` (`slotcask.c:3975-3990`)

Byte-identical shape to Site 1 (only `pool`/`target_*` already declared
above it in this function). Quoted anchor (exact):

```c
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = fs.capacity;
    } else {
        size_t rec_size = slotcask_record_size_varlen(klen, vlen);
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

Replace with:

```c
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    int got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                           db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

### Site 3 — `slotcask_upsert_with_hooks` fast path (`slotcask.c:4942-4955`)

Quoted anchor — this exact text is unique to this function (preceded
immediately by the comment `/* Reserve seg slot. */` a few lines above,
confirmed via reading `4930-4964`):

```c
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = fs.capacity;
    } else {
        size_t rec_size = slotcask_record_size_varlen(klen, vlen);
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg. */
```

Replace with:

```c
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg. */
```

### Site 4 — `upsert_slow_path` (`slotcask.c:5276-5293`)

Uses `write_vlen` instead of `vlen` (distinguishes this anchor uniquely).
Quoted anchor (exact):

```c
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + write_vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = fs.capacity;
    } else {
        size_t rec_size = slotcask_record_size_varlen(klen, write_vlen);
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

Replace with:

```c
    size_t rec_size = slotcask_record_size_varlen(klen, write_vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + write_vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0) {
            kfcache_release(&kh);
            free(callback_value);
            free(old_buf);
            return -1;
        }
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }
```

### Site 5 — `slotcask_insert_with_hooks` CAS path (`slotcask.c:5647-5660`)

Quoted anchor — preceded by `/* Reserve a target seg slot (free pool, else
append). */` a few lines above (confirmed via reading `5635-5669`):

```c
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = fs.capacity;
    } else {
        size_t rec_size = slotcask_record_size_varlen(klen, vlen);
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg with flag=1 set so kf can point to valid live data. */
```

Replace with:

```c
    size_t rec_size = slotcask_record_size_varlen(klen, vlen);
    got_pool = (pool_try_pop_for_size(pool, (uint32_t)(24 + klen + vlen),
                                       db->slot_size, &fs) == 0);
    if (got_pool) {
        target_fid = fs.file_id;
        target_off = fs.offset;
        slot_capacity = (uint32_t)rec_size;
        if (fs.capacity > slot_capacity)
            pool_split_leftover(db, target_stream, target_fid,
                                target_off + slot_capacity,
                                fs.capacity - slot_capacity);
    } else {
        uint32_t fid, off;
        if (append_reserve_single_varlen(db, pool, rec_size, &fid, &off) != 0)
            return -1;
        target_fid = (uint16_t)fid;
        target_off = off;
        slot_capacity = (uint32_t)rec_size;
    }

    /* Write seg with flag=1 set so kf can point to valid live data. */
```

### Site 6 — `slotcask_bulk_update` (`slotcask.c:4142-4155`)

This site already passes `rec_size` (not the donor's capacity) as the
padding target at its write call (`4160-4167`, unchanged) — it only needs
the split call added, plus correcting the tracked `.capacity` bookkeeping
to match (dead for this struct today — `infos[i].target.capacity` is never
read after assignment, confirmed via full-file grep — but keeping it
truthful is cheap and avoids re-planting the same header/tracked-value
mismatch pattern this bug is about). Quoted anchor (exact):

```c
        if (pool_try_pop_for_size(&db->streams[s],
                                  (uint32_t)(24 + recs[i].klen + recs[i].vlen),
                                  db->slot_size, &fs) == 0) {
            infos[i].target = fs;
        } else {
```

Replace with:

```c
        if (pool_try_pop_for_size(&db->streams[s],
                                  (uint32_t)(24 + recs[i].klen + recs[i].vlen),
                                  db->slot_size, &fs) == 0) {
            infos[i].target = fs;
            infos[i].target.capacity = (uint32_t)rec_size;
            if (fs.capacity > (uint32_t)rec_size)
                pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                    fs.offset + (uint32_t)rec_size,
                                    fs.capacity - (uint32_t)rec_size);
        } else {
```

### Site 7 — `bulk_phase3_seg_writes` (`slotcask.c:5991-5998`)

Same shape. Quoted anchor (exact):

```c
            if (pool_try_pop_for_size(pool, (uint32_t)needed,
                                      db->slot_size, &fs) == 0) {
                st[i].target_fid = fs.file_id;
                st[i].target_off = fs.offset;
                st[i].got_pool = 1;
                r->slot_capacity = fs.capacity;
            } else {
```

Replace with:

```c
            if (pool_try_pop_for_size(pool, (uint32_t)needed,
                                      db->slot_size, &fs) == 0) {
                st[i].target_fid = fs.file_id;
                st[i].target_off = fs.offset;
                st[i].got_pool = 1;
                r->slot_capacity = (uint32_t)rec_size;
                if (fs.capacity > (uint32_t)rec_size)
                    pool_split_leftover(db, (uint8_t)s, fs.file_id,
                                        fs.offset + (uint32_t)rec_size,
                                        fs.capacity - (uint32_t)rec_size);
            } else {
```

## Call sites verified as NOT needing changes

Exhaustive grep of every `pool_try_pop_for_size` / `seg_write_record_varlen`
/ `seg_record_emit` caller in `slotcask.c` confirms these are the only 7
write sites that pop from the free pool (the remaining `seg_record_emit`
caller, `slotcask_tombstone_and_push_back`, only flips a flag byte — it
doesn't write a new record). `recover_one_stream`, `recover_scan_tombstones_od`,
and `seg_scan_o_direct` need **no code changes** — they already correctly
trust the header to determine stride; that trust is exactly what this fix
restores by guaranteeing on-disk footprint always equals what the header
encodes.

## Edge cases

- **Zero leftover** (`fs.capacity == rec_size`, the common/exact-fit case):
  `pool_split_leftover` no-ops (guarded by `if (len == 0) return 0;`) — no
  extra I/O on the already-correct path.
- **Tiny leftover** (e.g. 1-7 bytes, capacity barely exceeds rec_size):
  still zeroed and pushed to the pool via `pool_push_free_cap` — it will
  simply never be popped again (no real record can fit in under 24 bytes),
  a permanent but harmless few-byte fragmentation loss, not a correctness
  issue. No minimum-size special-casing needed.
- **Concurrent access to the just-split leftover**: the split happens
  immediately after popping `fs` from the pool and before the main record
  is written or made visible via kf — no other thread can have a reference
  to this (file_id, offset) yet (it was private to this pool-pop), so
  `pool_split_leftover`'s own `segcache_acquire`/zero/`segcache_release` is
  the only writer touching that byte range at that moment. The main
  record's own `segcache_acquire` (via `seg_write_record_varlen`, disjoint
  byte range) runs independently after — both are rdlock-style
  `segcache_acquire` calls on the same file, which the existing
  `slotcask_tombstone_and_push_back` (concurrent single-byte flag writes to
  the same file) already establishes as safe.
- **Write failure after a successful split** (e.g. `seg_write_record_varlen`
  fails after `pool_split_leftover` already ran): existing rollback
  (`if (got_pool) pool_push_free_cap(pool, target_fid, target_off,
  slot_capacity, ...)`) pushes back exactly `slot_capacity` (== `rec_size`
  post-fix) — correct, since the leftover was already independently
  returned to the pool regardless of whether the main write later
  succeeds or fails; the rollback and the split return two disjoint,
  independently-tracked ranges, never double-counted.
- **`slotcask_bulk_update`'s `rec_size` local**: already computed one line
  above the anchor (`size_t rec_size = slotcask_record_size_varlen(recs[i].klen, recs[i].vlen);`
  at `slotcask.c:4142`, preceding the `if (pool_try_pop_for_size(...))` —
  confirmed in context above); Site 6's replacement reuses it, no new
  declaration needed.
- **Startup recovery correctness**: once no live-going-forward record can
  ever have footprint > header-computed size, `recover_one_stream`'s
  `reserve_off` reconstruction and `recover_scan_tombstones_od`'s free-pool
  population both become correct by construction for all newly-written
  data. Pre-existing on-disk data written by the *unfixed* binary (already
  present in a real deployment before this fix ships) can still carry the
  old mismatch — out of scope here (this branch has never been released;
  confirmed via `git log` no tagged release includes the variable-length
  segment format yet), but worth a one-line callout in the CHANGELOG entry
  for this fix so it isn't silently assumed to retroactively repair
  already-corrupted-stride files from a pre-fix build.

## Regression test (test-first)

New file `src/test/cases/test_varlen_pool_donation_stride.c`, sequential
(no threads — matches the empirically-confirmed sequential reproduction
above, avoiding any timing/ordering non-determinism in the regression
signal itself, per this repo's requirement that new tests not depend on
racy timing where a deterministic alternative exists).

1. Start the test daemon via the standard harness (`test_env_start`),
   create an object with fields `["v:int","writer:int"]` — same schema as
   `test_slotcask_v2_concurrent.c`, `splits:8`.
2. Run a deterministic mixed workload: 8,000 iterations, round-robin over
   4 synthetic "writer ids", `k = rand_r(&seed) % 50`, op selected by
   `rand_r(&seed) % 3` (0=insert both fields, 1=update `v` only, 2=delete)
   — identical shape to `test_slotcask_v2_concurrent.c`'s per-op logic,
   just called synchronously from the main test thread instead of from
   worker threads, with an explicit fixed seed (e.g. `12345u`) so the run
   is bit-for-bit identical every execution.
3. `count` the object (fast, kf-header-based, always correct — the
   oracle).
4. Time a `keys` call. Assert:
   - `keys` returns the same number of entries as `count` (the
     correctness assertion — catches the empty/truncated-result variant).
   - `keys` completes in under **2 seconds** wall-clock (the performance
     assertion — catches the resync-storm variant). Empirically the
     unfixed code takes ~2.5s at 5,000 ops and scales up from there
     (table above), so 8,000 ops / 2s bound gives comfortable margin on
     both sides: clearly fails pre-fix, clearly passes post-fix (expected
     near-instant, since post-fix gaps only occur at genuine end-of-data
     padding, not on ~every record).
5. Register via `TEST_REGISTER("test-varlen-pool-donation-stride", ...)`.

**Prove the regression signal is real before trusting it**: after writing
the test against the *unfixed* tree, run it and paste the actual failure
output (expect either the count-mismatch assertion or the 2-second timing
assertion to fail, or the request to hit the client's I/O timeout
entirely). Only then apply the seven-site fix above, rerun, and paste the
passing output. Do not proceed to the next task on a test that wasn't
actually observed red first.

## Cleanup (part of this same task, not a follow-up)

Revert all temporary debug instrumentation added during the investigation
that led to this plan — CORE-PROCESS's definition-of-done forbids leftover
debug prints:

- `src/db/io_direct.c`: remove the `dbg_resync_count` local and its two
  `fprintf(stderr, "[DBG seg_scan_o_direct] ...")` call sites (inside
  `do_resync`'s early-termination branch, and at the `done:` label inside
  `seg_scan_o_direct` specifically — there is a second, unrelated `done:`
  label elsewhere in the file that must not be touched).
- `src/db/query_find.c`: remove the `fprintf(stderr, "[DBG
  scan_shards_v2_o_direct] ...")` at function entry, revert
  `od_seg_file_worker` back to a bare, untimed
  `seg_scan_o_direct(...)` call (drop the `clock_gettime`/`dbg_rc`/
  `dbg_ms`/`fprintf` wrapping), and remove the `#include <time.h>` if
  nothing else in the file uses it after that revert.
- `src/test/cases/test_slotcask_v2_concurrent.c`: remove the
  `fprintf(stderr, "[concurrent][debug] raw keys resp ...")` line
  (currently ~line 249-250, immediately before the `ASSERT_EQ_INT(counted,
  key_count, ...)` call).

## Build / test / verify

Per this repo's standing exceptions:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-varlen-pool-donation-stride
./build/bin/shard-db-test run test-slotcask-v2-concurrent
./build/bin/shard-db-test run-all
```

This diff touches free-pool/segment allocation state that's shared across
concurrent writers (`SlotcaskStream.pool_lock`/`rotation_lock`) and the
segment mmap cache (`segcache_acquire`) — per this repo's AGENTS.md
dynamic-safety gate, run both sanitizer builds locally against at least the
affected cases before calling this done:

```bash
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
  ./build/bin/shard-db-test run-all --jobs 2

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
  ./build/bin/shard-db-test run-all --jobs 1
```

New findings from either sanitizer get root-caused and fixed now if simple,
or written up as a follow-up plan — never silently suppressed.

## Execution rules

- Do tasks in the order listed: helper → 7 call sites → regression test
  (written and confirmed red first) → apply fix → confirm test green →
  cleanup debug instrumentation → full test suite → ASan → TSan.
- If a quoted anchor isn't found exactly as shown (e.g. the file has
  drifted since this plan was written), stop immediately, write
  `PLAN_NOTES.md` describing the exact mismatch, and halt the entire run —
  do not guess or reinterpret, even for an anchor that looks "close
  enough."
- If a decision arises that this plan doesn't cover, stop and ask rather
  than improvise.
- Execution mode for this repo: leave the diff **uncommitted** when done —
  human + a fresh-eyes reviewing agent review the raw `git diff` before
  anything is committed.
- Do not start on the deferred UUID-key-serialization bug cluster or the
  `find total:true` gap (user-reported, explicitly deferred to a future
  plan) as part of this work.
