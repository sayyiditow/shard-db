# B3b — cross-request bulk commit-chain merge

Date: 2026-09-09 (revision 2, 2026-09-11)  
Status: draft for re-approval — revision 1 was halted by its own Task 0
coupling audit (`PLAN_NOTES.md`, 2026-09-11); this revision supplies the
complete chain-aware interface that audit demanded. Not approved or
executable until the human approves this revision.  
Follows: `2026-09-09-b3a-path-epoch-syncs.md`, merged (ddcf600). B3c
(single-record group commit) remains out of scope.

## Revision 2 — what changed and why

Revision 1 assumed the six per-shard phase helpers were mechanical
generalizations: iterate chain members and call the existing helpers. The
Task 0 audit found the opposite: every one of them reads request-owned
state (`req->shards[...]`, `req->db`, `req->num_shards`,
`request_owns_shard`, the member's `BulkMutationTxn` including its
`txn->req` nonce/kf-dir, and the request-owned `ReqShard` collections).
Revision 1 also omitted the complete `bulk_commit_chain_run` body and the
complete pipeline replacement, which its own Task 0a made mandatory once
the audit found a signature/field mismatch.

The resolution, now spelled out in complete code below:

- **No window moves between requests and no lower-level helper changes.**
  Each chain member keeps its own `SlotcaskBulkRequest`, its own
  `BulkMutationTxn` (so `txn->req->nonce`/`txn->req->kf_dir` and the
  `txn->req` tombstone-deferral branch keep working untouched), and its
  own `ReqShard`. Stage, publish, finalize, and release are called
  per member with that member's own handles — the existing helpers are
  reused verbatim.
- **Only the durability flush layer becomes chain-aware.** The four
  per-shard flush helpers (P, M dir fsync, K, and the I/K/A/T/clear
  commit flush) are replaced by chain-merged variants that aggregate
  every member's temporary touch/location/slot sets into one pass per
  barrier. The aggregate arrays are chain-local stack/malloc scratch and
  are never installed into any member's request.
- **Interface corrections found during this patch** (the Task 0a
  "signature or field mismatch" clause):
  - `BulkCommitChain` gains `char kf_dir[PATH_MAX]` plus a
    `bulk_commit_chain_init` constructor (the merged M/clear fsyncs need
    the shared kf directory; every member of a chain targets the same
    object, so `req->kf_dir` is identical across members).
  - `bulk_gate_waiter_take_matching` takes `int in_kind` carrying
    `SLOTCASK_BULK_INPUT_*` values. Revision 1 declared it as
    `BulkMutationKind`, whose enum values differ from the input kind's
    (`BULK_MUTATION_UPSERT`=0 vs `SLOTCASK_BULK_INPUT_UPSERT`=1) —
    comparing across the two enums would silently never match.
  - `SHARD_TEST_PHASE_PRE_MERGE`'s pause site is specified: immediately
    before the take-matching seam (red-run position: pipeline entry;
    Task 2's pipeline replacement relocates it).
  - The queue definition had to move before `writer_gates_init` (its
    init/destroy are called from open/close), so the placement is split:
    queue struct + init/destroy before `writer_gates_init`; waiter struct
    + queue ops + chain machinery immediately before the pipeline.
  - `SlotcaskBulkRequest` loses the three aggregate fields entirely
    (`any_pending`/`any_failed`/`saved_errno`), not just their writes —
    every reader and writer was inside the replaced code.
  - The superseded per-shard flush helpers are deleted (they would be
    uncalled statics); the chain variants preserve their bodies
    pass-for-pass, so a solo chain is behaviorally identical to the B2
    pipeline.
  - Compile order (review finding 1): the chain types lead the replaced
    region because the flush functions take `BulkCommitChain *`;
    `BULK_MERGE_MAX_JOINERS` and the `BulkGateWaiter` typedef are defined
    exactly once, in the admission-queue block that precedes this region
    in the file.
  - P-failure propagation (review finding 2): every failure exit of
    `bulk_commit_chain_flush_payloads` — slot-vector overflow,
    allocation failure, and sync failure — marks exactly the
    contributing members, so no record or input can report success after
    a hard P failure.

## Goal and root cause

Post-B3a measurements show that parallel bulk ingest collapses when
`connections × shards` produces many partially filled windows. Every
request currently pays its own P/M/K/A/T/clear commit chain while requests
serialize on the per-shard writer gate. The root cause is fixed per-chain
durability work, not insufficient worker parallelism.

B3b reduces that work by letting the current gate holder admit already
queued same-shard bulk requests and run one chain over all their windows.
There is no grace period and no change to single-record writes.

## Design

### D1 — owner-preserving chain seam

The merge must not move a window between requests. A `ReqWindow` contains
plans, marker paths, segment locations, and hook state; its `ReqShard` also
owns transaction state, record state, old-value buffers, `res_map`, and
cleanup. Moving only the windows creates leaks and loses the transaction
context needed by finalize/apply helpers. The chain therefore carries each
member's owner pointers and every phase operation is addressed through
them.

Replace everything from the quoted region header

```c
/* ── Per-shard pipeline flushes (B1) ─────────────────────────────────
```

through the closing brace of `slotcask_bulk_shard_flush_commit` (its last
lines are quoted below as the region's end anchor):

```c
    durability_test_pause(req->db->data_dir, "bulk-window-cleared");
    if (unlink_rc != 0) errno = unlink_errno ? unlink_errno : EIO;
    return unlink_rc;
}
```

with this complete section. It deletes the superseded
`slotcask_bulk_shard_flush_payloads`, `slotcask_bulk_shard_flush_marker_dir`,
`slotcask_bulk_shard_flush_kf`, and `slotcask_bulk_shard_flush_commit`
(their bodies survive, generalized, in the chain variants below) and
keeps `req_bitmap_file_path`/`fdatasync_path` verbatim. The chain types
lead the region — the flush functions take `BulkCommitChain *` — and
rely on `BULK_MERGE_MAX_JOINERS` and the `BulkGateWaiter` typedef from
the admission-queue block, which precedes this region in the file
(before `writer_gates_init`):

```c
/* ── B3b chain flushes (generalize the B1 per-shard flushes) ────────
   Each runs inside the committer's single gate hold and merges every
   member's contribution into one pass per durability barrier. A solo
   chain (one member) performs exactly the passes the superseded
   per-shard helpers performed. */

typedef struct {
    SlotcaskBulkRequest    *req;
    SlotcaskBulkShardInput *in;
    ReqShard               *rs;
    BulkGateWaiter         *waiter; /* NULL for the committer */
} BulkChainMember;

typedef struct {
    SlotcaskDb      *db;
    int              kf_shard_id;
    char             kf_dir[PATH_MAX]; /* shared by all members (same db) */
    BulkChainMember  members[BULK_MERGE_MAX_JOINERS + 1];
    size_t            nmembers;
} BulkCommitChain;

/* Bitmap file path for the chain commit flush (mirrors index.c's
   bm_build_path: <root>/<obj>/indexes/<field>/<NNN>.bm). */
static void req_bitmap_file_path(char *out, size_t outlen,
                                 const char *db_root, const char *object,
                                 const char *field, int shard_idx) {
    snprintf(out, outlen, "%s/%s/indexes/%s/%03d.bm",
             db_root, object, field, shard_idx);
}

/* fdatasync a file by path without touching any cache. B3a: routed
   through the path-keyed epoch so concurrent commit flushes syncing
   the same bitmap file coalesce. The raw op converts ENOENT to success
   (missing file = nothing ever written), so this returns 0 for a
   missing file. */
static int fdatasync_path(const char *path) {
    return durability_epoch_fdatasync_path_ex(path, NULL);
}

/* Mark every non-stage-failed member holding staged payload locations as
   payload-failed. Used by the P flush's early failure exits (slot-count
   overflow, allocation), before any np reset — with the sync-failure
   marking below, every failure exit of the flush marks exactly the
   contributing members, so no record or input can report success after
   a hard P failure. */
static void bulk_chain_mark_payload_failed(BulkCommitChain *chain) {
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        if (mem->rs->stage_failed || mem->rs->np == 0) continue;
        mem->rs->payload_failed = 1;
        mem->rs->step_failed = 1;
        if (!mem->rs->step_errno)
            mem->rs->step_errno = errno ? errno : EIO;
    }
}

/* Chain P barrier: one merged, sorted, deduplicated sync pass over every
   non-stage-failed member's staged payload locations. Contributors are
   the members with rs->np > 0; on failure exactly the contributors are
   marked payload_failed (their records report -1 and publish is
   skipped) — non-contributing members are untouched. Mirrors the B1
   flush's stage_failed skip, note-sync position, and unconditional
   np reset on the sync path. */
static int bulk_commit_chain_flush_payloads(BulkCommitChain *chain) {
    size_t total = 0;
    for (size_t m = 0; m < chain->nmembers; m++) {
        ReqShard *rs = chain->members[m].rs;
        if (rs->stage_failed) continue;
        if (size_add_checked(&total, rs->np) != 0) {
            bulk_chain_mark_payload_failed(chain);
            return -1;
        }
    }
    if (total == 0) return 0;
    SegLoc *all = malloc(total * sizeof(*all));
    if (!all) {
        bulk_chain_mark_payload_failed(chain);
        return -1;
    }
    size_t n = 0;
    for (size_t m = 0; m < chain->nmembers; m++) {
        ReqShard *rs = chain->members[m].rs;
        if (rs->stage_failed) continue;
        if (rs->np) {
            memcpy(all + n, rs->p_locs, rs->np * sizeof(*all));
            n += rs->np;
        }
    }
    if (n > 1 && !segloc_is_sorted(all, n))
        qsort(all, n, sizeof(*all), segloc_cmp);
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0) all[w++] = all[i];
    uint64_t t0 = now_us();
    int rc = bulk_seg_apply_and_sync(chain->db, all, w, 0, 0);
    int saved_errno = rc != 0 ? (errno ? errno : EIO) : 0;
    commit_phase_us_record(&g_commit_segment_sync_us_total, t0);
    commit_phase_us_record(&g_commit_segment_p_us_total, t0);
    free(all);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_P)) {
        errno = EIO;
        rc = -1;
        saved_errno = EIO;
    }
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        if (mem->rs->stage_failed) continue;
        if (rc != 0 && mem->rs->np > 0) {
            mem->rs->payload_failed = 1;
            mem->rs->step_failed = 1;
            if (!mem->rs->step_errno) mem->rs->step_errno = saved_errno;
        }
        mem->rs->np = 0;
    }
    if (rc != 0) errno = saved_errno ? saved_errno : EIO;
    return rc;
}

/* Chain M barrier: ONE fsync of the shared kf directory (every member of
   a chain targets the same object, so req->kf_dir is identical) after
   every member's windows' markers are link-published. On failure every
   member with published windows is marked marker_dir_failed — its
   finalize is skipped and its markers stay retained (rc -2,
   EINPROGRESS); members that published nothing are unaffected. */
static int bulk_commit_chain_flush_marker_dir(BulkCommitChain *chain) {
    int any = 0;
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++)
            any |= chain->members[m].rs->windows[i].published;
    if (!any) return 0;
    int rc = durability_epoch_fsync_dir(chain->kf_dir);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_M)) {
        errno = EIO;
        rc = -1;
    }
    if (rc != 0) {
        for (size_t m = 0; m < chain->nmembers; m++) {
            ReqShard *rs = chain->members[m].rs;
            int pub = 0;
            for (size_t i = 0; i < rs->nwindows; i++)
                pub |= rs->windows[i].published;
            if (!pub) continue;
            rs->marker_dir_failed = 1;
            rs->step_failed = 1;
            if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
        }
    }
    return rc;
}

/* Chain K barrier: one mmap durability wait for the chain's merged kf
   slot vector (all members are windows of the same kf shard). */
static int bulk_commit_chain_flush_kf(BulkCommitChain *chain) {
    size_t total = 0;
    int header_changed = 0;
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
            ReqWindow *rw = &chain->members[m].rs->windows[i];
            if (!rw->converged) continue;
            if (size_add_checked(&total, rw->nkf) != 0) return -1;
            header_changed |= rw->kf_header_changed;
        }
    if (total == 0 && !header_changed) return 0;
    if (total > SIZE_MAX / sizeof(size_t)) { errno = EOVERFLOW; return -1; }
    size_t *slots = total ? malloc(total * sizeof(*slots)) : NULL;
    if (total && !slots) { errno = ENOMEM; return -1; }
    size_t n = 0;
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
            ReqWindow *rw = &chain->members[m].rs->windows[i];
            if (!rw->converged) continue;
            if (rw->nkf > 0) {
                memcpy(slots + n, rw->kf_slots, rw->nkf * sizeof(*slots));
                n += rw->nkf;
            }
        }
    if (n > 1) {
        size_t *scratch = malloc(n * sizeof(*scratch));
        if (scratch) {
            radix_sort_sizes(slots, scratch, n);
            free(scratch);
        } else {
            qsort(slots, n, sizeof(*slots), size_cmp);
        }
    }
    size_t w = 0;
    for (size_t i = 0; i < n; i++)
        if (w == 0 || slots[w - 1] != slots[i]) slots[w++] = slots[i];
    SlotcaskKfHandle kh;
    if (kf_shard_acquire(&kh, chain->db, chain->kf_shard_id, 1) != 0) {
        free(slots);
        return -1;
    }
    int rc = kfcache_sync_slots_locked(&kh, slots, w, header_changed);
    kfcache_release(&kh);
    free(slots);
    if (rc == 0 && SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_K)) {
        errno = EIO;
        rc = -1;
    }
    return rc;
}

/* Chain commit barriers + clear: merged index flush [I] → chain K sync →
   merged A/T segment syncs → batched marker unlink + ONE fsync of the
   shared kf dir → reclaim + commit_done per window. Per-window
   predicates (published/converged/cleared) make ineligible windows
   no-ops, so the helper is safe to run after any earlier member's phase
   failure. */
static int bulk_commit_chain_flush_commit(BulkCommitChain *chain) {
    /* 1. Index flush: merge every member's converged windows' touch
          sets, dedupe, issue through the parallel issuer; bitmaps
          serial. */
    size_t total = 0;
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++)
            if (chain->members[m].rs->windows[i].converged)
                if (size_add_checked(&total,
                        chain->members[m].rs->windows[i].plan.touch.n) != 0)
                    return -1;
    if (total > 0) {
        uint64_t t0i = now_us();
        if (total > SIZE_MAX / sizeof(IdxTouch)) {
            errno = EOVERFLOW;
            return -1;
        }
        IdxTouch *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        char eff_root[PATH_MAX], object[256];
        split_data_dir(chain->db->data_dir, eff_root, sizeof(eff_root),
                       object, sizeof(object));
        for (size_t m = 0; m < chain->nmembers; m++)
            for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
                ReqWindow *rw = &chain->members[m].rs->windows[i];
                if (!rw->converged) continue;
                if (rw->plan.touch.n)
                    memcpy(all + n, rw->plan.touch.v,
                           rw->plan.touch.n * sizeof(*all));
                n += rw->plan.touch.n;
            }
        qsort(all, n, sizeof(*all), idx_touch_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || idx_touch_cmp(&all[w - 1], &all[i]) != 0)
                all[w++] = all[i];
        n = w;   /* deduplicated count drives everything below */
        size_t npaths = 0;
        for (size_t i = 0; i < n; i++)
            if (all[i].type != IT_BITMAP) npaths++;
        if (npaths > 0) {
            char *path_buf = malloc(npaths * PATH_MAX);
            const char **paths = malloc(npaths * sizeof(*paths));
            if (!path_buf || !paths) {
                free(all); free(path_buf); free(paths);
                return -1;
            }
            size_t w2 = 0;
            for (size_t i = 0; i < n; i++) {
                if (all[i].type == IT_BITMAP) continue;
                int shard = idx_shard_for_hash(all[i].hash16,
                                               chain->db->num_shards);
                if (all[i].type == IT_TRIGRAM)
                    tg_build_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                  eff_root, object, all[i].field, shard);
                else
                    build_idx_path(path_buf + w2 * PATH_MAX, PATH_MAX,
                                   eff_root, object, all[i].field, shard);
                paths[w2] = path_buf + w2 * PATH_MAX;
                w2++;
            }
            int frc = index_sync_path_set(paths, npaths);
            free(path_buf); free(paths);
            if (frc != 0) { free(all); return -1; }
            __atomic_add_fetch(&g_commit_index_sync_ops_total,
                               (uint64_t)npaths, __ATOMIC_RELAXED);
        }
        for (size_t i = 0; i < n; i++) {
            if (all[i].type != IT_BITMAP) continue;
            /* Plain-fd fdatasync: the dirty bitmap pages belong to the
               inode (written via the bm cache's mmap), so syncing any fd
               of the file is equivalent — without re-entering the bm
               cache, whose entry wrlocks are parked by each member's own
               prepare-staged handles until finalize. A missing file means
               nothing was ever written — not an error. */
            char bpath[1024];
            req_bitmap_file_path(bpath, sizeof(bpath), eff_root, object,
                                 all[i].field, all[i].idx_shard);
            if (fdatasync_path(bpath) != 0) {
                if (errno != ENOENT) { free(all); return -1; }
                continue;
            }
            __atomic_add_fetch(&g_commit_index_sync_ops_total, 1,
                               __ATOMIC_RELAXED);
        }
        free(all);
        commit_phase_us_record(&g_commit_index_sync_us_total, t0i);
    }

    /* 2. K barrier: one mmap durability wait for this dirty kf shard. */
    if (bulk_commit_chain_flush_kf(chain) != 0) return -1;

    /* 3. Segment barriers: activation + tombstone bytes across every
          member's windows, one pass each. */
    for (int pass = 0; pass < 2; pass++) {
        size_t total = 0;
        for (size_t m = 0; m < chain->nmembers; m++)
            for (size_t i = 0; i < chain->members[m].rs->nwindows; i++)
                if (size_add_checked(&total,
                        pass == 0 ? chain->members[m].rs->windows[i].na
                                  : chain->members[m].rs->windows[i].nt) != 0)
                    return -1;
        if (total == 0) continue;
        if (total > SIZE_MAX / sizeof(SegLoc)) {
            errno = EOVERFLOW;
            return -1;
        }
        SegLoc *all = malloc(total * sizeof(*all));
        if (!all) return -1;
        size_t n = 0;
        for (size_t m = 0; m < chain->nmembers; m++)
            for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
                ReqWindow *rw = &chain->members[m].rs->windows[i];
                if (pass == 0) {
                    if (rw->na) memcpy(all + n, rw->a_locs,
                                       rw->na * sizeof(*all));
                    n += rw->na;
                } else {
                    if (rw->nt) memcpy(all + n, rw->t_locs,
                                       rw->nt * sizeof(*all));
                    n += rw->nt;
                }
            }
        if (n > 1 && !segloc_is_sorted(all, n))
            qsort(all, n, sizeof(*all), segloc_cmp);
        size_t w = 0;
        for (size_t i = 0; i < n; i++)
            if (w == 0 || segloc_cmp(&all[w - 1], &all[i]) != 0)
                all[w++] = all[i];
        uint64_t t0s = now_us();
        int rc = bulk_seg_apply_and_sync(chain->db, all, w, 0, 0);
        int saved_errno = rc != 0 ? (errno ? errno : EIO) : 0;
        commit_phase_us_record(&g_commit_segment_sync_us_total, t0s);
        commit_phase_us_record(&g_commit_segment_post_us_total, t0s);
        free(all);
        if (rc != 0) {
            errno = saved_errno;
            return -1;
        }
    }
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_A)) {
        errno = EIO;
        return -1;
    }
    if (SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_T)) {
        errno = EIO;
        return -1;
    }

    /* 4. Batched clear: unlink every member's converged window's marker
          via its preserved path, then ONE dir fsync. Failed
          (non-converged) windows stay retained. */
    int any_unlinked = 0;
    int unlink_rc = 0;
    int unlink_errno = 0;
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
            ReqWindow *rw = &chain->members[m].rs->windows[i];
            if (!rw->published || !rw->converged || rw->cleared) continue;
            if (kfm2_unlink_by_path(rw->marker_path) != 0) {
                if (!unlink_errno) unlink_errno = errno;
                unlink_rc = -1;
                continue; /* still fsync every successful unlink */
            }
            rw->unlink_succeeded = 1;
            any_unlinked = 1;
        }
    int dir_rc = 0;
    uint64_t t0c = now_us();
    if (any_unlinked &&
        durability_epoch_fsync_dir(chain->kf_dir) != 0) dir_rc = -1;
    if (any_unlinked)
        commit_phase_us_record(&g_commit_marker_clear_us_total, t0c);
    if (any_unlinked && dir_rc == 0 &&
        SHARD_TEST_NOTE_SYNC(SHARD_TEST_PHASE_C)) {
        errno = EIO;
        dir_rc = -1;
    }
    if (dir_rc != 0) return -1; /* no reclaim: a marker may survive crash */

    /* 5. Directory-durable clears: reclaim OLD capacity, then transfer
          terminal ownership. Each window's reclaim uses its own member's
          transaction. This is the only deferred commit_done site. */
    for (size_t m = 0; m < chain->nmembers; m++)
        for (size_t i = 0; i < chain->members[m].rs->nwindows; i++) {
            ReqWindow *rw = &chain->members[m].rs->windows[i];
            if (!rw->unlink_succeeded || rw->cleared) continue;
            rw->cleared = 1;
            bulk_reclaim_old_payloads_locked(&chain->members[m].rs->txn,
                                             &rw->plan);
            if (rw->hooks_staged && rw->hooks.commit_done)
                rw->hooks.commit_done(rw->hooks.ctx, rw->hook_state);
            rw->hooks_staged = 0;
            rw->hook_state = NULL;
        }
    /* Deterministic pause surface for cross-process crash tests (once
       per chain: the same point the per-shard pass offered, scoped to
       this chain's cleared windows). */
    durability_test_pause(chain->db->data_dir, "bulk-window-cleared");
    if (unlink_rc != 0) errno = unlink_errno ? unlink_errno : EIO;
    return unlink_rc;
}
```

Immediately after that section, still before the pipeline definition, add
the chain operations. `BulkChainMember`/`BulkCommitChain` were defined at
the top of the replaced region above; `BULK_MERGE_MAX_JOINERS` and the
`BulkGateWaiter` typedef come from the admission-queue block, which
precedes this point in the file (define both exactly once — there):

```c
static void bulk_commit_chain_init(BulkCommitChain *chain, SlotcaskDb *db,
                                   int kf_shard_id) {
    memset(chain, 0, sizeof(*chain));
    chain->db = db;
    chain->kf_shard_id = kf_shard_id;
}

static void bulk_commit_chain_add(BulkCommitChain *chain,
                                  SlotcaskBulkRequest *req,
                                  SlotcaskBulkShardInput *in,
                                  ReqShard *rs,
                                  BulkGateWaiter *waiter) {
    assert(chain && req && in && rs);
    assert(chain->nmembers < BULK_MERGE_MAX_JOINERS + 1);
    assert(in->kf_shard_id == chain->kf_shard_id);
    assert(chain->nmembers == 0 ||
           in->kind == chain->members[0].in->kind);
    if (chain->nmembers == 0)
        snprintf(chain->kf_dir, sizeof(chain->kf_dir), "%s", req->kf_dir);
    chain->members[chain->nmembers++] =
        (BulkChainMember){ .req = req, .in = in, .rs = rs,
                           .waiter = waiter };
}

/* Per-member outcome fold (the pipeline's former pre-release block):
   retained windows own the terminal state (degraded flag, rc -2);
   hard-failed members report rc -1 plus their first step errno; clean
   members rc 0. Writes in->rc / in->error_no, then runs that member's
   own terminal release — exactly once, under the chain's single gate
   hold, before any waiter is signalled. */
static void bulk_commit_chain_fold_member(BulkChainMember *mem) {
    ReqShard *rs = mem->rs;
    rs->retained = 0;
    for (size_t i = 0; i < rs->nwindows; i++)
        rs->retained |= rs->windows[i].published && !rs->windows[i].cleared;
    if (rs->retained) {
        int *out = rs->kind == BULK_MUTATION_UPSERT
                 ? rs->opts.upsert.out_durability_degraded
                 : rs->opts.delete_.out_durability_degraded;
        if (out) *out = 1;
        mem->in->rc = -2;
    } else if (rs->step_failed) {
        mem->in->rc = -1;
        if (!mem->in->error_no) mem->in->error_no = rs->step_errno;
    }
    slotcask_bulk_shard_release(mem->req, rs->kf_shard_id);
}

/* One owner-preserving commit chain over every member's windows of one
   kf shard, under the committer's single writer-gate hold:
   P (merged) → publish per member → ONE M dir fsync → per-member
   published seam → finalize per member → merged I/K/A/T/clear →
   per-member fold + terminal release. Every step runs even after an
   earlier failure; the per-member and per-window predicates make
   ineligible work no-ops (the same rule the B1 pipeline applied via its
   phase joins). Returns 0 only when every member folded clean; the
   per-member outcomes are read from each member's in->rc. */
static int bulk_commit_chain_run(BulkCommitChain *chain) {
#ifdef TEST_BUILD
    __atomic_add_fetch(&g_shard_test_bulk_chains, 1, __ATOMIC_RELAXED);
#endif
    int rc = 0;

    /* 1. P flush for all members; a failure marks only the affected
          (contributing) member's records -1. */
    if (bulk_commit_chain_flush_payloads(chain) != 0) rc = -1;
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        if (mem->rs->stage_failed || mem->rs->payload_failed)
            for (size_t i = 0; i < mem->in->nrecs; i++)
                if (mem->in->recs[i].status == 0)
                    mem->in->recs[i].status = -1;
    }

    /* 2. Publish each member's windows, then one M directory fsync for
          the chain. */
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        if (mem->rs->stage_failed || mem->rs->payload_failed) continue;
        if (slotcask_bulk_publish_shard(mem->req,
                                        chain->kf_shard_id) != 0) {
            mem->rs->step_failed = 1;
            if (!mem->rs->step_errno)
                mem->rs->step_errno = errno ? errno : EIO;
        }
    }
    if (bulk_commit_chain_flush_marker_dir(chain) != 0) rc = -1;

    /* Per-member published seam: this member's markers are published and
       dir-durable; its finalize has not run. Same surface the B1
       pipeline offered, per member. */
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        ReqShard *rs = mem->rs;
        int any_pub = 0;
        for (size_t i = 0; i < rs->nwindows; i++)
            any_pub |= rs->windows[i].published;
        if (any_pub && !rs->marker_dir_failed) {
            char pause_phase[64];
            snprintf(pause_phase, sizeof(pause_phase),
                     "shard-published-%03x", (unsigned)chain->kf_shard_id);
            SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_SHARD_PUBLISHED);
            durability_test_pause(chain->db->data_dir, pause_phase);
        }
    }

    /* 3. Finalize each member's published windows with that member's
          transaction and shard (its idempotent forward retry runs
          inside). */
    for (size_t m = 0; m < chain->nmembers; m++) {
        BulkChainMember *mem = &chain->members[m];
        if (slotcask_bulk_finalize_shard(mem->req,
                                         chain->kf_shard_id) != 0) {
            mem->rs->step_failed = 1;
            if (!mem->rs->step_errno)
                mem->rs->step_errno = errno ? errno : EIO;
        }
    }

    /* 4. One chain K/index/A/T/clear flush over aggregate temporary
          touch/location sets; each window keeps its own marker path and
          hook state throughout. */
    if (bulk_commit_chain_flush_commit(chain) != 0) rc = -1;

    /* 5. Fold each member's outcome independently and release every
          member before the writer gate is unlocked (the pipeline
          signals waiters only after this returns). */
    for (size_t m = 0; m < chain->nmembers; m++)
        bulk_commit_chain_fold_member(&chain->members[m]);
    return rc;
}

static void bulk_commit_chain_destroy(BulkCommitChain *chain) {
    if (!chain) return;
    memset(chain, 0, sizeof(*chain));
}
```

The chain coordinator is the only new execution seam. Admission is fully
validated before any waiter is marked admitted, so adding a member cannot
fail after admission. `bulk_gate_waiter_take_matching` validates the
shard, mutation kind, and fixed capacity while the waiter is still queued,
then marks the selected waiters admitted while holding their waiter
mutexes. Duplicate shard inputs are already rejected by
`slotcask_bulk_request_execute`, so the queue does not claim to validate
duplicate requests. `bulk_commit_chain_add` then only stores an
already-validated member in the inline array; it performs no allocation
and has no failure return. `bulk_commit_chain_run` returns only after all
members have been folded, released, and have their
`SlotcaskBulkShardInput.rc` and `error_no` fields written.

On any phase failure, the chain propagates the phase result to every
affected member but retains the existing per-window predicates and marker
semantics: a published-but-uncleared member reports `-2`/`EINPROGRESS`;
a hard failure before publication reports `-1`. The chain continues
through cleanup for all members, including members whose earlier phase
failed.

The old single-member path remains behaviorally identical: with an empty
queue the chain has exactly one member and performs pass-for-pass the
same barriers the B1 pipeline performed (one merged P pass, one M dir
fsync, per-window finalize with the same double-attempt retry, one I
flush, one K sync, one A pass, one T pass, one batched clear with one dir
fsync, per-window reclaim + commit_done, one cleared-pause).

The implementation must include complete function bodies; no executor
choice between alternative queue or ownership designs is permitted. An
impossible assertion failure in `bulk_commit_chain_add` is a programming
invariant failure; the executor writes `PLAN_NOTES.md` and halts.

### D2 — opaque bounded admission queues

In `src/db/slotcask.h`, insert the opaque declaration immediately before
the quoted:

```c
typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
```

so it reads:

```c
/* B3b: per-shard bounded admission queues for the bulk commit-chain
   merge. Opaque here; the definition lives in slotcask.c. */
typedef struct BulkGateWaiterQ BulkGateWaiterQ;

typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
```

In `src/db/slotcask.h`, replace the tail of `SlotcaskDb` identified by:

```c
    pthread_mutex_t        *writer_gates;
    size_t                  writer_gates_inited;
```

with:

```c
    pthread_mutex_t        *writer_gates;
    size_t                  writer_gates_inited;
    BulkGateWaiterQ        *writer_gate_waiters;
    size_t                  writer_gate_waiters_inited;
```

Keep `BulkGateWaiterQ` opaque in the header. Its complete definition and
the complete `BulkGateWaiter` definition stay in `slotcask.c`; the queue
must not depend on a `.c`-local macro from the public header.

In `src/db/slotcask.c`, add `#include <assert.h>` (the chain invariant
asserts need it) by replacing the quoted:

```c
#include <fcntl.h>
#include <sys/stat.h>
```

with:

```c
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
```

Immediately before `static int writer_gates_init(SlotcaskDb *db) {`
insert the queue definition and lifecycle bodies (they must precede the
init/destroy call sites in `slotcask_open`/`slotcask_close`):

```c
/* ── B3b: bounded admission queues for the bulk commit-chain merge ──
   One queue per kf shard, allocated with the writer gates. A pipeline
   task registers itself before taking the gate; the current gate holder
   may admit up to BULK_MERGE_MAX_JOINERS queued same-kind requests into
   one commit chain. Queue mutexes are never held across staging, cache
   locks, the writer gate, or I/O.
   BULK_MERGE_MAX_JOINERS is defined exactly once, here: this block
   precedes both the queue-operations block and the chain-flush region,
   which both rely on it. */
#define BULK_MERGE_MAX_JOINERS 4

struct BulkGateWaiter;
typedef struct BulkGateWaiter BulkGateWaiter;

struct BulkGateWaiterQ {
    pthread_mutex_t mu;
    BulkGateWaiter *slot[BULK_MERGE_MAX_JOINERS + 1];
    size_t n;
};

static int bulk_gate_waiters_init(SlotcaskDb *db) {
    db->writer_gate_waiters =
        calloc((size_t)db->num_shards, sizeof(*db->writer_gate_waiters));
    if (!db->writer_gate_waiters) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        int rc = pthread_mutex_init(&db->writer_gate_waiters[s].mu, NULL);
        if (rc != 0) {
            errno = rc;
            while (db->writer_gate_waiters_inited > 0) {
                db->writer_gate_waiters_inited--;
                pthread_mutex_destroy(
                    &db->writer_gate_waiters[
                        db->writer_gate_waiters_inited].mu);
            }
            free(db->writer_gate_waiters);
            db->writer_gate_waiters = NULL;
            return -1;
        }
        db->writer_gate_waiters_inited++;
    }
    return 0;
}

static void bulk_gate_waiters_destroy(SlotcaskDb *db) {
    if (!db->writer_gate_waiters) return;
    while (db->writer_gate_waiters_inited > 0) {
        db->writer_gate_waiters_inited--;
        pthread_mutex_destroy(
            &db->writer_gate_waiters[db->writer_gate_waiters_inited].mu);
    }
    free(db->writer_gate_waiters);
    db->writer_gate_waiters = NULL;
}
```

Initialization increments `writer_gate_waiters_inited` after each
successful mutex init and unwinds only initialized mutexes on failure.
Destruction first checks the pointer and destroys only the initialized
prefix; the init failure path never calls a destructor on a null or
partially initialized array.

Replace `writer_gates_init` and `writer_gates_destroy` wholesale with:

```c
static int writer_gates_init(SlotcaskDb *db) {
    db->writer_gates = calloc((size_t)db->num_shards,
                              sizeof(*db->writer_gates));
    if (!db->writer_gates) return -1;
    for (int s = 0; s < db->num_shards; s++) {
        int rc = pthread_mutex_init(&db->writer_gates[s], NULL);
        if (rc != 0) {
            errno = rc;
            while (db->writer_gates_inited > 0) {
                db->writer_gates_inited--;
                pthread_mutex_destroy(
                    &db->writer_gates[db->writer_gates_inited]);
            }
            free(db->writer_gates);
            db->writer_gates = NULL;
            return -1;
        }
        db->writer_gates_inited++;
    }
    if (bulk_gate_waiters_init(db) != 0) {
        while (db->writer_gates_inited > 0) {
            db->writer_gates_inited--;
            pthread_mutex_destroy(&db->writer_gates[db->writer_gates_inited]);
        }
        free(db->writer_gates);
        db->writer_gates = NULL;
        return -1;
    }
    return 0;
}

static void writer_gates_destroy(SlotcaskDb *db) {
    bulk_gate_waiters_destroy(db);
    while (db->writer_gates_inited > 0) {
        db->writer_gates_inited--;
        pthread_mutex_destroy(&db->writer_gates[db->writer_gates_inited]);
    }
    free(db->writer_gates);
    db->writer_gates = NULL;
}
```

Immediately before the pipeline (after the chain machinery from D1),
define the waiter and the queue operations:

```c
enum { BGW_WAITING = 0, BGW_ADMITTED = 1 };

struct BulkGateWaiter {
    SlotcaskBulkRequest    *req;
    SlotcaskBulkShardInput *in;
    pthread_mutex_t         mu;
    pthread_cond_t          cv;
    int                     state;     /* WAITING or ADMITTED */
    int                     in_queue;
    int                     done;
};

static int bulk_gate_waiter_push(SlotcaskDb *db, int shard,
                                 BulkGateWaiter *w) {
    BulkGateWaiterQ *q = &db->writer_gate_waiters[shard];
    pthread_mutex_lock(&q->mu);
    if (q->n >= BULK_MERGE_MAX_JOINERS + 1) {
        w->in_queue = 0;
        pthread_mutex_unlock(&q->mu);
        return 0;
    }
    q->slot[q->n++] = w;
    w->in_queue = 1;
    pthread_mutex_unlock(&q->mu);
    return 1;
}

static void bulk_gate_waiter_unpush(SlotcaskDb *db, int shard,
                                    BulkGateWaiter *w) {
    BulkGateWaiterQ *q = &db->writer_gate_waiters[shard];
    pthread_mutex_lock(&q->mu);
    if (w->in_queue) {
        for (size_t i = 0; i < q->n; i++) {
            if (q->slot[i] != w) continue;
            memmove(&q->slot[i], &q->slot[i + 1],
                    (q->n - i - 1) * sizeof(q->slot[0]));
            q->n--;
            break;
        }
        w->in_queue = 0;
    }
    pthread_mutex_unlock(&q->mu);
}

/* Admit up to `cap` queued waiters whose input targets `shard` with
   input kind `in_kind` (SLOTCASK_BULK_INPUT_*). FIFO order preserved;
   retained nodes compacted by forward copying. Runs under the caller's
   writer-gate hold; the queue mutex is taken inside it, never the
   reverse. `in_kind` is intentionally the SlotcaskBulkShardInput enum,
   NOT BulkMutationKind — the two enums' values differ. */
static size_t bulk_gate_waiter_take_matching(
        SlotcaskDb *db, int shard, int in_kind,
        BulkGateWaiter **out, size_t cap) {
    BulkGateWaiterQ *q = &db->writer_gate_waiters[shard];
    size_t taken = 0, kept = 0;
    pthread_mutex_lock(&q->mu);
    for (size_t i = 0; i < q->n; i++) {
        BulkGateWaiter *w = q->slot[i];
        if (taken < cap && w->in && w->in->kf_shard_id == shard &&
            (int)w->in->kind == in_kind) {
            pthread_mutex_lock(&w->mu);
            w->state = BGW_ADMITTED;
            w->in_queue = 0;
            pthread_mutex_unlock(&w->mu);
            out[taken++] = w;
        } else {
            q->slot[kept++] = w;
        }
    }
    q->n = kept;
    pthread_mutex_unlock(&q->mu);
    return taken;
}
```

`push` returns 1 only when the node is stored; a full queue returns 0 and
the caller proceeds directly to the gate. `unpush` is idempotent. The
take operation preserves FIFO order, admits at most the requested cap,
checks the input kind while holding the queue mutex, and compacts
retained nodes by forward copying.

In `src/db/slotcask.h`, change the existing tail:

```c
    int rc;                         /* per-shard aggregate result:
                                       0 ok, -1 failed, -2 pending
                                       (retained marker, EINPROGRESS) */
} SlotcaskBulkShardInput;
```

to:

```c
    int rc;                         /* 0 ok, -1 failed, -2 pending */
    int error_no;                   /* first hard error for this shard */
} SlotcaskBulkShardInput;
```

`slotcask_bulk_request_execute` initializes both fields before dispatch
(hunk below). Its caller-side aggregation happens only after
`parallel_for_io` returns: iterate the caller-owned `inputs[]`, set
`EINPROGRESS` if any `rc == -2`, otherwise return the first `error_no`
for an `rc == -1`, falling back to `EIO`. This replaces the request-level
aggregate: remove the three fields `any_pending`, `any_failed`, and
`saved_errno` from `struct SlotcaskBulkRequest` (every reader and writer
was inside the replaced pipeline tail and post-join snapshot) by
replacing the quoted:

```c
    int        *touched;               /* [ntouched] ascending, deduped    */
    size_t      ntouched;
    /* B1: per-shard outcomes are folded into ReqShard before each gate
       release; the request keeps only the aggregate for the final
       errno. */
    int         any_pending;           /* any shard retained markers       */
    int         any_failed;            /* any shard errored or pending     */
    int         saved_errno;           /* first hard failure's errno       */
};
```

with:

```c
    int        *touched;               /* [ntouched] ascending, deduped    */
    size_t      ntouched;
    /* B3b: per-shard outcomes are folded into each SlotcaskBulkShardInput
       (rc / error_no) inside that shard's own pipeline; the request
       object no longer aggregates across pipeline tasks (the old
       any_pending/any_failed/saved_errno writes raced between tasks and
       are read post-join from inputs[] instead). */
};
```

In `slotcask_bulk_request_execute`, replace the quoted:

```c
        inputs[i].rc = 0;
```

with:

```c
        inputs[i].rc = 0;
        inputs[i].error_no = 0;
```

and replace the quoted post-join block:

```c
    /* Snapshot the aggregate before request_end frees req. */
    int any_pending = req->any_pending;
    int any_failed = req->any_failed;
    int saved_errno = req->saved_errno;
    slotcask_bulk_request_end(req);
    if (any_pending) { errno = EINPROGRESS; return -1; }
    if (any_failed) {
        errno = saved_errno ? saved_errno : EIO;
        return -1;
    }
    return 0;
}
```

with:

```c
    /* B3b: per-shard outcomes live in inputs[]; the request object no
       longer aggregates across tasks. */
    int pending = 0, failed = 0, first_errno = 0;
    for (size_t i = 0; i < ninputs; i++) {
        if (inputs[i].rc == -2) pending = 1;
        else if (inputs[i].rc == -1) {
            failed = 1;
            if (!first_errno && inputs[i].error_no)
                first_errno = inputs[i].error_no;
        }
    }
    slotcask_bulk_request_end(req);
    if (pending) { errno = EINPROGRESS; return -1; }
    if (failed) { errno = first_errno ? first_errno : EIO; return -1; }
    return 0;
}
```

Pending takes precedence over hard failure, matching the existing request
contract. `error_no` is written before the owning chain fold returns and
is read only after all tasks have joined.

### D3 — waiter protocol and pipeline integration

Replace the entire `slotcask_bulk_shard_pipeline` function (head quoted
for anchoring; the function ends immediately before the quoted
`typedef struct {` that opens `BulkPipelineTask`):

```c
static void slotcask_bulk_shard_pipeline(SlotcaskBulkRequest *req,
                                         SlotcaskBulkShardInput *in) {
    SlotcaskDb *db = req->db;
    int s = in->kf_shard_id;
    ReqShard *rs = &req->shards[s];

    writer_gate_lock(db, s);
```

with this complete body:

```c
static void slotcask_bulk_shard_pipeline(SlotcaskBulkRequest *req,
                                         SlotcaskBulkShardInput *in) {
    SlotcaskDb *db = req->db;
    int s = in->kf_shard_id;
    ReqShard *rs = &req->shards[s];

    /* B3b: register as a chain candidate BEFORE taking the gate, so the
       current holder can admit this request at its take seam. push
       precedes writer_gate_lock, so a waiter parked at the GATE_WAIT
       test pause is already queued — the tests' barrier. */
    BulkGateWaiter w;
    memset(&w, 0, sizeof(w));
    w.req = req;
    w.in = in;
    w.state = BGW_WAITING;
    pthread_mutex_init(&w.mu, NULL);
    pthread_cond_init(&w.cv, NULL);
    int queued = bulk_gate_waiter_push(db, s, &w);

    writer_gate_lock(db, s);

    if (queued) {
        pthread_mutex_lock(&w.mu);
        int admitted = w.state == BGW_ADMITTED;
        int done = w.done;
        while (admitted && !done) {
            /* Unreachable while the committer signals before its gate
               unlock, but the persistent done flag makes the wakeup
               lossless if scheduling ever interleaves differently. */
            pthread_cond_wait(&w.cv, &w.mu);
            done = w.done;
        }
        pthread_mutex_unlock(&w.mu);
        if (admitted) {
            /* The committer staged, committed, folded, and released this
               shard under its own gate hold; only the gate is left to
               us. Never stage again, run another chain, or touch this
               request's state. */
            writer_gate_unlock(db, s);
            pthread_mutex_destroy(&w.mu);
            pthread_cond_destroy(&w.cv);
            return;
        }
        /* We hold the gate and no holder was at the take seam while we
           waited for it (only a gate holder admits), so the node is
           stale; retire it and run the normal path. */
        bulk_gate_waiter_unpush(db, s, &w);
        pthread_mutex_destroy(&w.mu);
        pthread_cond_destroy(&w.cv);
    }

    /* Stage (gate replay + folded pre-grow + staging). A stage failure
       has already marked every record -1 inside
       slotcask_bulk_stage_shard. */
    uint64_t t0st = now_us();
    if (slotcask_bulk_stage_shard(req, s, in->recs, in->nrecs,
            in->kind == SLOTCASK_BULK_INPUT_UPSERT ? &in->opts.upsert : NULL,
            in->kind == SLOTCASK_BULK_INPUT_DELETE ? &in->opts.delete_
                                                   : NULL) != 0) {
        rs->step_failed = 1;
        if (!rs->step_errno) rs->step_errno = errno ? errno : EIO;
    }
    commit_phase_us_record(&g_bulk_stage_us_total, t0st);

    /* B3b take seam: park surface for the merge tests, then admit queued
       same-kind requests, stage each once under this gate, and run ONE
       owner-preserving chain over all members. The complete member set
       is installed before any joiner is staged; admission already
       happened under the queue mutex with the capacity check, so
       chain_add cannot fail. */
    BulkCommitChain chain;
    bulk_commit_chain_init(&chain, db, s);
    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_PRE_MERGE);
    BulkGateWaiter *joiners[BULK_MERGE_MAX_JOINERS];
    size_t njoiners = bulk_gate_waiter_take_matching(db, s, (int)in->kind,
                                                     joiners,
                                                     BULK_MERGE_MAX_JOINERS);
    bulk_commit_chain_add(&chain, req, in, rs, NULL);
    for (size_t j = 0; j < njoiners; j++) {
        BulkGateWaiter *jd = joiners[j];
        bulk_commit_chain_add(&chain, jd->req, jd->in,
                              &jd->req->shards[s], jd);
    }
    for (size_t j = 0; j < njoiners; j++) {
        BulkGateWaiter *jd = joiners[j];
        uint64_t t0js = now_us();
        if (slotcask_bulk_stage_shard(jd->req, s, jd->in->recs,
                jd->in->nrecs,
                jd->in->kind == SLOTCASK_BULK_INPUT_UPSERT
                    ? &jd->in->opts.upsert : NULL,
                jd->in->kind == SLOTCASK_BULK_INPUT_DELETE
                    ? &jd->in->opts.delete_ : NULL) != 0) {
            ReqShard *jrs = &jd->req->shards[s];
            jrs->step_failed = 1;
            if (!jrs->step_errno) jrs->step_errno = errno ? errno : EIO;
        }
        commit_phase_us_record(&g_bulk_stage_us_total, t0js);
    }
    (void)bulk_commit_chain_run(&chain);
    bulk_commit_chain_destroy(&chain);

    /* Signal each admitted waiter only after every member's fold and
       release: gate held → waiter mu → done=1 → broadcast → unlock the
       waiter mutex, and only then the writer gate. The waiter checks
       state and done under mu; the persistent done flag prevents a lost
       wakeup. The committer never destroys another thread's waiter
       mutex or condition variable. */
    for (size_t j = 0; j < njoiners; j++) {
        BulkGateWaiter *jd = joiners[j];
        pthread_mutex_lock(&jd->mu);
        jd->done = 1;
        pthread_cond_broadcast(&jd->cv);
        pthread_mutex_unlock(&jd->mu);
    }
    writer_gate_unlock(db, s);
}
```

Protocol summary (why this is race-free):

- A queued waiter is blocked either inside `writer_gate_lock` or in its
  post-lock state check; only a gate holder runs `take_matching`, so a
  waiter that holds the gate and observes `BGW_WAITING` can never be
  admitted afterwards. Its `unpush` + destroy is then private.
- Lock order is everywhere gate → queue mutex → waiter mutex; the
  committer signals with gate → waiter mutex. No path takes them in the
  reverse order.
- The committer sets `done = 1` and broadcasts while still holding the
  shard writer gate, then unlocks. An admitted waiter can only acquire
  the gate after that unlock, so it observes `done == 1` and never
  stage/chain-runs again.
- A queue-full waiter (`push` returned 0) skips the whole admission
  branch and simply takes the gate normally.
- The admitted waiter's own `parallel_for_io` task returns only after
  the committer's cleanup, so its `execute()` reads `in->rc` post-join
  with a proper happens-before edge.

The per-input result write, per-owner release, waiter signalling, and
gate unlock all appear in the replacement body above; no partial hunk or
prose-only replacement is valid.

### D4 — failure and ownership invariants

The implementation must prove and test all of these:

- Every record in every member ends nonzero: success, policy rejection,
  hard failure, or pending durability. (Scenarios 13, 16, 17, 18.)
- A failure in P, M, finalize, K/index/A/T, or C affects only the
  relevant member's result and record statuses, while published markers
  remain replayable. (Scenarios 16–18: merged-phase failures hit exactly
  the contributing members; per-member apply-hook failure isolates one
  member inside a shared chain.)
- Every member's transaction state, `BulkMutationShard.st`, old buffers,
  `res_map`, payload locations, windows, and hooks are released exactly
  once by that member's own release path (`bulk_commit_chain_fold_member`
  → `slotcask_bulk_shard_release`, called once per member).
  Hook-count assertions in scenario 18; sanitizer gates cover the frees.
- The chain holds exactly one writer gate and never waits for another
  gate (G1 counter assertion in scenarios 13–15; the chain runs entirely
  under the committer's single hold).
- Same-kind requests only; mixed upsert/delete requests serialize
  normally. (Scenario 14.)
- Empty queue is behaviorally equivalent to B2 (solo-chain equivalence
  argument in D1; the existing `test-request-flush-batching` scenarios
  1–12 must stay green unchanged).
- A crash at every existing durability pause leaves valid marker
  evidence and startup forward-replay remains unchanged (pause sites
  preserved: P/M/A/I/K/T/C note-syncs, per-member shard-published seam,
  chain cleared-pause; covered by the existing cross-process crash tests
  plus scenario 17's retained-marker replay).

### D5 — test seams

In `src/db/shard_test_ctl.h`, replace the quoted enum tail:

```c
    SHARD_TEST_PHASE_SHARD_PUBLISHED, /* per shard: this shard's windows'
                                       markers published + kf dir fsync
                                       done, before this shard's finalize
                                       (B1 pipeline; supersedes the
                                       request-wide REQ_PUBLISHED seam) */
    SHARD_TEST_PHASE_COUNT
} ShardTestPhase;
```

with:

```c
    SHARD_TEST_PHASE_SHARD_PUBLISHED, /* per shard: this shard's windows'
                                       markers published + kf dir fsync
                                       done, before this shard's finalize
                                       (B1 pipeline; supersedes the
                                       request-wide REQ_PUBLISHED seam) */
    SHARD_TEST_PHASE_PRE_MERGE,       /* B3b: parks a shard-pipeline gate
                                       holder immediately before it takes
                                       matching waiters from the merge
                                       queue (red-run position: pipeline
                                       entry; final position: the take
                                       seam). */
    SHARD_TEST_PHASE_GATE_WAIT,       /* B3b: fires in writer_gate_lock on
                                       try-lock contention, after the
                                       caller's queue push and before the
                                       blocking lock — a hit proves the
                                       caller is queued. */
    SHARD_TEST_PHASE_COUNT
} ShardTestPhase;
```

Beside the existing extern declarations, add the chain counter by
inserting after the quoted:

```c
extern _Atomic long g_shard_test_gate_held_max;
```

the line:

```c
/* B3b: count of bulk commit-chain coordinator invocations. A solo
   request is 1; a merged pair is 1. */
extern _Atomic long g_shard_test_bulk_chains;
```

and in `shard_test_ctl_reset`, replace the quoted:

```c
    atomic_store(&g_shard_test_gate_held_max, 0);
}
```

with:

```c
    atomic_store(&g_shard_test_gate_held_max, 0);
    atomic_store(&g_shard_test_bulk_chains, 0);
}
```

In `src/db/slotcask.c`, add the matching definition by replacing the
quoted:

```c
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
_Atomic long g_shard_test_gate_held_max;
```

with:

```c
long g_shard_test_sync_counts[SHARD_TEST_PHASE_COUNT];
_Atomic long g_shard_test_gate_held_max;
_Atomic long g_shard_test_bulk_chains;
```

For the base-compatible contention seam, replace the exact existing
block:

```c
static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
#ifdef TEST_BUILD
```

with a complete block that uses `pthread_mutex_trylock`: if the try-lock
reports contention, call
`SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_GATE_WAIT)` and then perform
the existing blocking lock; if it succeeds, continue directly. Preserve
the existing held-gate accounting after the lock. The non-TEST_BUILD
path remains one blocking lock and has no pause or counter:

```c
static void writer_gate_lock(SlotcaskDb *db, int kf_shard) {
#ifdef TEST_BUILD
    if (pthread_mutex_trylock(&db->writer_gates[kf_shard]) != 0) {
        SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_GATE_WAIT);
        pthread_mutex_lock(&db->writer_gates[kf_shard]);
    }
#else
    pthread_mutex_lock(&db->writer_gates[kf_shard]);
#endif
#ifdef TEST_BUILD
    t_writer_gates_held++;
    long cur = atomic_load(&g_shard_test_gate_held_max);
    while (t_writer_gates_held > cur &&
           !atomic_compare_exchange_weak(&g_shard_test_gate_held_max, &cur,
                                         (long)t_writer_gates_held)) {}
#endif
}
```

Tests may arm only one pause phase at a time (the control header's
single `g_shard_test_pause_phase`); the scenarios below re-arm between
parks. Because `GATE_WAIT` fires for any contending gate acquirer,
tests must arm it only while bulk pipelines are the expected
contenders (single-record writers and legacy multi-gate holders must
not be parked mid-acquisition).

## Call-site / consumer inventory

- `slotcask_bulk_shard_pipeline`: replaced wholesale (waiter
  registration, admission, chain construction, stage of joiners, chain
  run, per-input result write via the fold, per-owner release, waiter
  signalling, gate unlock).
- `slotcask_bulk_shard_flush_payloads`, `..._flush_marker_dir`,
  `..._flush_kf`, `..._flush_commit`: deleted; superseded by the chain
  variants in D1. No other translation unit references them (verified by
  search: all references are in `slotcask.c`).
- `slotcask_bulk_stage_shard`, `slotcask_bulk_publish_shard`,
  `slotcask_bulk_finalize_shard`, `slotcask_bulk_shard_release`: reused
  verbatim, called per member with that member's own request handle
  (Task 0 audit: each operates on an owner member when the member's own
  `SlotcaskBulkRequest`/`BulkMutationTxn`/`ReqShard` is passed — which
  the chain guarantees).
- `writer_gates_init` / `writer_gates_destroy`: queue lifecycle and
  partial initialization failure paths (complete bodies in D2).
- `SlotcaskDb` in `slotcask.h`: opaque queue pointer and initialized
  count; daemon, test, bench, and library targets must rebuild.
- `slotcask_bulk_request_execute`: unchanged externally; admitted tasks
  return only after their own cleanup has completed. Its post-join
  result aggregation is the sole writer of request-level error state
  (complete hunks in D2).
- `SlotcaskBulkShardInput` consumers: every construction/copy site in
  `src/db/query_bulk.c` (the `SlotcaskBulkShardInput input;` and
  `calloc(np, sizeof(*inputs))` sites) passes through
  `slotcask_bulk_request_execute`, which initializes `rc` and
  `error_no`. `query_bulk.c` reads only `.rc` after the call (verified);
  `error_no` needs no changes there. This is a source-struct change, not
  a wire or on-disk format change; embedded callers must recompile, and
  the build must rebuild daemon, test, bench, and library targets.
- `struct SlotcaskBulkRequest`: `.c`-local; loses the three aggregate
  fields (complete hunk in D2).
- Lower-level window helpers (`bulk_publish_one_kf_window`,
  `bulk_apply_and_sync_kf_locked`, `bulk_tombstone_old_payloads_locked`,
  …): unchanged. Each member's `txn->req` stays intact, so the nonce/
  kf-dir reads and the tombstone-deferral branch keep operating on the
  member's own request.
- TEST-only controls: `shard_test_ctl.h` enum + counter, `slotcask.c`
  definition, `writer_gate_lock` seam — consumed by the new regression
  tests only.
- Docs: `docs/concepts/concurrency.md` and
  `docs/reference/changelog.md`.

## Tasks

Execution is on a fresh branch stacked on
`perf/b3a-path-epoch-syncs`, remains uncommitted, and stops immediately on
any missing quoted anchor. Use `SKIP_TESTS=1 ./build.sh` for builds and
`./build/bin/shard-db-test run <name>` for focused tests. Do not run benches
or sanitizer gates as the executor.

### Task 0 — coupling audit (completed 2026-09-11)

Done; findings recorded in `PLAN_NOTES.md`. The audit found the request
coupling described in the Revision 2 note; execution halted as required.
This revision is the patched plan that audit's halt protocol calls for.
Task 0a's demand — complete transformed bodies plus the complete pipeline
replacement pasted before approval — is satisfied by D1/D2/D3. If the
executor finds any further field or signature mismatch against these
blocks, it writes `PLAN_NOTES.md` and halts; it does not adapt them.

### Task 1 — red test and seams

Apply the D5 hunks (enum, counter extern/reset/definition,
`writer_gate_lock` seam), plus the red-only pipeline seam: replace the
quoted pipeline head

```c
    writer_gate_lock(db, s);

    /* Stage (gate replay + folded pre-grow + staging). A stage failure
```

with:

```c
    writer_gate_lock(db, s);
#ifdef TEST_BUILD
    __atomic_add_fetch(&g_shard_test_bulk_chains, 1, __ATOMIC_RELAXED);
#endif
    SHARD_TEST_PHASE_PAUSE(SHARD_TEST_PHASE_PRE_MERGE);

    /* Stage (gate replay + folded pre-grow + staging). A stage failure
```

(Task 2's pipeline replacement removes this increment — the counter then
lives only at the first statement of `bulk_commit_chain_run` — and
relocates the pause to the take seam.)

In `src/test/cases/test_request_flush_batching.c`, insert the helper
thread/args types after the quoted `rf_two_shard_req_thread` function:

```c
/* Drives one execute() over one FIXED shard input (B3b scenarios). */
struct RfOneShardArgs {
    RfDb *w;
    SlotcaskBulkShardInput *in;
    int done;
    int rc;
    int err;
};

static void *rf_one_shard_req_thread(void *raw) {
    struct RfOneShardArgs *a = raw;
    a->rc = slotcask_bulk_request_execute(&a->w->db, a->in, 1);
    a->err = errno;
    a->done = 1;
    return NULL;
}

/* Poll until the armed pause counter reaches `n` (B3b choreographies):
 * each hit proves the corresponding thread passed its push / reached
 * its seam. Never used as the admission proof itself — the GATE_WAIT
 * pause hit is. */
static void rf_wait_pause_hits_at_least(int n) {
    for (int i = 0; i < 30000 && atomic_load(&g_shard_test_pause_hits) < n;
         i++)
        usleep(1000);
}
```

Then insert the new scenarios immediately before the quoted closing:

```c
    rf_db_close(&w);
    return t_ctx->failed > 0 ? 1 : 0;
}
```

Scenario 13 is the red/green merge test; on the base behavior it must
fail with `chains == 2`:

```c
    /* ── Scenario 13 (B3b): deterministic same-shard chain merge ──
     * A parks at the PRE_MERGE seam holding shard 0's gate; B is proven
     * queued (GATE_WAIT pause fires after B's push, before its blocking
     * lock); release lets A admit B: one chain, two members. Three
     * consecutive cycles prove no waiter/queue state leaks. Red on
     * base: the counter lives at the pipeline head, so two requests
     * report chains == 2. */
    {
        static RfBatch ba, bb;
        static char keysa[16][24], valsa[16][24];
        static SlotcaskBulkRec recsa[16];
        static SlotcaskBulkRec perma[16];
        static char keysb[16][24], valsb[16][24];
        static SlotcaskBulkRec recsb[16];
        static SlotcaskBulkRec permb[16];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&ba, 60, 16, "v60", 0);    /* all shard 0 */
        rf_batch_fill(&bb, 61, 16, "v61", 0);    /* all shard 0 */

        for (int cycle = 0; cycle < 3; cycle++) {
            shard_test_ctl_reset();
            g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
            g_shard_test_pause_occurrence = 1;

            RfReqThr ta = { .w = &w, .b = &ba, .done = 0, .rc = 0 };
            pthread_t tha;
            ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_req_thread, &ta),
                          0, "spawn A (committer)");
            rf_wait_pause_hit();
            ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 1,
                        "A parked at the PRE_MERGE seam holding the gate");

            g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
            g_shard_test_pause_occurrence = 1;
            RfReqThr tb = { .w = &w, .b = &bb, .done = 0, .rc = 0 };
            pthread_t thb;
            ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_req_thread, &tb),
                          0, "spawn B (same shard, must queue)");
            rf_wait_pause_hits_at_least(2);
            ASSERT_TRUE(atomic_load(&g_shard_test_pause_hits) >= 2,
                        "B hit the GATE_WAIT barrier: queued before "
                        "A's take");

            atomic_store(&g_shard_test_pause_release, 1);
            pthread_join(tha, NULL);
            pthread_join(thb, NULL);
            ASSERT_EQ_INT(ta.rc, 0, "A rc 0");
            ASSERT_EQ_INT(tb.rc, 0, "B rc 0");
            long chains = atomic_load(&g_shard_test_bulk_chains);
            ASSERT_EQ_INT((int)chains, 1,
                          "one chain covered both requests");
            ASSERT_TRUE(rf_record_visible(&w, ba.keys[0], "v60-0000") &&
                        rf_record_visible(&w, bb.keys[0], "v61-0000"),
                        "both members' records visible");
            ASSERT_EQ_INT(rt_marker_scan(w.base), 0,
                          "no markers retained");
        }
    }
```

Scenario 14 — mixed-kind non-admission (regression guard: passes on
base, catches over-admission after B3b):

```c
    /* ── Scenario 14 (B3b): mixed kinds never share a chain ── */
    {
        static RfBatch ba;
        static char keysa[8][24], valsa[8][24];
        static SlotcaskBulkRec recsa[8];
        static SlotcaskBulkRec perma[8];
        static char keysb[8][24];
        static SlotcaskBulkRec recsb[8];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        rf_batch_fill(&ba, 62, 8, "v62", 0);
        SlotcaskUpsertOpts seed_opts;
        memset(&seed_opts, 0, sizeof(seed_opts));
        for (size_t i = 0; i < 8; i++) {
            rf_key_shard0("rf-mk", (int)i, keysb[i], sizeof(keysb[i]));
            ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, keysb[i],
                                                     strlen(keysb[i]),
                                                     "seed", 4, &seed_opts,
                                                     NULL), 0,
                          "seed delete key");
            memset(&recsb[i], 0, sizeof(recsb[i]));
            recsb[i].key = keysb[i];
            recsb[i].klen = strlen(keysb[i]);
        }

        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;

        static SlotcaskBulkShardInput ina, inb;
        SlotcaskBulkOpts oa;
        rf_fill_opts(&oa, NULL);
        memset(&ina, 0, sizeof(ina));
        ina.kf_shard_id = 0; ina.recs = recsa; ina.nrecs = ba.n;
        ina.kind = SLOTCASK_BULK_INPUT_UPSERT; ina.opts.upsert = oa;
        SlotcaskBulkDeleteOpts ob;
        memset(&ob, 0, sizeof(ob));
        memset(&inb, 0, sizeof(inb));
        inb.kf_shard_id = 0; inb.recs = recsb; inb.nrecs = 8;
        inb.kind = SLOTCASK_BULK_INPUT_DELETE; inb.opts.delete_ = ob;

        static struct RfOneShardArgs ta;
        ta.w = &w; ta.in = &ina; ta.done = 0; ta.rc = 0; ta.err = 0;
        pthread_t tha;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_one_shard_req_thread,
                                     &ta), 0, "spawn A (upsert)");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        static struct RfOneShardArgs tb;
        tb.w = &w; tb.in = &inb; tb.done = 0; tb.rc = 0; tb.err = 0;
        pthread_t thb;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_one_shard_req_thread,
                                     &tb), 0, "spawn B (delete)");
        rf_wait_pause_hits_at_least(2);
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        ASSERT_EQ_INT(ta.rc, 0, "A (upsert) rc 0");
        ASSERT_EQ_INT(tb.rc, 0, "B (delete) rc 0");
        long chains = atomic_load(&g_shard_test_bulk_chains);
        ASSERT_EQ_INT((int)chains, 2,
                      "mixed kinds serialize: two chains");
        ASSERT_TRUE(rf_record_visible(&w, ba.keys[0], "v62-0000"),
                    "A's record visible");
        ASSERT_TRUE(!rf_record_visible(&w, keysb[0], "seed"),
                    "B's delete applied");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "no markers retained");
    }
```

Scenario 15 — fixed admission capacity (15a) and queue-full fallback
(15b). On base both fail on their chains assertions (every request runs
its own pipeline); after B3b, 15a is exactly one chain and 15b proves
the overflow waiter falls through cleanly:

```c
    /* ── Scenario 15 (B3b): admission capacity + queue-full fallback ── */
    {
        enum { N15 = 7 };   /* committer + 6 candidates */
        static RfBatch b15[N15];
        static char k15[N15][16][24], v15[N15][16][24];
        static SlotcaskBulkRec r15[N15][16];
        static SlotcaskBulkRec p15[N15][16];
        static SlotcaskBulkShardInput ins15[N15];
        static struct RfOneShardArgs tas15[N15];
        static pthread_t ths15[N15];
        SlotcaskBulkOpts o15;
        rf_fill_opts(&o15, NULL);
        for (int r = 0; r < N15; r++) {
            b15[r].keys = k15[r]; b15[r].vals = v15[r];
            b15[r].recs = r15[r]; b15[r].perm = p15[r];
            rf_batch_fill(&b15[r], 63 + r, 16, "v63", 0);
            memset(&ins15[r], 0, sizeof(ins15[r]));
            ins15[r].kf_shard_id = 0;
            ins15[r].recs = r15[r]; ins15[r].nrecs = 16;
            ins15[r].kind = SLOTCASK_BULK_INPUT_UPSERT;
            ins15[r].opts.upsert = o15;
        }

        /* 15a: committer parked pre-merge, four waiters queued → one
         * five-member chain (BULK_MERGE_MAX_JOINERS + committer). */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;
        tas15[0].w = &w; tas15[0].in = &ins15[0];
        tas15[0].done = 0; tas15[0].rc = 0; tas15[0].err = 0;
        ASSERT_EQ_INT(pthread_create(&ths15[0], NULL,
                                     rf_one_shard_req_thread,
                                     &tas15[0]), 0, "15a spawn committer");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        for (int r = 1; r <= 4; r++) {
            tas15[r].w = &w; tas15[r].in = &ins15[r];
            tas15[r].done = 0; tas15[r].rc = 0; tas15[r].err = 0;
            ASSERT_EQ_INT(pthread_create(&ths15[r], NULL,
                                         rf_one_shard_req_thread,
                                         &tas15[r]), 0, "15a spawn waiter");
        }
        rf_wait_pause_hits_at_least(5);
        atomic_store(&g_shard_test_pause_release, 1);
        for (int r = 0; r <= 4; r++) pthread_join(ths15[r], NULL);
        for (int r = 0; r <= 4; r++)
            ASSERT_EQ_INT(tas15[r].rc, 0, "15a request rc 0");
        long chains15a = atomic_load(&g_shard_test_bulk_chains);
        ASSERT_EQ_INT((int)chains15a, 1,
                      "15a: one chain covered all five requests");
        for (int r = 0; r <= 4; r++)
            ASSERT_TRUE(rf_record_visible(&w, k15[r][0], v15[r][0]),
                        "15a record visible");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "15a no markers");

        /* 15b: six candidates — the queue holds five, the sixth falls
         * through to the gate. All seven must commit exactly once with
         * no retained markers and no waiter state leaking. The retained
         * fifth either runs its own chain or is admitted by the sixth,
         * so chains ∈ {2,3}. */
        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;
        tas15[0].w = &w; tas15[0].in = &ins15[0];
        tas15[0].done = 0; tas15[0].rc = 0; tas15[0].err = 0;
        ASSERT_EQ_INT(pthread_create(&ths15[0], NULL,
                                     rf_one_shard_req_thread,
                                     &tas15[0]), 0, "15b spawn committer");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        for (int r = 1; r <= 6; r++) {
            tas15[r].w = &w; tas15[r].in = &ins15[r];
            tas15[r].done = 0; tas15[r].rc = 0; tas15[r].err = 0;
            ASSERT_EQ_INT(pthread_create(&ths15[r], NULL,
                                         rf_one_shard_req_thread,
                                         &tas15[r]), 0, "15b spawn waiter");
        }
        rf_wait_pause_hits_at_least(7);
        atomic_store(&g_shard_test_pause_release, 1);
        for (int r = 0; r <= 6; r++) pthread_join(ths15[r], NULL);
        for (int r = 0; r <= 6; r++)
            ASSERT_EQ_INT(tas15[r].rc, 0, "15b request rc 0");
        long chains15b = atomic_load(&g_shard_test_bulk_chains);
        ASSERT_TRUE(chains15b >= 2 && chains15b <= 3,
                    "15b: queue-full overflow serialized cleanly");
        for (int r = 0; r <= 6; r++)
            ASSERT_TRUE(rf_record_visible(&w, k15[r][0], v15[r][0]),
                        "15b record visible");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "15b no markers");
        ASSERT_TRUE(atomic_load(&g_shard_test_gate_held_max) <= 1,
                    "15b: G1 — at most one gate held per thread");
    }
```

Build and run the test before the merge implementation:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-request-flush-batching
```

Paste the real failure output: scenario 13 must fail with
`chains == 2` (scenarios 15a/15b fail their chains assertions on base
too — that is the expected red state for the admission tests; scenario
14 passes on base by design). Also run
`./build/bin/shard-db-test run-all` once and paste the summary so the
pre-existing green set is on record.

### Task 2 — implementation and focused verification

Apply D1, D2, D3 in that order (the D3 pipeline replacement removes
Task 1's red-only seam). Then add the failure-injection scenarios below,
immediately before the same `rf_db_close(&w);` anchor (after scenario
15). First insert the counting-hook helper beside the other hook
helpers (after `rf_fill_opts`):

```c
/* Counting hook set with fail_all switch (B3b member-isolation tests). */
typedef struct {
    int prepare, apply, commit_done, release_window, abort_window;
    int fail_all;
} RfHookCtl;
static int rf_hc_prepare(SlotcaskBulkRec *recs, const size_t *active,
                         size_t nactive, void *ctx, void **out_window_state) {
    (void)recs; (void)active; (void)nactive;
    ((RfHookCtl *)ctx)->prepare++;
    *out_window_state = NULL;
    return 0;
}
static int rf_hc_apply(SlotcaskBulkRec *recs, const size_t *active,
                       size_t nactive, void *ctx, void *window_state) {
    (void)recs; (void)active; (void)nactive; (void)window_state;
    RfHookCtl *c = ctx;
    c->apply++;
    return c->fail_all ? -1 : 0;
}
static void rf_hc_commit_done(void *ctx, void *window_state) {
    (void)window_state;
    ((RfHookCtl *)ctx)->commit_done++;
}
static void rf_hc_release_window(void *ctx, void *window_state) {
    (void)window_state;
    ((RfHookCtl *)ctx)->release_window++;
}
static void rf_hc_abort_window(void *ctx, void *window_state) {
    (void)window_state;
    ((RfHookCtl *)ctx)->abort_window++;
}
static void rf_fill_hc_opts(SlotcaskBulkOpts *o, RfHookCtl *c) {
    memset(o, 0, sizeof(*o));
    o->has_indexed_fields = 1;
    o->prepare_window = rf_hc_prepare;
    o->apply_window = rf_hc_apply;
    o->commit_done = rf_hc_commit_done;
    o->release_window = rf_hc_release_window;
    o->abort_window = rf_hc_abort_window;
    o->bulk_hook_ctx = c;
}
```

Scenario 16 — merged P failure hits exactly the chain's contributors
(red on base: base fails only the first request's pipeline):

```c
    /* ── Scenario 16 (B3b): merged P failure → both members fail ── */
    {
        static RfBatch ba, bb;
        static char keysa[16][24], valsa[16][24];
        static SlotcaskBulkRec recsa[16];
        static SlotcaskBulkRec perma[16];
        static char keysb[16][24], valsb[16][24];
        static SlotcaskBulkRec recsb[16];
        static SlotcaskBulkRec permb[16];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&ba, 70, 16, "v70", 0);
        rf_batch_fill(&bb, 71, 16, "v71", 0);

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_P;
        g_shard_test_fail_occurrence = 1;
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;

        static SlotcaskBulkShardInput ina, inb;
        SlotcaskBulkOpts oa, ob;
        rf_fill_opts(&oa, NULL);
        rf_fill_opts(&ob, NULL);
        memset(&ina, 0, sizeof(ina));
        ina.kf_shard_id = 0; ina.recs = recsa; ina.nrecs = ba.n;
        ina.kind = SLOTCASK_BULK_INPUT_UPSERT; ina.opts.upsert = oa;
        memset(&inb, 0, sizeof(inb));
        inb.kf_shard_id = 0; inb.recs = recsb; inb.nrecs = bb.n;
        inb.kind = SLOTCASK_BULK_INPUT_UPSERT; inb.opts.upsert = ob;

        static struct RfOneShardArgs ta, tb;
        ta.w = &w; ta.in = &ina; ta.done = 0; ta.rc = 0; ta.err = 0;
        tb.w = &w; tb.in = &inb; tb.done = 0; tb.rc = 0; tb.err = 0;
        pthread_t tha, thb;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_one_shard_req_thread,
                                     &ta), 0, "16 spawn A");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_one_shard_req_thread,
                                     &tb), 0, "16 spawn B");
        rf_wait_pause_hits_at_least(2);
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        ASSERT_EQ_INT(ta.rc, -1, "16 A rc -1 (merged P failed)");
        ASSERT_EQ_INT(tb.rc, -1, "16 B rc -1 (merged P failed)");
        ASSERT_TRUE(ta.err != EINPROGRESS && tb.err != EINPROGRESS,
                    "16 pre-M failure is not EINPROGRESS");
        int bad = 0;
        for (size_t i = 0; i < ba.n; i++) bad |= recsa[i].status != -1;
        for (size_t i = 0; i < bb.n; i++) bad |= recsb[i].status != -1;
        ASSERT_TRUE(bad == 0, "16 all member records -1");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "16 no markers");
        ASSERT_TRUE(!rf_record_visible(&w, keysa[0], "v70-0000") &&
                    !rf_record_visible(&w, keysb[0], "v71-0000"),
                    "16 nothing committed");
    }
```

Scenario 17 — chain K failure retains both members; gate replay
converges:

```c
    /* ── Scenario 17 (B3b): chain K failure → both retained → replay ── */
    {
        static RfBatch ba, bb;
        static char keysa[16][24], valsa[16][24];
        static SlotcaskBulkRec recsa[16];
        static SlotcaskBulkRec perma[16];
        static char keysb[16][24], valsb[16][24];
        static SlotcaskBulkRec recsb[16];
        static SlotcaskBulkRec permb[16];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&ba, 72, 16, "v72", 0);
        rf_batch_fill(&bb, 73, 16, "v73", 0);

        shard_test_ctl_reset();
        g_shard_test_fail_phase = SHARD_TEST_PHASE_K;
        g_shard_test_fail_occurrence = 1;
        g_shard_test_fail_sticky = 1;
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;

        static SlotcaskBulkShardInput ina, inb;
        int deg_a = 0, deg_b = 0;
        SlotcaskBulkOpts oa, ob;
        rf_fill_opts(&oa, NULL); oa.out_durability_degraded = &deg_a;
        rf_fill_opts(&ob, NULL); ob.out_durability_degraded = &deg_b;
        memset(&ina, 0, sizeof(ina));
        ina.kf_shard_id = 0; ina.recs = recsa; ina.nrecs = ba.n;
        ina.kind = SLOTCASK_BULK_INPUT_UPSERT; ina.opts.upsert = oa;
        memset(&inb, 0, sizeof(inb));
        inb.kf_shard_id = 0; inb.recs = recsb; inb.nrecs = bb.n;
        inb.kind = SLOTCASK_BULK_INPUT_UPSERT; inb.opts.upsert = ob;

        static struct RfOneShardArgs ta, tb;
        ta.w = &w; ta.in = &ina; ta.done = 0; ta.rc = 0; ta.err = 0;
        tb.w = &w; tb.in = &inb; tb.done = 0; tb.rc = 0; tb.err = 0;
        pthread_t tha, thb;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_one_shard_req_thread,
                                     &ta), 0, "17 spawn A");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_one_shard_req_thread,
                                     &tb), 0, "17 spawn B");
        rf_wait_pause_hits_at_least(2);
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        g_shard_test_fail_phase = -1; g_shard_test_fail_occurrence = 0;
        g_shard_test_fail_sticky = 0;
        /* a->rc is execute()'s return value; the per-input fold result
         * (rc -2 for a retained shard) lives on the input struct. */
        ASSERT_EQ_INT(ta.in->rc, -2, "17 A retained");
        ASSERT_EQ_INT(tb.in->rc, -2, "17 B retained");
        ASSERT_EQ_INT(ta.err, EINPROGRESS, "17 A EINPROGRESS");
        ASSERT_EQ_INT(tb.err, EINPROGRESS, "17 B EINPROGRESS");
        ASSERT_EQ_INT(deg_a, 1, "17 A degraded");
        ASSERT_EQ_INT(deg_b, 1, "17 B degraded");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 2,
                      "17 one retained marker per member");

        /* Golden follow-up write on the shard: gate replay converges
           BOTH retained markers. */
        char fk[24], fv[24];
        rf_key_shard0("rf-follow", 17, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow17");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "17 follow-up replay converges");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "17 markers cleared");
        ASSERT_TRUE(rf_record_visible(&w, keysa[0], "v72-0000") &&
                    rf_record_visible(&w, keysb[0], "v73-0000"),
                    "17 both members' records visible after replay");
    }
```

Scenario 18 — member isolation inside one shared chain (per-member
apply failure; hook counts prove exactly-one terminal callback):

```c
    /* ── Scenario 18 (B3b): one member's apply failure isolates only
     * that member inside a shared chain ── */
    {
        static RfBatch ba, bb;
        static char keysa[16][24], valsa[16][24];
        static SlotcaskBulkRec recsa[16];
        static SlotcaskBulkRec perma[16];
        static char keysb[4][24], valsb[4][24];
        static SlotcaskBulkRec recsb[4];
        static SlotcaskBulkRec permb[4];
        ba.keys = keysa; ba.vals = valsa; ba.recs = recsa; ba.perm = perma;
        bb.keys = keysb; bb.vals = valsb; bb.recs = recsb; bb.perm = permb;
        rf_batch_fill(&ba, 74, 16, "v74", 0);
        rf_batch_fill(&bb, 75, 4, "v75", 0);

        RfHookCtl ctl_a, ctl_b;
        memset(&ctl_a, 0, sizeof(ctl_a));
        memset(&ctl_b, 0, sizeof(ctl_b));
        ctl_b.fail_all = 1;

        shard_test_ctl_reset();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_PRE_MERGE;
        g_shard_test_pause_occurrence = 1;

        static SlotcaskBulkShardInput ina, inb;
        SlotcaskBulkOpts oa, ob;
        int deg_b = 0;
        rf_fill_hc_opts(&oa, &ctl_a);
        rf_fill_hc_opts(&ob, &ctl_b);
        ob.out_durability_degraded = &deg_b;
        memset(&ina, 0, sizeof(ina));
        ina.kf_shard_id = 0; ina.recs = recsa; ina.nrecs = ba.n;
        ina.kind = SLOTCASK_BULK_INPUT_UPSERT; ina.opts.upsert = oa;
        memset(&inb, 0, sizeof(inb));
        inb.kf_shard_id = 0; inb.recs = recsb; inb.nrecs = bb.n;
        inb.kind = SLOTCASK_BULK_INPUT_UPSERT; inb.opts.upsert = ob;

        static struct RfOneShardArgs ta, tb;
        ta.w = &w; ta.in = &ina; ta.done = 0; ta.rc = 0; ta.err = 0;
        tb.w = &w; tb.in = &inb; tb.done = 0; tb.rc = 0; tb.err = 0;
        pthread_t tha, thb;
        ASSERT_EQ_INT(pthread_create(&tha, NULL, rf_one_shard_req_thread,
                                     &ta), 0, "18 spawn A");
        rf_wait_pause_hit();
        g_shard_test_pause_phase = SHARD_TEST_PHASE_GATE_WAIT;
        g_shard_test_pause_occurrence = 1;
        ASSERT_EQ_INT(pthread_create(&thb, NULL, rf_one_shard_req_thread,
                                     &tb), 0, "18 spawn B");
        rf_wait_pause_hits_at_least(2);
        atomic_store(&g_shard_test_pause_release, 1);
        pthread_join(tha, NULL);
        pthread_join(thb, NULL);
        long chains = atomic_load(&g_shard_test_bulk_chains);
        ASSERT_EQ_INT((int)chains, 1, "18 one shared chain");
        ASSERT_EQ_INT(ta.in->rc, 0, "18 A clean");
        ASSERT_EQ_INT(tb.in->rc, -2, "18 B retained");
        ASSERT_EQ_INT(deg_b, 1, "18 B degraded");
        ASSERT_TRUE(rf_record_visible(&w, keysa[0], "v74-0000"),
                    "18 A committed despite B's failure");
        ASSERT_TRUE(!rf_record_visible(&w, keysb[0], "v75-0000"),
                    "18 B not visible before replay");
        /* Exactly one terminal callback per window: A committed, B
           released (both finalize attempts failed → one release). */
        ASSERT_EQ_INT(ctl_a.prepare, 1, "18 A one window prepared");
        ASSERT_EQ_INT(ctl_a.apply, 1, "18 A one apply");
        ASSERT_EQ_INT(ctl_a.commit_done, 1, "18 A commit_done once");
        ASSERT_EQ_INT(ctl_b.prepare, 1, "18 B one window prepared");
        ASSERT_TRUE(ctl_b.apply >= 2, "18 B finalize retried");
        ASSERT_EQ_INT(ctl_b.release_window, 1, "18 B release once");
        ASSERT_EQ_INT(ctl_b.commit_done, 0, "18 B never commit_done");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 1, "18 B marker retained");

        char fk[24], fv[24];
        rf_key_shard0("rf-follow", 18, fk, sizeof(fk));
        snprintf(fv, sizeof(fv), "follow18");
        SlotcaskUpsertOpts so;
        memset(&so, 0, sizeof(so));
        ASSERT_EQ_INT(slotcask_upsert_with_hooks(&w.db, 0, fk, strlen(fk),
                                                 fv, strlen(fv), &so, NULL),
                      0, "18 follow-up replay converges B");
        ASSERT_TRUE(rf_record_visible(&w, keysb[0], "v75-0000"),
                    "18 B visible after replay");
        ASSERT_EQ_INT(rt_marker_scan(w.base), 0, "18 markers cleared");
    }
```

Verification sequence (paste every output):

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-request-flush-batching
./build/bin/shard-db-test run test-durability-epoch-sync
./build/bin/shard-db-test run test-durability-epoch-marker-dir
./build/bin/shard-db-test run test-single-op-index-sync
```

Scenario 13 must now pass with `chains == 1` (paste the green output
alongside the red output from Task 1). Do not run sanitizer gates as
part of executor Task 2. Run the regular all-core suite with the human's
approval and paste all actual outputs; never claim a pass from an
unshown command.

### Task 3 — documentation

At the quoted `Per shard the pipeline runs, in order` block in
`docs/concepts/concurrency.md`, state that the gate holder may admit up to
four already-queued same-kind bulk requests and run one owner-preserving
chain; state that uncontended requests are unchanged and member cleanup is
per-owner. Update the `## Unreleased` paragraph in
`docs/reference/changelog.md` with the group-commit behavior.

### Task 4 — human definition-of-done gates

The human runs the ordinary suite once, then each sanitizer suite three
fresh times with default all-core parallelism:

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all

BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
./build/bin/shard-db-test run-all
./build/bin/shard-db-test run-all

BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
TSAN_OPTIONS="second_deadlock_stack=1:print_stacktrace=1" ./build/bin/shard-db-test run-all
```

No `--jobs` override, suppressions file, or `halt_on_error=0` is allowed.
Any sanitizer finding is root-caused and fixed or explicitly escalated in
`PLAN_NOTES.md`; it is not papered over.

After the gates, the human measures five-run medians before/after at the
1M/splits=64 collapsed shape and the 5M parity guard, plus the existing
indexed `bench-parallel` target. If the primary shape does not recover
materially, record the result and stop before planning B3c.

## Out of scope

- B3c single-record group commit.
- K-barrier epochization across shards, adaptive window sizing, and a
  configurable joiner cap.
- Scan/reindex paths, segment-coalescer changes, marker-format changes,
  and the B3a epoch module.
