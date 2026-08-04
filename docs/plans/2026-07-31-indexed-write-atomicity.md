# Plan B — atomic indexed writes

**Status: REVISED DRAFT v2 — do not execute until explicitly approved and
re-reviewed.**

**Dependency:** Plan A (2026-07-31-index-atomic-publication.md) is complete and
has passed raw-diff review. This plan is not executable before that dependency.

## Revision-v2 review disposition

| Review finding | Binding revision |
|---|---|
| Delete marker was not representable or forward-replayable | `KfMarkerSlot.op` now has explicit UPSERT/DELETE encodings, preserves legacy `op=0`, has a complete validity predicate, and specifies marker-only, sidecar-paired, and orphan-sidecar delete recovery. |
| `seg_write_flag` anchor was ambiguous | The caller audit records both occurrences and directs insertion after the definition only. |
| Other anchors were not unique | The audit table declares all four known repeated anchors, their exact counts, and their targets. |
| `has_indexed_fields` was stale | The plan now requires forwarding the existing fields and adds them only to the new delete option structs. |
| Degraded-result semantics were unspecified | A dedicated contract distinguishes committed-cleanup degradation from rejected, abort-pending mutations. |
| Change steps lacked literal structure/function blocks | The revision supplies complete blocks for the marker format, abort header helpers, delete-option API, delete forward/abort recovery, durable flag write, and null-safe index extraction. |
| Bulk Kf sync could retain a forward marker as degraded | The post-publication bulk sync failure now performs synchronous forward replay or fails closed; it never takes the abort path or reports degraded. |

## Goal and binding invariants

Every indexed mutation has one contract: successful responses are Kf-visible
only after every required B-tree, trigram, and bitmap change is durable. Once
an index-apply failure is reported, that mutation can never become visible
later. A durable failure decision recovers by abort, never forward replay.

**Root cause.** The current post-marker failure branch retains its normal
commit-intent marker and routes recovery through `kf_marker_replay_*`. That
replay applies the forward index diff and publishes/repoints Kf, even when the
client has already received an index-apply failure. The legacy bulk-update
path is worse: it may publish Kf before its `pre_commit` index diff fails.
Thus the durable evidence currently represents only *forward* intent; it has
no durable, ordered decision that the same marker must be aborted instead.

**Delete is in scope.** “Every indexed mutation” includes single delete,
key-list bulk delete, and criteria bulk delete. A delete's Kf tombstone must
not become visible until all B-tree/trigram/bitmap removals are durable. An
index-remove failure therefore uses the same durable abort decision, but the
abort inverse is a forward index insert and leaves the OLD segment live.

The scope is deliberately complete: single insert, single update, single
delete, bulk insert, JSON bulk update, delimiter/CSV bulk update, structured
bulk update, key-list bulk delete, and criteria bulk delete. No legacy indexed
`pre_commit` path is exempt.

## Mandatory caller audit

Before coding, record `rg -n` output for every anchor below in `PLAN_NOTES.md`.
An unexpected indexed caller halts the run for a revised plan. The following
anchors are intentionally non-unique and all listed occurrences are targets;
their multiplicity is therefore not an anchor mismatch:

| Anchor | Required occurrences and meaning |
|---|---|
| `bulk_upsert_slow_in_kfshard(` | declaration, dispatch call, definition (3) |
| `cmd_delete_v2(` | definition and `cmd_delete` call (2) |
| `static int seg_write_flag(` | forward declaration and definition (2); add the new helper after the definition only |
| `SlotcaskUpsertOpts opts = {` | `cmd_insert_v2` and `cmd_update_v2` initializers (2) |

All other anchors in this section must occur exactly once unless this plan
explicitly says otherwise:

- ~slotcask_upsert_with_hooks(~, ~slotcask_insert_with_hooks(~,
  ~slotcask_bulk_upsert_in_kfshard(~, ~bulk_upsert_slow_in_kfshard(~, and
  ~if (opts->apply_window(~ in src/db/slotcask.c;
- ~v2_bulk_ins_apply_window(~, ~v2_bulk_upd_pre_commit_bulk(~,
  ~v2_bulk_upd_delim_pre_commit_bulk(~, and
  ~v2_bulk_upd_json_pre_commit_bulk(~ in src/db/query_bulk.c;
- ~v2_insert_apply_commit(~ and ~v2_update_pre_commit(~ in src/db/storage.c;
- ~cmd_delete_v2(~ and ~v2_delete_pre_commit(~ in src/db/storage.c;
- ~slotcask_delete_with_hooks(~ and ~slotcask_bulk_delete_in_kfshard(~ in
  src/db/slotcask.c, plus ~v2_bulk_del_pre_commit_bulk(~ and
  ~v2_bulk_del_crit_pre_commit_bulk(~ in src/db/query_bulk.c;
- the single-record commit-intent marker family in src/db/slotcask.c —
  ~kf_marker_path(~, ~kf_marker_write(~, ~kf_marker_gate(~, and
  ~kf_marker_replay_current(~ (per-shard ~%03x_marker.dat~ files, a distinct
  family from the batch markers Task 1 extends) — because Task 2 gives
  single-record inserts/updates their own abort sidecar.

**On-disk consumer audit.** Task 1 changes the transient `KfMarkerSlot`
format. The executor must record all of these consumers before editing it:
`KfMarkerSlot` in `src/db/shard_db_internal.h`; `kf_marker_write`,
`kf_marker_read`, `kf_marker_replay_entry_locked`, `kf_marker_replay_locked`,
`kf_marker_replay_current`, `kf_marker_gate`, `kf_batch_marker_write`,
`kf_batch_marker_gate`, `marker_recovery_sweep_object`, and every
`KfMarkerSlot marker =` / `KfMarkerSlot *mslots` producer in
`src/db/slotcask.c`; and the TEST_BUILD marker constructors in
`src/test/cases/test_slotcask_v2_crash.c`. There are no persistent external
consumers: marker files are internal crash-recovery artifacts and are removed
on a clean successful operation. Nevertheless, `op=0` is retained as the
legacy forward-upsert spelling so a binary upgraded while an old marker is on
disk can recover it; no migration is required.

## Task 1 — durable abort evidence and recovery

### Test first

Immediately before the unique registration anchor
~TEST_REGISTER("test-slotcask-v2-crash", test_slotcask_v2_crash_run)~ in
src/test/cases/test_slotcask_v2_crash.c, add parser tests for valid,
truncated, checksum-invalid, wrong-shard, wrong-batch-id, and
marker-count-mismatched sidecars. Each invalid state fails closed and retains
its evidence.

Immediately before
~TEST_REGISTER("test-msync-range", test_msync_range_raw_fails_on_main)~ in
src/test/cases/test_durability_ordering.c, add daemon/restart tests for:
crash immediately after durable sidecar creation; injected inverse-index
failure; injected durable segment-flag failure; pure insert undo; update undo;
delete forward replay (marker only); delete undo (marker plus sidecar); and
orphan sidecar discovery before an unrelated batch-id reuse. Each test uses
a distinct object and port plus append_durability_pause_config, wait_for_path,
and test_env_stop_keep.

### Change

At ~static void kf_batch_marker_path(~ in src/db/slotcask.c, add batch and
single-record abort sidecars. The exact fixed, on-disk header is below; do not
derive its size from a pointer or accept trailing bytes. `kind` is the only
distinction between `%03x_batch_%u_abort.dat` and `%03x_marker_abort.dat`.

~~~c
enum { KF_ABORT_MAGIC = 0x4b464142u, KF_ABORT_VERSION = 1 };
enum { KF_ABORT_SINGLE = 1, KF_ABORT_BATCH = 2 };

typedef struct {
    uint32_t magic;
    uint16_t version;
    uint16_t kind;
    uint32_t kf_shard;
    uint32_t batch_id;       /* 0 for KF_ABORT_SINGLE */
    uint32_t marker_count;   /* exactly 1 for KF_ABORT_SINGLE */
    uint32_t checksum;       /* XXH32 through marker_count */
} KfAbortHeader;

_Static_assert(sizeof(KfAbortHeader) == 24,
               "abort sidecar header is a fixed on-disk format");
~~~

Immediately after `#define KF_MARKER_MAGIC` in
`src/db/shard_db_internal.h`, replace the marker declaration with this complete
32-byte-compatible definition. `op` consumes the first byte previously
reserved; the remaining four bytes must be zero whenever a marker is written.

~~~c
#define KF_MARKER_MAGIC 0x4B464D31u /* "KFM1" */

enum KfMarkerOp {
    KF_MARKER_OP_LEGACY_UPSERT = 0, /* v1 marker writer; recover as UPSERT */
    KF_MARKER_OP_UPSERT = 1,
    KF_MARKER_OP_DELETE = 2,
};

typedef struct {
    uint32_t magic;        /* KF_MARKER_MAGIC */
    uint32_t kf_slot;      /* UPDATE/DELETE: existing slot; INSERT: UINT32_MAX */
    uint32_t old_offset;
    uint32_t new_offset;
    uint16_t old_file_id;
    uint16_t new_file_id;
    uint8_t  old_stream_id;
    uint8_t  new_stream_id;
    uint8_t  has_old;      /* UPSERT: 0=insert, 1=update; DELETE: always 1 */
    uint8_t  op;           /* enum KfMarkerOp */
    uint8_t  reserved[4];  /* must be zero */
    uint32_t checksum;     /* XXH32 over [0, offsetof(checksum)) */
} KfMarkerSlot;

_Static_assert(sizeof(KfMarkerSlot) == 32,
               "KfMarkerSlot must stay 32 bytes");

static inline int kf_marker_op_valid(const KfMarkerSlot *marker) {
    if (!marker) return 0;
    if (marker->op != KF_MARKER_OP_LEGACY_UPSERT &&
        marker->op != KF_MARKER_OP_UPSERT &&
        marker->op != KF_MARKER_OP_DELETE)
        return 0;
    if (marker->reserved[0] || marker->reserved[1] ||
        marker->reserved[2] || marker->reserved[3])
        return 0;
    if (marker->op == KF_MARKER_OP_DELETE)
        return marker->has_old == 1 && marker->kf_slot != UINT32_MAX &&
               marker->new_offset == 0 && marker->new_file_id == 0 &&
               marker->new_stream_id == 0;
    return marker->has_old <= 1;
}
~~~

Every existing upsert producer must set `.op = KF_MARKER_OP_UPSERT` explicitly.
Every delete producer must set `.op = KF_MARKER_OP_DELETE`, `.has_old = 1`,
populate only `kf_slot` and the OLD location, and zero all NEW location fields.
Every marker reader validates `magic`, checksum, and `kf_marker_op_valid()`
before using a location. This is an on-disk format extension, not a changed
file size: the checksum covers `op` and the reserved bytes, and the old
all-zero value remains a supported, forward-upsert marker.

Create a sidecar with `O_WRONLY|O_CREAT|O_EXCL`, one complete `pwrite`,
`fsync(fd)`, `close(fd)`, and `fsync_dir(data/kf)`. A pre-existing sidecar is
idempotent only after `kf_abort_read_exact` validates every header field
against the requested `(kind, shard, batch_id, marker_count)`; otherwise it is
corrupt evidence and must fail closed. Add the following complete helpers at
that anchor (the existing `fsync_dir` is the helper used below):

~~~c
static int kf_abort_read_exact(const char *path, uint16_t want_kind,
                               uint32_t want_shard, uint32_t want_batch,
                               uint32_t want_count, KfAbortHeader *out) {
    struct stat st;
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return errno == ENOENT ? 1 : -1;
    if (fstat(fd, &st) != 0 || st.st_size != (off_t)sizeof(*out)) {
        int saved = errno ? errno : EILSEQ; close(fd); errno = saved; return -1;
    }
    ssize_t n = pread(fd, out, sizeof(*out), 0);
    int saved = errno;
    close(fd);
    if (n != (ssize_t)sizeof(*out)) { errno = n < 0 ? saved : EILSEQ; return -1; }
    if (out->magic != KF_ABORT_MAGIC || out->version != KF_ABORT_VERSION ||
        out->kind != want_kind || out->kf_shard != want_shard ||
        out->batch_id != want_batch || out->marker_count != want_count ||
        out->checksum != XXH32(out, offsetof(KfAbortHeader, checksum), 0)) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

static int kf_abort_clear_after_marker(const char *abort_path,
                                       const char *kf_dir) {
    if (unlink(abort_path) != 0 && errno != ENOENT) return -1;
    return fsync_dir(kf_dir);
}
~~~

For a batch, the paired marker is valid only when its exact `st_size` is
`marker_count * sizeof(KfMarkerSlot)`, `marker_count > 0`, every entry has
`KF_MARKER_MAGIC` and a valid checksum, and a final `pread` confirms EOF.
For a single marker, use the existing exact-size reader plus `marker_count=1`.
Never use `fread`-until-EOF for abort recovery: a truncated marker, a partial
entry, an extra entry, a mismatched filename/header shard or batch id, or a
duplicate sidecar is corrupt evidence. Leave both files in place and invoke
`kf_marker_fail_closed`.

At ~static int kf_marker_gate(~, ~static int kf_batch_marker_gate(~, and
~int marker_recovery_sweep_object(~, collect marker and sidecar ids. For each
marker: no sidecar selects the existing forward replay; valid sidecar selects
abort; corrupt/short/extra evidence calls kf_marker_fail_closed. A sidecar
without a marker is revalidated and cleared only as completed cleanup. The
single-record write-time gate must apply the same pairing rule before its
existing forward replay; `kf_shard_marker_gate` already calls it before the
batch gate. The startup sweep consumes every pair before serving, but this
write-time rule prevents an executor from treating the single family as an
exception. Update ~int object_has_pending_markers(~ so a graceful shutdown
never writes clean state while a recognized abort sidecar remains.

The cleanup order is binding and is deliberately asymmetric:

1. apply every inverse index diff and durably tombstone each speculative NEW
   segment (or, for a delete abort, make no segment change);
2. unlink the forward marker and `fsync_dir(data/kf)`;
3. unlink the abort sidecar and `fsync_dir(data/kf)`.

The implementation must never clear the abort sidecar first. A crash after
step 2 leaves an orphan, valid sidecar; its absence of a marker proves that
the abort completed, so recovery may clear it. A crash before step 2 leaves
both files and must redo abort. This is the only circumstance in which a
sidecar without a marker may be removed.

Immediately before the **definition** occurrence of
~static int kf_marker_replay_entry_locked(~, add three complete recovery
helpers. `read_marker_old_live` and `read_marker_new_live` are the existing
record-reading body factored from the old replayer; both verify the segment
record is live and return its owned value buffer plus key/hash metadata. The
following functions define every state transition; their called helpers are
the existing marker/index/Kf helpers named in the code.

~~~c
static int kf_marker_replay_delete_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfMarkerSlot *marker) {
    MarkerRecord old_rec = {0};
    int rc = -1;

    if (!kf_marker_op_valid(marker) || marker->op != KF_MARKER_OP_DELETE ||
        read_marker_old_live(data_dir, marker, &old_rec) != 0)
        goto out;
    if (kf_marker_apply_recovery_diff(eff_root, object, kf_shard,
                                      marker->kf_slot, &old_rec, NULL) != 0)
        goto out;
    if (kf_marker_verify_kf_old_at_slot(kh, marker->kf_slot, &old_rec,
                                        marker, data_dir) != 0)
        goto out;
    size_t slot = marker->kf_slot;
    kf_tombstone_at_slot(kh, slot);
    if (kfcache_sync_slots_locked(kh, &slot, 1, 1) != 0 ||
        seg_write_flag_durable(data_dir, marker->old_stream_id,
                               marker->old_file_id, marker->old_offset, 2) != 0)
        goto out;
    rc = 0;
out:
    marker_record_destroy(&old_rec);
    return rc;
}

static int kf_marker_replay_entry_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        void *kh_opaque, const KfMarkerSlot *marker) {
    SlotcaskKfHandle *kh = (SlotcaskKfHandle *)kh_opaque;

    if (!kh || !kh->writer || !kf_marker_op_valid(marker)) {
        errno = EILSEQ;
        return -1;
    }
    if (marker->op == KF_MARKER_OP_DELETE)
        return kf_marker_replay_delete_entry_locked(eff_root, object,
                                                     data_dir, kf_shard, kh,
                                                     marker);
    return kf_marker_replay_upsert_entry_locked(eff_root, object, data_dir,
                                                 kf_shard, kh, marker);
}

static int kf_batch_marker_abort_locked(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        SlotcaskKfHandle *kh, const KfMarkerSlot *markers, size_t count,
        const char *marker_path, const char *abort_path) {
    for (size_t i = 0; i < count; i++) {
        const KfMarkerSlot *marker = &markers[i];
        if (!kf_marker_op_valid(marker)) { errno = EILSEQ; goto failed; }
        if (marker->op == KF_MARKER_OP_DELETE) {
            if (kf_marker_apply_abort_diff(eff_root, object, data_dir,
                    kf_shard, marker->kf_slot, marker) != 0)
                goto failed;
            continue; /* OLD remains live and Kf was never tombstoned. */
        }
        if (kf_marker_apply_abort_diff(eff_root, object, data_dir,
                kf_shard, marker->kf_slot, marker) != 0 ||
            seg_write_marker_new_tombstone_durable(data_dir, marker) != 0)
            goto failed;
    }
    char kf_dir[PATH_MAX];
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    if (unlink(marker_path) != 0 || fsync_dir(kf_dir) != 0)
        goto failed;
    if (kf_abort_clear_after_marker(abort_path, kf_dir) != 0)
        goto failed;
    return 0;
failed:
    kf_marker_fail_closed(data_dir, kf_shard, "abort recovery");
    return -1;
}
~~~

The extraction helpers used above are not left to interpretation. Add these
complete definitions immediately before the replayer block; both forward and
abort recovery use these same record handles, so segment mappings remain held
until the registered index callback has finished.

~~~c
static int seg_write_flag_durable(const char *data_dir, uint8_t stream_id,
                                  uint16_t file_id, uint32_t offset,
                                  uint8_t flag);

typedef struct {
    SlotcaskSegHandle handle;
    const uint8_t *key;
    const uint8_t *value;
    uint8_t hash[16];
    uint16_t klen;
    uint32_t vlen;
    int open;
} MarkerRecord;

static void marker_record_destroy(MarkerRecord *record) {
    if (record && record->open) segcache_release(&record->handle);
    if (record) memset(record, 0, sizeof(*record));
}

static int marker_record_read_live(const char *data_dir, uint8_t stream_id,
        uint16_t file_id, uint32_t offset, MarkerRecord *out) {
    char path[PATH_MAX];
    const uint8_t *record;

    if (!data_dir || !out) { errno = EINVAL; return -1; }
    memset(out, 0, sizeof(*out));
    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&out->handle, path, 0, 0, 0) != 0) return -1;
    out->open = 1;
    record = out->handle.map + offset;
    if (__atomic_load_n(&record[18], __ATOMIC_ACQUIRE) != 1) {
        errno = EILSEQ;
        marker_record_destroy(out);
        return -1;
    }
    memcpy(out->hash, record, sizeof(out->hash));
    out->klen = seg_rec_klen(record);
    out->vlen = seg_rec_vlen(record);
    out->key = record + 24;
    out->value = out->key + out->klen;
    return 0;
}

static int read_marker_old_live(const char *data_dir,
        const KfMarkerSlot *marker, MarkerRecord *out) {
    if (!marker || !marker->has_old) { errno = EILSEQ; return -1; }
    return marker_record_read_live(data_dir, marker->old_stream_id,
                                   marker->old_file_id, marker->old_offset,
                                   out);
}

static int read_marker_new_live(const char *data_dir,
        const KfMarkerSlot *marker, MarkerRecord *out) {
    if (!marker || marker->op == KF_MARKER_OP_DELETE) {
        errno = EILSEQ;
        return -1;
    }
    return marker_record_read_live(data_dir, marker->new_stream_id,
                                   marker->new_file_id, marker->new_offset,
                                   out);
}

static int kf_marker_apply_recovery_diff(const char *eff_root,
        const char *object, int kf_shard, uint32_t kf_slot,
        const MarkerRecord *old_record, const MarkerRecord *new_record) {
    char err_buf[256] = {0};
    const MarkerRecord *identity = new_record ? new_record : old_record;

    if (!identity) { errno = EINVAL; return -1; }
    if (!g_recovery_index_diff_fn) return 0;
    return g_recovery_index_diff_fn(eff_root, object, kf_shard, kf_slot,
        identity->hash,
        old_record ? old_record->value : NULL,
        old_record ? old_record->vlen : 0,
        new_record ? new_record->value : NULL,
        new_record ? new_record->vlen : 0, err_buf, sizeof(err_buf));
}

static int kf_marker_verify_kf_old_at_slot(SlotcaskKfHandle *kh,
        size_t expected_slot, const MarkerRecord *old_record,
        const KfMarkerSlot *marker, const char *data_dir) {
    uint8_t flag, stream_id;
    uint16_t file_id;
    uint32_t offset;
    size_t found_slot;

    if (!kh || !old_record || !marker ||
        kf_lookup_with_slot(kh, old_record->hash, old_record->key,
                            old_record->klen, data_dir, &flag, &stream_id,
                            &file_id, &offset, &found_slot) != 0 ||
        found_slot != expected_slot || flag != 1 ||
        stream_id != marker->old_stream_id || file_id != marker->old_file_id ||
        offset != marker->old_offset) {
        errno = EILSEQ;
        return -1;
    }
    return 0;
}

static int seg_write_marker_new_tombstone_durable(const char *data_dir,
        const KfMarkerSlot *marker) {
    if (!marker || marker->op == KF_MARKER_OP_DELETE) {
        errno = EILSEQ;
        return -1;
    }
    return seg_write_flag_durable(data_dir, marker->new_stream_id,
                                  marker->new_file_id, marker->new_offset, 2);
}

static int kf_marker_apply_abort_diff(const char *eff_root,
        const char *object, const char *data_dir, int kf_shard,
        uint32_t kf_slot, const KfMarkerSlot *marker) {
    MarkerRecord old_record = {0}, new_record = {0};
    int rc;

    if (!kf_marker_op_valid(marker)) { errno = EILSEQ; return -1; }
    if (marker->op == KF_MARKER_OP_DELETE) {
        rc = read_marker_old_live(data_dir, marker, &old_record) == 0
            ? kf_marker_apply_recovery_diff(eff_root, object, kf_shard,
                                            kf_slot, NULL, &old_record)
            : -1;
        marker_record_destroy(&old_record);
        return rc;
    }
    if (read_marker_new_live(data_dir, marker, &new_record) != 0) return -1;
    if (marker->has_old && read_marker_old_live(data_dir, marker,
                                                &old_record) != 0) {
        marker_record_destroy(&new_record);
        return -1;
    }
    rc = kf_marker_apply_recovery_diff(eff_root, object, kf_shard, kf_slot,
                                       &new_record,
                                       marker->has_old ? &old_record : NULL);
    marker_record_destroy(&old_record);
    marker_record_destroy(&new_record);
    return rc;
}
~~~

`kf_marker_replay_upsert_entry_locked` is the old complete upsert body,
renamed without behavioral changes, except it rejects `op=DELETE` and accepts
both `op=0` and `op=1`. The complete `MarkerRecord`, record-reader,
verification, index-diff, and tombstone helper blocks above are binding;
`kf_marker_replay_upsert_entry_locked` uses them by factoring the old replayer
without duplicating its Kf-publish body. It passes explicit nullable OLD/NEW
records to `kf_marker_apply_recovery_diff`, so a NULL record is never passed
to `build_index_key_from_record_into`.

This gives both delete crash directions their binding behavior:

| Durable evidence at crash | Recovery action |
|---|---|
| delete marker only | Forward delete: remove OLD from indexes, tombstone and sync its Kf slot, durably tombstone OLD segment, clear marker. |
| delete marker + valid abort sidecar | Abort delete: insert OLD into indexes, leave OLD segment and Kf slot live, clear marker then sidecar. |
| delete abort sidecar only | Validate it, then clear it; the missing marker proves the abort completed. |

At the **definition** occurrence of ~static int seg_write_flag(~, add this
complete helper after `seg_write_flag`; do not add it at the forward
declaration occurrence:

~~~c
static int seg_write_flag_durable(const char *data_dir, uint8_t stream_id,
                                  uint16_t file_id, uint32_t offset,
                                  uint8_t flag) {
    char path[PATH_MAX];
    SlotcaskSegHandle h;
    int rc = -1;

    seg_path_for(path, data_dir, stream_id, file_id);
    if (segcache_acquire(&h, path, 0, 0, 1) != 0) return -1;
    __atomic_store_n(&h.map[offset + 18], flag, __ATOMIC_RELEASE);
    if (h.slot >= 0) {
        SegCacheEntry *entry = &g_segcache[h.slot];
        durability_mark_dirty(&entry->dirty, &entry->dirty_since_ms);
    }
    if (durability_msync_range(h.map, offset + 18, 1) == 0 &&
        fdatasync(h.fd) == 0)
        rc = 0;
    segcache_release(&h);
    return rc;
}
~~~

At ~static int apply_index_diff(~ in src/db/storage.c, retain the existing
function body but make this exact replacement at all three NEW-value extraction
sites (arena `*_into`, its `rn == -1` fallback, and the non-arena direct path):

~~~c
        int rn = a->new_value
            ? build_index_key_from_record_into(a->idx_ts, a->new_value,
                                               a->idx_fields[i], new_slot,
                                               INDEX_KEY_MAX, &new_len)
            : 0;
        have_new = (rn == 1);
        new_buf = have_new ? new_slot : NULL;
        if (rn == -1) {
            have_new = a->new_value
                ? build_index_key_from_record(a->idx_ts, a->new_value,
                                              a->idx_fields[i], &new_buf,
                                              &new_len)
                : 0;
            if (have_new) fb_bufs[n_fb++] = new_buf;
        }
~~~

~~~c
            if (a->new_value)
                have_new = build_index_key_from_record(a->idx_ts,
                                                       a->new_value,
                                                       a->idx_fields[i],
                                                       &new_buf, &new_len);
~~~

`old_value == NULL` remains a forward insert and `new_value == NULL` is a
delete or insert-abort inverse. Preserve `sync_after = 1` in every generated
`UpdateIdxArg`, and return only after `bm_flush_thread_bitmap_cache()`.

## Task 2 — migrate all indexed write paths

### Test first

For each group below, inject an index-apply failure after one durable index
mutation; prove primary lookup and indexed count return old/absent results
before and after restart. Each matrix row must run once for B-tree, trigram,
and bitmap, not merely whichever two a shared fixture happens to support:

1. single insert;
2. single update;
3. bulk insert;
4. JSON bulk update;
5. delimiter/CSV bulk update;
6. structured bulk update.
7. single delete;
8. key-list bulk delete;
9. criteria bulk delete.

Add a TEST_BUILD-only deterministic hook immediately before each real index
operation is dispatched: `indexed_abort_fail_after=N` decrements after a
successful durable B-tree/trigram/bitmap mutation and returns `EIO` at zero.
The hook must be shared by the single, bulk-upsert, and delete adapters so the
test can prove partial apply rather than a failure before any mutation. Each
test pauses at `abort-sidecar-after-fsync`, verifies the sidecar exists, kills
the daemon, restarts it, and asserts all of: direct get/exists, indexed find,
indexed count, and the other two index classes show the prior state. It must
then retry the same key or batch id to prove an orphan sidecar cannot mask a
new write.

Place tests beside their existing caller-family anchors from the caller audit.
Do not combine index classes into a permissive fixture: each B-tree, trigram,
and bitmap run needs its own asserted query result and restart assertion.

### Change

At ~if (opts->apply_window(~ in bulk_upsert_slow_in_kfshard, replace the
current keep_marker branch with: write sidecar; resolve abort while holding the
Kf writer lock; mark every active record rejected; skip Kf commit/repoint; then
return the apply error. The client must not receive that error before sidecar
fsync succeeds.

Within the unique parent anchor ~if (opts->prepare_window && opts->apply_window)~
in `bulk_upsert_slow_in_kfshard`, replace its `if (nvslots > 0 &&
kfcache_sync_slots_locked(...))` block (the first of the two such blocks in
that function) with the following. The later occurrence is the legacy branch
and is intentionally non-targeted as recorded below. Replace the old `keep_marker`/
`out_durability_degraded` branch with the following complete hunk. This is
forward convergence, not abort: the Kf map has already been repointed, so an
abort could expose a Kf/index mismatch.

~~~c
                if (nvslots > 0 &&
                    kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                    int saved = errno ? errno : EIO;
                    if (kf_batch_marker_replay_current_locked(
                            db->data_dir, kf_shard_id, &kh, mslots, nsurvive,
                            bpath) != 0) {
                        errno = saved;
                        kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                              "indexed bulk Kf sync");
                        hard_error = 1;
                    }
                }
~~~

`hard_error` is a new `int`, initialized to zero with `keep_marker` at the
window declaration; after releasing the Kf handle, a nonzero value makes
`slotcask_bulk_upsert_in_kfshard` return `-1` without changing any record's
successful status. The caller treats this as an unknown-outcome durability
error, never as a rejected index-apply error and never as
`durability_degraded`. On successful synchronous replay, the forward diff,
the Kf slots, and marker cleanup have all converged, so the normal successful
path continues with `out_durability_degraded` still zero. The legacy
single-phase branch at the second matching source text (`:6119` at audit time)
is outside this plan because Task 2 removes every indexed caller from it; list
that occurrence in `PLAN_NOTES.md` as intentionally non-targeted.

Immediately before `kf_batch_marker_abort_locked`, add this complete helper;
it is the batch equivalent of the existing single-marker
`kf_marker_replay_current` and is used only after a post-publication Kf sync
failure:

~~~c
static int kf_batch_marker_replay_current_locked(const char *data_dir,
        int kf_shard, SlotcaskKfHandle *kh, const KfMarkerSlot *markers,
        size_t count, const char *marker_path) {
    char eff_root[PATH_MAX], object[256], kf_dir[PATH_MAX];

    if (!data_dir || !kh || !kh->writer || !markers || count == 0 ||
        !marker_path) {
        errno = EINVAL;
        return -1;
    }
    split_data_dir(data_dir, eff_root, sizeof(eff_root), object,
                   sizeof(object));
    for (size_t i = 0; i < count; i++) {
        if (kf_marker_replay_entry_locked(eff_root, object, data_dir,
                                          kf_shard, kh, &markers[i]) != 0)
            return -1;
    }
    snprintf(kf_dir, sizeof(kf_dir), "%s/data/kf", data_dir);
    if (unlink(marker_path) != 0 || fsync_dir(kf_dir) != 0)
        return -1;
    return 0;
}
~~~

At the three option initializers anchored by:

~~~c
.pre_commit = v2_bulk_upd_pre_commit_bulk,
.pre_commit = v2_bulk_upd_delim_pre_commit_bulk,
.pre_commit = v2_bulk_upd_json_pre_commit_bulk,
~~~

replace legacy post-Kf pre_commit wiring with prepare_window, apply_window, and
abort_window adapters. Preparation may reject policy/cap failures before marker
creation. Apply failures are only I/O/OOM and enter the common sidecar abort
resolver. No indexed bulk-update caller may reach the legacy pre_commit loop.

The exact migration rule for all three adapters is: use `prepare_window` only
for allocation/cap/CAS checks and staging; use `apply_window` for the forward
diff; its `abort_window` frees staging without writing; and route a nonzero
`apply_window` through `kf_batch_marker_abort_locked`, not `keep_marker`.
The same replacement applies to the existing bulk-insert adapter.
`has_indexed_fields` already exists in both `SlotcaskUpsertOpts` and
`SlotcaskBulkOpts`; do not add it. Instead, initialize it at **every**
migrated upsert initializer with the exact expression used to load that
caller's indexed fields (`nidx > 0` / `w->nidx > 0`). Add it to the new delete
option structs too, and route `has_indexed_fields == 1` through the marker and
sidecar state machine unconditionally. A missing initializer is a plan error,
not permission to select the legacy branch.

At the SlotcaskUpsertOpts initializers in cmd_insert_v2 and cmd_update_v2,
anchored by ~SlotcaskUpsertOpts opts = {~, use the same prepare/apply/abort
state machine for both inserts and updates. Note that
~SlotcaskUpsertOpts opts = {~ is intentionally non-unique: it occurs exactly
twice, one initializer in cmd_insert_v2 and one in cmd_update_v2, and both
are targets — enumerate both occurrences in PLAN_NOTES.md during the caller
audit so the non-unique-anchor halt rule does not misfire. A single-record
abort sidecar mirrors batch semantics; it extends the single-record marker
family anchored in the caller audit (kf_marker_path / kf_marker_write /
kf_marker_gate / kf_marker_replay_current, per-shard %03x_marker.dat files)
as %03x_marker_abort.dat, with the same fixed-header sidecar protocol Task 1
defines for batch markers. The old single-marker forward replay remains only
for crashes before an abort decision is durable.

Extend `SlotcaskDeleteOpts` and `SlotcaskBulkDeleteOpts` with the same
prepare/apply/abort state-machine fields and migrate `cmd_delete_v2`,
`v2_bulk_del_pre_commit_bulk`, and `v2_bulk_del_crit_pre_commit_bulk`. For
deletes the forward diff is `(old=OLD,new=NULL)` and the abort inverse is
`(old=NULL,new=OLD)`. The primitive must write a delete marker before applying
the forward diff, call apply, then synchronously tombstone the Kf slot; on
apply failure it writes the matching abort sidecar, performs the inverse while
the writer lock remains held, rejects the record, and returns the original
apply error only after the sidecar was fsynced. Bulk criteria delete may no
longer defer index drops until after Kf tombstones: that deferred list must be
eliminated because it cannot satisfy the contract.

Immediately before the existing `SlotcaskBulkDeleteOpts` declaration in
`src/db/slotcask.h`, add these complete callback declarations and replace both
delete option structs with the following blocks. The single-record callbacks
use `pre_commit_ctx`; batch callbacks use `bulk_hook_ctx` and mirror the
upsert window contract exactly.

~~~c
typedef int (*slotcask_delete_prepare_fn)(const SlotcaskOldRecord *old,
                                          uint32_t kf_slot, void *ctx);
typedef int (*slotcask_delete_apply_fn)(const SlotcaskOldRecord *old,
                                        uint32_t kf_slot, void *ctx);
typedef void (*slotcask_delete_abort_fn)(void *ctx);

typedef struct {
    slotcask_check_fn          check;
    void                      *check_ctx;
    int (*pre_commit)(const SlotcaskOldRecord *old, void *ctx);
    void                      *pre_commit_ctx;
    int                        skip_old_read;
    int                       *out_kf_shard;
    uint32_t                  *out_kf_slot;
    slotcask_delete_prepare_fn prepare_commit;
    slotcask_delete_apply_fn   apply_commit;
    slotcask_delete_abort_fn   abort_commit;
    int                        has_indexed_fields;
    int                       *out_durability_degraded;
} SlotcaskDeleteOpts;

typedef int (*slotcask_bulk_del_prepare_window_fn)(
    SlotcaskBulkRec *recs, const size_t *active, size_t nactive, void *ctx);
typedef int (*slotcask_bulk_del_apply_window_fn)(
    SlotcaskBulkRec *recs, const size_t *active, size_t nactive, void *ctx);
typedef void (*slotcask_bulk_del_abort_window_fn)(void *ctx);

typedef struct {
    slotcask_bulk_del_pre_commit_fn       pre_commit;
    int                                    pre_commit_needs_old;
    slotcask_bulk_del_prepare_window_fn   prepare_window;
    slotcask_bulk_del_apply_window_fn     apply_window;
    slotcask_bulk_del_abort_window_fn     abort_window;
    void                                  *bulk_hook_ctx;
    int                                    has_indexed_fields;
    int                                   *out_durability_degraded;
} SlotcaskBulkDeleteOpts;
~~~

**`out_durability_degraded` contract.** Initialize it to zero before each
primitive starts. Set it to one only when the mutation has committed—Kf and
every index have converged—and the sole remaining failure is marker/sidecar
cleanup after that committed state. Do not set it for an `apply_window` failure
whose abort sidecar was fsynced: that record is rejected and the caller returns
the original index-apply error. If abort recovery itself cannot complete,
retain the marker and sidecar, invoke `kf_marker_fail_closed`, return the
original apply error, and leave `out_durability_degraded` zero; the Kf still
exposes the old/absent state and subsequent writers are blocked by the recovery
gate. The same rule applies to single, bulk-upsert, single-delete,
key-list-delete, and criteria-delete adapters.

## Task 3 — observability, documentation, verification

At ~static void kf_marker_fail_closed(~ and each sidecar helper/resolver, log
object, shard, batch id, path, operation, state, and errno. Distinguish corrupt
evidence from an I/O failure.

Update src/db/slotcask.h immediately after the existing comment preceding
~typedef struct SlotcaskBulkOpts {~ to document prepare/apply/abort behavior.
Update docs/concepts/storage-model.md immediately after ~## Crash safety~ to
document forward replay before durable abort evidence and abort recovery after.

For each test capture base failure, failure after temporarily removing its fix,
and pass after restoration. Run exactly:

~~~bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2
BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1
~~~

Record real output, do not run benchmarks, and leave the implementation
uncommitted for blind raw-diff review.

## Execution rules

- Branch from main only after explicit human approval.
- Execute Tasks 1–3 in order.
- If a quoted anchor is absent, or non-unique and not documented in this plan
  as intentionally non-unique (the two ~SlotcaskUpsertOpts opts = {~
  initializers), write `PLAN_NOTES.md` describing the mismatch and halt the
  entire execution run immediately — do not guess, reinterpret, or continue
  to any further task, even an unrelated one. Resuming requires the human (or
  the planning model, re-engaged) to read `PLAN_NOTES.md`, decide whether this
  is a stale-anchor or wrong-assumption problem, and hand back a patched or
  fresh plan. Execution never resumes on its own.
- A newly found indexed mutation caller requires a revised plan; it is never
  silently exempted.
- For every regression test, capture the base-branch failure, temporarily
  remove only the fix and capture the expected failure again, restore it, and
  capture the passing output. Do not weaken, skip, or rerun-until-green.
