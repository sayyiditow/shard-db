/* slotcask.h — Bitcask-style storage engine with snake-game free-slot reuse.
 *
 * Replaces probe-into-slot Zone A / Zone B layout. Per object:
 *   keyfile_NNN.kf — mmap'd hash table, 24B slots, atomic 8B repoint, MAP_SHARED
 *   stream_NNN/data_NNNNNN.dat — append-only fixed-slot segments, rotated at 128 MB
 *
 * Design lock + prototype validation: see memory `engine_design_locked_2026_05_07`
 * and `proto_bitcask_v2.c`. This file ports that prototype with two production
 * adaptations:
 *   1. Keyfile shards live in a global `kfcache` (path-keyed, per-entry rwlock,
 *      LRU). Modeled on `bt_cache` in btree.c.
 *   2. Data segments live in a global `segcache` (same model). Active-segment
 *      append still goes through the per-stream rotation lock; the cache only
 *      provides the mmap pointer + fd, not the rotation primitive.
 *
 * Public API mirrors the prototype: open / close / insert / update / get /
 * delete / bulk_update. Phase-1 standalone; Phase-2 wires this into
 * cmd_insert / cmd_get / cmd_delete in storage.c.
 */
#ifndef SLOTCASK_H
#define SLOTCASK_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <pthread.h>
/* PATH_MAX: portable across Linux + macOS via <limits.h> + <sys/param.h>. */
#include <limits.h>
#include <sys/param.h>

typedef struct SlotRef SlotRef;   /* full definition in types.h; only the
                                      tag is needed here so slotcask.h can
                                      declare SlotRef-typed fields before
                                      types.h is necessarily included
                                      (slotcask.c includes this header
                                      before types.h) */

/* ============================================================ Tunables */

/* Segment file rotation point. Chosen to keep individual files small enough for
   `cp` / `du` / `tar` to stream comfortably while leaving plenty of records per
   segment (with slot_size=760, ~177K records per 128 MB segment). */
#define SLOTCASK_SEG_MAX_BYTES   (128ull * 1024 * 1024)

/* Streams per object — hardcoded by nproc per locked design.
   nproc <= 8  -> nproc;  nproc <= 16 -> 8;  else 16.
   On-disk (stream_id is in the keyfile entry) so changing this at runtime
   would orphan existing data. */
#define SLOTCASK_MAX_STREAMS     16

/* Keyfile shard count cap mirrors splits ceiling for the engine. */
#define SLOTCASK_MAX_SHARDS      4096

/* No per-shard slot ceiling. kf shards double via auto-resplit indefinitely;
   shard-stats surfaces per-shard load to the operator, who reshards via
   vacuum --splits=N if a single shard becomes operationally unwieldy. */

/* On-disk kf header (24 bytes, prefixes every kf file). `total` counts
   non-empty slots (live + tombstoned) — the resplit trigger metric, since
   tombstones still create lookup probe-chain pressure. `deleted` counts
   tombstones; live = total - deleted, computed when callers need it.
   Updates happen under the kf shard's wrlock (acquired via kfcache_acquire(
   writer=1)) — no atomics required. */
#define SLOTCASK_KF_MAGIC      0x31464B53u  /* 'SKF1' little-endian */
#define SLOTCASK_KF_VERSION    1u
#define SLOTCASK_KF_HDR_SIZE   24

typedef struct __attribute__((packed)) {
    uint32_t magic;          /* SLOTCASK_KF_MAGIC */
    uint32_t version;        /* SLOTCASK_KF_VERSION */
    uint64_t total;          /* non-empty slots (live + tombstoned) */
    uint64_t deleted;        /* tombstoned slots */
} SlotcaskKfHeader;

/* Per-shard slot count is chosen by tier, parameterised on `splits`:
     splits ≤ 16     → 1M  slots/shard   (24-48 MB total kf — small DBs)
     splits ≤ 128    → 256K slots/shard  (96 MB - 384 MB total — medium)
     splits ≤ 1024   → 128K slots/shard  (768 MB - 3 GB total — large)
     splits ≤ 4096   → 64K  slots/shard  (3 GB - 6 GB total — very large)
   Each tier targets ~50 % load at the documented 78K-200K rec/shard
   sweet spot. Per-shard auto-resplit at 80 % load doubles slots in place
   (no global rehash) up to SLOTCASK_MAX_SLOTS_PER_SHARD = 16M. The
   floor for all tiers is 64K so even max-splits objects have headroom.
   Tuning history: started at 2M flat, dropped to 512K, then 128K flat,
   now per-tier so the kf stays bounded at all splits.
   Public via slotcask_default_slots_for_splits(). */
#define SLOTCASK_MAX_SLOTS_PER_SHARD      (16u * 1024 * 1024)
size_t slotcask_default_slots_for_splits(int splits);

/* Keyfile entry header (24 B, packed). hash16 + 1B flag + 1B stream_id +
   2B file_id + 4B offset. The trailing 8 bytes (flag/stream/file/offset) are
   updated atomically as a single uint64 store on repoint; see kf_repoint. */
typedef struct __attribute__((packed)) {
    uint8_t  hash[16];
    uint8_t  flag;       /* 0=empty, 1=live, 2=tombstone */
    uint8_t  stream_id;
    uint16_t file_id;
    uint32_t offset;
} SlotcaskKfEntry;

/* On-disk record header (24 B, prefix of every slot): 16B hash + 2B klen +
   1B flag + 1B reserved + 4B vlen. flag at offset 18 mirrors the keyfile flag
   so a pwrite of one byte tombstones a slot. Layout matches the prototype so
   recovery code can scan segments byte-for-byte. */

/* ============================================================ kfcache */

/* Path-keyed cache of mmap'd keyfile shards. Entry holds (fd, mmap, size,
   slots_capacity, rwlock). Capacity from slotcask_init(). LRU eviction once
   half-full; rwlock-handoff to caller on get; release() drops the rwlock. */
typedef struct {
    int     slot;            /* cache slot index, or -1 if uncached fallback */
    int     writer;          /* 1 = held wrlock, 0 = held rdlock */
    int     fd;
    SlotcaskKfHeader *hdr;   /* points at byte 0 of the mmap (24-byte header) */
    SlotcaskKfEntry  *map;   /* points at byte 24 of the mmap (slot array) */
    size_t  map_size;        /* total mmap bytes (header + slots) */
    size_t  capacity;        /* slots in this shard = (map_size - 24) / 24 */
} SlotcaskKfHandle;

void kfcache_init(int cap);
void kfcache_shutdown(void);
/* Acquire (open or hit) a keyfile shard. writer=1 takes wrlock + creates if
   absent (sized to slots_capacity * 24B). writer=0 takes rdlock; fails if
   absent. Returns 0 on success, -1 on error. */
int  kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                     size_t slots_capacity, int writer);
void kfcache_release(SlotcaskKfHandle *h);
int  kfcache_sync_slots_locked(SlotcaskKfHandle *h,
                               const size_t *slots, size_t nslots,
                               int header_changed);

/* Fast-path acquire for read-only callers that hold a SlotRef.
   On gen match: takes rdlock and returns 0 without touching the table mutex.
   On gen mismatch (eviction since last open): falls through to kfcache_acquire,
   then updates *ref with the new (slot, gen). Always passes writer=0.
   db and kf_shard_id are used only on the slow path to refresh *ref.
   Note: void *db avoids a C scoping issue — struct SlotcaskDb is not yet
   visible at this point in the header. The definition in .c casts to the
   full type. */
int  kfcache_acquire_direct(SlotcaskKfHandle *h, SlotRef *ref,
                             const char *path, size_t slots_capacity,
                             void *db, int kf_shard_id);

/* Build the canonical kf shard path under a slotcask data_dir. Public
   wrapper around the internal kf_path_for so query.c (bitmap index path)
   can construct kf paths without duplicating the layout convention. */
void slotcask_kf_path(char *out, size_t outlen,
                      const char *data_dir, int shard_id);

/* ============================================================ segcache */

typedef struct {
    int     slot;
    int     writer;
    int     fd;
    uint8_t *map;            /* mmap'd region, full SLOTCASK_SEG_MAX_BYTES */
    size_t  map_size;        /* always SLOTCASK_SEG_MAX_BYTES on success */
} SlotcaskSegHandle;

void segcache_init(int cap);
void segcache_shutdown(void);
/* writer=1: create+mmap MAP_SHARED. writer=0: open+mmap, fail if absent. */
/* `create` controls O_CREAT + ftruncate-to-max in seg_open_file (set on
   write paths so the first write to a freshly-rotated segment file
   materialises it). `writer` controls the entry rwlock mode: 1 = wrlock
   (exclusive — used by callers that mutate cache state, e.g. recovery).
   Routine record writes use create=1, writer=0: rdlock is sufficient
   because each caller owns a unique reserved offset, and rdlock still
   serialises against eviction (which takes wrlock). */
int  segcache_acquire(SlotcaskSegHandle *h, const char *path,
                      int create, int writer, int must_cache);
void segcache_release(SlotcaskSegHandle *h);

/* Fast-path acquire for read-only callers that hold a SlotRef.
   On gen match: takes rdlock and returns 0 without touching g_segcache_lock.
   On gen mismatch: falls through to segcache_acquire and updates *ref.
   create must be 0 (read paths only). writer must be 0. */
int  segcache_acquire_direct(SlotcaskSegHandle *h, SlotRef *ref,
                              const char *path);

/* Deterministic TEST_BUILD-only pause hook for the single-partial-update
   regression seam. Installed by the daemon's test-control thread on an
   INSTALL message; fires at most once per install (the invocation takes and
   clears the stored pair atomically). `under_kf_wrlock` distinguishes the
   pre-fix stale-snapshot site (0) from the fixed under-lock callback site
   (1) so the test never relies on timing. Never present in production
   builds; no production code calls it. */
#ifdef TEST_BUILD
typedef void (*slotcask_test_after_old_fn)(int under_kf_wrlock, void *ctx);
void slotcask_test_set_after_old_hook(slotcask_test_after_old_fn fn, void *ctx);
void slotcask_test_after_old(int under_kf_wrlock);
#endif

#ifdef TEST_BUILD
void segcache_test_force_identity_mismatches(int count);
int  segcache_test_identity_mismatches_remaining(void);
#endif

/* ============================================================ Per-stream pool */

/* Format constants — stored in <data_dir>/segment_format file.
   FIXED = original padded slots (slot_size bytes each).
   VARIABLE = no padding; record is exactly 24 + klen + vlen bytes. */
#define SLOTCASK_FORMAT_FIXED    0
#define SLOTCASK_FORMAT_VARIABLE 1

/* Number of size-class buckets in the per-stream free pool.
   Bucket 0: capacity < 256B
   Bucket 1: capacity < 1024B
   Bucket 2: capacity < 8192B
   Bucket 3: capacity <= max_slot_size (catch-all) */
#define SLOTCASK_POOL_BUCKETS 4

typedef struct {
    uint16_t file_id;
    uint32_t offset;
    uint32_t capacity; /* actual slot size in bytes (24 + klen + vlen) */
} SlotcaskFreeSlot;

typedef struct {
    int             stream_id;
    char            stream_dir[PATH_MAX];

    /* Append path */
    pthread_mutex_t rotation_lock;
    uint32_t        active_file_id;
    uint64_t        reserve_off;

    /* Free pool — try_lock pattern; only one consumer at a time.
       Bucketed by slot capacity for variable-length format:
       bucket 0 < 256B, 1 < 1024B, 2 < 8192B, 3 = catch-all.
       Fixed-format uses bucket 0 only (all slots same size). */
    pthread_mutex_t   pool_lock;
    SlotcaskFreeSlot *free_slots[SLOTCASK_POOL_BUCKETS];
    size_t            free_count[SLOTCASK_POOL_BUCKETS];
    size_t            free_cap[SLOTCASK_POOL_BUCKETS];
} SlotcaskStream;

/* ============================================================ DB handle */

/* Callback for trimming record values before writing (VARIABLE format only).
   Returns the number of bytes of val that should be stored; must be ≤ vlen.
   Set db->trim_fn = NULL to disable. */
typedef size_t (*SlotcaskTrimFn)(const void *val, size_t vlen, void *ctx);

typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
    int     num_shards;
    int     num_streams;
    int     slot_size;       /* max slot size; for varlen = 24 + max_key + max_value */
    int     format;          /* SLOTCASK_FORMAT_FIXED or SLOTCASK_FORMAT_VARIABLE */
    size_t  slots_per_shard; /* per-shard kf capacity floor; individual
                                shards may have grown larger via auto-resplit */

    SlotcaskStream *streams;

    /* Per-shard kf slot refs — populated at slotcask_open time, updated
       on gen mismatch. Array of num_shards entries; slot==-1 means not
       yet cached (safe initial value since the array is calloc'd). */
    SlotRef *kf_slot_refs;

    /* Per-stream segment slot refs. seg_slot_refs[stream_id] is an array
       of seg_slot_caps[stream_id] entries indexed by file_id.
       Allocated lazily (NULL until first segcache hit for that stream).
       seg_slot_caps[stream_id] is the allocated capacity of that array. */
    SlotRef **seg_slot_refs;
    int      *seg_slot_caps;

    /* Optional value trim callback. When non-NULL and format == SLOTCASK_FORMAT_VARIABLE,
       called in insert_with_hooks / upsert_with_hooks to shorten vlen before writing.
       trim_ctx is passed as the third argument. Not used by compact (which passes
       the trim function explicitly). Not thread-safe to change after first write. */
    SlotcaskTrimFn  trim_fn;
    void           *trim_ctx;
} SlotcaskDb;

/* Test-only: write a synthetic `total` (and matching `deleted`) into a kf
   shard's header. Used by resplit tests to trip the 75 % trigger without
   inserting millions of records. Must be called between slotcask_open and
   the next kf-touching op; takes the kf wrlock internally. Returns 0 on
   success. NOT for production use. */
int slotcask_test_set_kf_total(SlotcaskDb *db, int shard_id,
                               uint64_t total, uint64_t deleted);

/* Sum the per-shard kf headers into total/deleted. live = total - deleted.
   Reads the 24-byte header of each kf shard under the rdlock — at splits=4096
   that's 4096 × 24B = 96 KB read, sub-millisecond once mmap'd. Returns 0 on
   success, non-zero if any shard fails to acquire. The kf header is updated
   atomically by slotcask_put / slotcask_delete and is the single source of
   truth for record counts; callers should prefer this over a separate counts
   file (which can go stale on ungraceful shutdown). */
int slotcask_sum_kf_totals(SlotcaskDb *db,
                           uint64_t *out_total, uint64_t *out_deleted);

/* Initialize global caches. Call once at process startup, after db.env load. */
void slotcask_init(int kfcache_cap, int segcache_cap);
void slotcask_shutdown(void);

/* Compute the recommended stream count for this host. Public so callers can
   pass it to slotcask_open(); the engine's create-object path uses this. */
int  slotcask_streams_for_nproc(void);

/* Open (or create) an object's slotcask state. data_dir is the per-object root
   (e.g., $DB_ROOT/<dir>/<obj>). num_shards must be a power of 2 in
   [1, SLOTCASK_MAX_SHARDS]. slot_size is the fixed per-record byte width
   (header + max key + max value, rounded to 8). Performs crash recovery
   if `.dirty` marker is present. */
int  slotcask_open(SlotcaskDb *db, const char *data_dir,
                   int num_shards, int num_streams, int slot_size);
void slotcask_close(SlotcaskDb *db);

/* Direction-C seg compaction. For each stream, pair-merges sparse non-active
   seg files into denser ones — donor's live records are migrated into
   recipient's tombstone holes via kf_repoint_at_slot, then the donor file
   is unlinked. Active seg of each stream is never touched. Caller must hold
   objlock_wrlock for the object. *out_dropped (optional) receives the total
   number of seg files unlinked across all streams. Returns 0 on success. */
int  slotcask_compact_segs(SlotcaskDb *db, int *out_dropped);
/* Rebuild kf from segment scan.  Caller holds objlock_wrlock.
   Returns number of entries repaired, or -1 on fatal error. */
int  slotcask_rebuild_kf(SlotcaskDb *db);

/* Migrate an object's segment files from fixed-size to variable-length format.
   Daemon must be stopped. Uses atomic rename: writes to streams.new/ + kf.new/,
   renames atomically, writes segment_format file, cleans up old dirs.
   Returns 0 on success. */
int  slotcask_migrate_to_varlen(SlotcaskDb *db);

/* Repack a VARIABLE-format object in-place, applying trim_fn to shorten each
   value. Writes compacted records to a fresh file-ID range, repoints KF entries,
   then deletes the old segment files. Idempotent across two calls (alternates
   between MIGRATE_STREAM_BASE and COMPACT_STREAM_BASE ranges). Returns 0 on
   success, -1 on error (object state is consistent on any partial failure). */
int slotcask_compact(SlotcaskDb *db, SlotcaskTrimFn trim_fn, void *trim_ctx);

/* Returns the pool bucket index (0-3) for a slot of given capacity.
   max_slot_size is db->slot_size (the object's schema max). */
int  slotcask_bucket_for(uint32_t capacity, int max_slot_size);

/* Rebuild every kf shard in place, dropping flag=2 (tombstone) entries.
   After this, kf->total = live count and kf->deleted = 0 across all
   shards. Used by cmd_vacuum so the kf-derived "orphaned" counter drops
   to 0 (matching the pre-kf-derived behavior of the legacy text counts
   file). Holds each shard's wrlock for the rebuild duration. */
int  slotcask_compact_kf(SlotcaskDb *db);

/* ============================================================ Two-phase bulk fetch
 *
 * Resolved record location — output of phase 1 KF probe.
 * 24 bytes: 16B hash + 1B sid + 2B fid + 4B off + 1B padding.
 * Phase 2 (fetch) reads segment files at these locations. */
typedef struct __attribute__((packed)) {
    uint8_t  hash[16];
    uint8_t  sid;          /* stream id */
    uint16_t fid;          /* file id */
    uint32_t off;          /* byte offset in segment file */
} SlotcaskResolvedRec;

/* ============================================================ Public CRUD */

/* Insert a NEW key. Returns 0 on success, -2 if key already exists, -1 on error.
   stream_id_hint < 0 routes by hash; otherwise must be < num_streams. */
int slotcask_insert(SlotcaskDb *db, int stream_id_hint,
                    const void *key, size_t klen,
                    const void *value, size_t vlen);

/* Update an EXISTING key (snake-game: pool-slot if available else append, then
   tombstone old). Returns 0 on success, -1 if missing or error. */
int slotcask_update(SlotcaskDb *db, int stream_id_hint,
                    const void *key, size_t klen,
                    const void *value, size_t vlen);

/* Delete (tombstone) a key. Returns 0 on success, -1 if missing or error. */
int slotcask_delete(SlotcaskDb *db,
                    const void *key, size_t klen);

/* Read. *val_out is malloc'd on success; caller frees. Returns 0 on success,
   -1 if missing or error. */
int slotcask_get(SlotcaskDb *db,
                 const void *key, size_t klen,
                 void **val_out, size_t *vlen_out);

/* Bulk update with all-or-nothing per-stream routing. Records must already
   exist (no upsert); missing key fails the whole batch. Returns 0 on success,
   -1 on error. */
typedef struct {
    const void *key;
    size_t      klen;
    const void *value;
    size_t      vlen;
} SlotcaskRecord;

int slotcask_bulk_update(SlotcaskDb *db, const SlotcaskRecord *recs, size_t n);

/* Stripped-down lookup. Returns 1 if the key exists (not tombstoned), 0 if
   missing, -1 on I/O error. No value copy. */
int slotcask_exists(SlotcaskDb *db, const void *key, size_t klen);

/* ============================================================ CAS hooks
 *
 * The plain slotcask_insert / _update / _delete are straight-line — no CAS,
 * no index hooks. The hook variants below give callers two callbacks that
 * fire at well-defined points under the kf-shard wrlock:
 *
 *   check_fn      — fires AFTER lookup (so it sees the current old record,
 *                   if any) but BEFORE any data is written. Caller validates
 *                   if_not_exists, criteria, etc.
 *
 *   pre_commit_fn — fires AFTER the new record is written to a segment but
 *                   BEFORE the kf entry is repointed/inserted (commit point).
 *                   Caller updates secondary indexes here. Returning non-zero
 *                   aborts: the freshly-written slot is tombstoned and
 *                   pushed to the free pool, no kf change happens.
 *
 * The kf-shard wrlock is held across both callbacks for the upsert path
 * (and the lookup → kf-mutation window for delete). Reads to the same shard
 * block while a CAS path is running. Phase-1-prototype-style fine-grained
 * lock acquisition can be reintroduced in the perf phase.
 */

typedef struct {
    /* NULL when key doesn't exist (fresh insert). Otherwise points to the
       OLD on-disk value bytes — caller must copy if it wants to retain
       past pre_commit return. */
    const uint8_t *value;
    size_t         vlen;
} SlotcaskOldRecord;

/* Return 1 to proceed; 0 to abort with condition_not_met. NULL = always proceed. */
typedef int (*slotcask_check_fn)(const SlotcaskOldRecord *old, void *ctx);

/* Opt-in NEW-from-OLD constructor for the upsert path. When
   SlotcaskUpsertOpts.new_from_old is non-NULL, the slow upsert path builds
   the replacement record from the OLD bytes it reads while holding the
   kf-shard write lock, instead of trusting the caller's earlier snapshot.
   The callback runs after the built-in if_not_exists / require_existing /
   check gates have accepted the current OLD, and before segment
   reservation, segment writing, or pre_commit. The caller's earlier
   'value'/'vlen' are ignored in this mode. Output contract: '*out_vlen'
   must be 0 unless the callback sets it, and '*out_vlen <= out_capacity'
   is mandatory (the upsert path re-checks the final record-header/key/
   value size before reservation). Return 0 to commit the produced bytes;
   any non-zero return aborts the update without reserving a segment or
   tombstoning anything. The callback must not perform network I/O or
   re-enter the database. */
typedef int (*slotcask_new_from_old_fn)(const SlotcaskOldRecord *old,
                                         uint8_t *out_value,
                                         size_t out_capacity,
                                         size_t *out_vlen,
                                         void *ctx);

/* Return 0 to commit; non-zero to abort. NULL = always commit. */
typedef int (*slotcask_pre_commit_fn)(const SlotcaskOldRecord *old,
                                       const uint8_t *new_value, size_t new_vlen,
                                       int is_update, void *ctx);

/* Two-phase hooks for indexed new-key inserts only (single-record and bulk
 * windowed paths).
 *
 * prepare_commit — fires AFTER the segment write, BEFORE the commit-intent
 *   marker exists. Must perform every check that can legitimately reject
 *   the write (e.g. bitmap distinct-value cap) and MUST NOT durably mutate
 *   index state. planned_kf_slot is the physical slot this record will
 *   commit to (see kf_plan_insert_slot). Returning non-zero rejects: no
 *   marker is ever written, the speculative segment slot is tombstoned,
 *   caller gets an ordinary error — never routed through fail-closed.
 *
 * apply_commit — fires AFTER the marker is durable, BEFORE kf is committed.
 *   Performs the actual index mutation. Returning non-zero here is always a
 *   genuine failure (I/O/OOM), never a policy rejection — per the
 *   commit-intent rule this is never rolled back; replayed (idempotent) or
 *   fails closed exactly like every other post-marker-fsync failure today.
 *
 * Return 0 to proceed, non-zero to reject/fail. These hooks are required
 * for an indexed fresh insert that also supplies pre_commit; slotcask must
 * reject a partial/missing pair with EINVAL rather than silently falling
 * back to the unsafe single-phase ordering. */
typedef int (*slotcask_prepare_commit_fn)(const uint8_t *new_value, size_t new_vlen,
                                           uint32_t planned_kf_slot, void *ctx);
typedef int (*slotcask_apply_commit_fn)(const uint8_t *new_value, size_t new_vlen,
                                         uint32_t planned_kf_slot, void *ctx);
/* Releases whatever prepare_commit staged (e.g. retained bitmap writer
 * handles), without applying it. Only reachable if prepare_commit
 * succeeded but something else (currently: the marker write itself)
 * failed before apply_commit could run — a narrow, rare I/O-failure
 * window. Optional: NULL is fine when prepare_commit stages nothing
 * that needs releasing. */
typedef void (*slotcask_abort_commit_fn)(void *ctx);

typedef struct {
    int                       if_not_exists;     /* fail if key exists (insert path) */
    int                       require_existing;  /* fail if key missing (update path) */
    /* Set to 1 if `check` may inspect OLD even on the INSERT path (i.e. it
       returns 0 when old is NULL — typical CAS-on-insert semantics). When
       0 (default), the fast path calls check(NULL) for new keys; check
       only sees OLD on the upgrade-to-update branch. When 1, the slow
       path is forced so OLD is loaded before check is called for both
       insert and update branches. */
    int                       check_needs_old;
    slotcask_check_fn         check;
    void                     *check_ctx;
    /* Opt-in NEW-from-OLD constructor (see slotcask_new_from_old_fn). When
       set, the slow upsert path is forced and the supplied 'value'/'vlen'
       are ignored — the callback produces the replacement from the OLD the
       upsert reads under the kf-shard wrlock. NULL = existing behavior. */
    slotcask_new_from_old_fn  new_from_old;
    void                     *new_from_old_ctx;
    slotcask_pre_commit_fn    pre_commit;
    void                     *pre_commit_ctx;
    /* Two-phase hooks — required together for a fresh indexed insert that
       also needs pre_commit-style index application (has_indexed_fields=1
       and pre_commit != NULL); slotcask rejects a partial/missing pair with
       EINVAL rather than silently falling back to the unsafe single-phase
       ordering. See slotcask_prepare_commit_fn / slotcask_apply_commit_fn
       above. Both share pre_commit_ctx. */
    slotcask_prepare_commit_fn prepare_commit;
    slotcask_apply_commit_fn   apply_commit;
    slotcask_abort_commit_fn   abort_commit;
    /* Optional out-params: when non-NULL, slotcask writes the target kf
       shard index + kf slot index here BEFORE invoking pre_commit. The
       pre_commit ctx can read them via its own pointer to the same
       storage. Used by bitmap-index updates which key by (shard, slot)
       rather than by hash. Existing callers leave these NULL and the
       fields are ignored — no behaviour change. */
    int                      *out_kf_shard;
    uint32_t                 *out_kf_slot;
    /* Set to 1 when the object has at least one indexed field. Gates the
       marker-write / index-sync / marker-clear sequence. Zero-index objects
       skip the entire marker path — segment-write + kf-repoint is already
       crash-safe without a third structure to reconcile. */
    int                       has_indexed_fields;
    /* When non-NULL, set to 1 after the kf + index mutation if the
       marker could not be deleted (kf and indexes have converged; the
       object is safe to read but a retry or restart must clean up the
       orphaned marker). The caller must propagate this to the wire
       response as "durability_degraded". */
    int                      *out_durability_degraded;
} SlotcaskUpsertOpts;

/* ============================================================ Bulk upsert
 *
 * Per-record kfcache_acquire/release pairs were the dominant cost in
 * bulk-insert-via-slotcask_upsert_with_hooks (the per-record kf-wrlock
 * acquisitions add up). The bulk primitive amortises that lock by
 * accepting a batch where every record hashes to the SAME kf-shard,
 * acquiring the wrlock once for the whole batch. Caller pre-buckets
 * records by kf_shard_id (the engine's bulk_insert_shard_worker_v2
 * already does so via compute_record_shard).
 */
typedef struct {
    /* input: caller fills */
    const void *key;
    size_t      klen;
    const void *value;
    size_t      vlen;
    void       *user_ctx;              /* passed to pre_commit per record */
    /* Per-record CAS: when set, this record is strict-insert regardless
       of the per-batch opts.if_not_exists. Used by auto-key bulk-insert
       to mark omit-key records (generated UUIDv4 / seq.next) as
       strict-insert while provided-key records in the same batch
       remain upsert. The primitive OR-combines this with opts->if_not_exists.
       Zero-init = today's behaviour (per-batch opts only). */
    int         if_not_exists;
    /* Optional: caller-provided OLD value. If old_value != NULL the bulk
       primitive uses it for pre_commit and skips its own segcache read
       — useful when the caller already has OLD in hand (bulk-update
       computes new_value from old_value, so it must read OLD anyway).
       Lifetime: caller owns the buffer; must outlive the bulk call. */
    const void *old_value;
    size_t      old_vlen;
    /* output: callee fills */
    int         status;                /* 0=ok, -2=cond_not_met, -1=error */
    int         was_update;
    uint32_t    slot_capacity;         /* on-disk capacity for this slot (varlen) */
    /* Physical location of the kf entry (insert: slot kf_put_new chose;
       update: slot kf_lookup found). Written BEFORE pre_commit fires so
       per-record bitmap updates can address the slot. */
    int         kf_shard;
    uint32_t    kf_slot;
} SlotcaskBulkRec;

typedef int (*slotcask_bulk_pre_commit_fn)(const SlotcaskOldRecord *old,
                                            SlotcaskBulkRec *rec,
                                            int is_update);

/* Compute the NEW record value from the OLD record per-record. Fires
   AFTER the batched OLD reads (Phase 1b) and BEFORE the seg writes
   (Phase 3). Used by bulk-update workers that derive each record's new
   value from its existing one (memcpy old → patch fields → return).
   Must populate rec->value and rec->vlen with the new bytes — the
   pointer must outlive the bulk call (typically a worker-allocated
   scratch slab keyed off rec->user_ctx).
   Return 0 to write, non-zero to skip (e.g. CAS rejection); skipped
   records get rec.status=-2 and the seg slot is never written. */
typedef int (*slotcask_bulk_value_fn)(const SlotcaskOldRecord *old,
                                       SlotcaskBulkRec *rec);

/* Two-phase, window-scoped hooks for indexed bulk-insert windows
 * (BULK_COMMIT_MAX_RECORDS records per commit window).
 *
 * prepare_window — fires once per window, on the bulk worker thread,
 *   BEFORE the window's batch marker exists. active[] lists indices into
 *   recs[] that still have a valid segment write and planned kf target.
 *   May reject an individual record for a legitimate policy condition
 *   (e.g. bitmap cap) by setting recs[active[i]].status = -1; must not
 *   durably mutate index state for any record, accepted or rejected.
 *   Returns 0 (window may proceed with whatever active[] holds after
 *   rejections) or non-zero for a hard staging failure (aborts the whole
 *   window, no marker written).
 *
 * apply_window — fires once per window, AFTER the batch marker is durable,
 *   BEFORE kf is committed for the window's surviving records. Performs
 *   the actual index mutations for every record in active[]. A non-zero
 *   return is always a genuine failure (I/O/OOM), never a policy
 *   rejection. The primitive durably writes an abort sidecar, applies the
 *   inverse index diff, tombstones speculative NEW segments, rejects the
 *   window, and returns the original error; it never publishes Kf for that
 *   failed window or reports durability_degraded.
 *
 * abort_window — fires instead of apply_window when the window's batch
 *   marker never became durable (marker alloc/open/fsync failed, or every
 *   staged record was individually rejected before or during marker
 *   write). Releases whatever prepare_window staged (open bitmap writer
 *   handles, tracked buffers, queued index ops) without performing any
 *   index mutation — apply_window will never be called for this window.
 *   Optional; NULL is fine for hooks that stage nothing durable-adjacent
 *   in prepare_window. */
typedef int (*slotcask_bulk_prepare_window_fn)(SlotcaskBulkRec *recs,
                                                const size_t *active,
                                                size_t nactive, void *ctx);
typedef int (*slotcask_bulk_apply_window_fn)(SlotcaskBulkRec *recs,
                                              const size_t *active,
                                              size_t nactive, void *ctx);
typedef void (*slotcask_bulk_abort_window_fn)(void *ctx);

typedef struct {
    int                          if_not_exists;     /* skip if key exists */
    int                          require_existing;  /* skip if key missing (bulk-update use) */
    slotcask_bulk_pre_commit_fn  pre_commit;        /* fired per record under kf wrlock; NULL = no-op */
    /* Set to 1 iff pre_commit will dereference the SlotcaskOldRecord *old
       arg. When 0 (e.g. non-indexed bulk-insert), the primitive skips the
       per-record read_record_value on UPDATE — that's the dominant per-
       record cost on update-heavy workloads. Hook still fires with
       old=NULL. Forced to 1 internally when value_compute is set. */
    int                          pre_commit_needs_old;
    /* Optional NEW-from-OLD compute hook — see slotcask_bulk_value_fn. */
    slotcask_bulk_value_fn       value_compute;
    /* Two-phase window hooks — required together for a fresh indexed bulk
       insert window (has_indexed_fields=1 and pre_commit != NULL); see
       slotcask_bulk_prepare_window_fn / slotcask_bulk_apply_window_fn
       above. Both share bulk_hook_ctx (not pre_commit's per-record ctx —
       the window hooks operate on the whole window at once). */
    slotcask_bulk_prepare_window_fn prepare_window;
    slotcask_bulk_apply_window_fn   apply_window;
    slotcask_bulk_abort_window_fn   abort_window;
    void                            *bulk_hook_ctx;
    /* Gate: when 0, skip marker path entirely. Set from load_index_fields()
       same as the single-record path. */
    int                          has_indexed_fields;
    /* When non-NULL, set to 1 if kf+index converged but marker clear failed
       (safe degraded state). Caller propagates to wire response. */
    int                         *out_durability_degraded;
} SlotcaskBulkOpts;

/* Returns 0 if the batch ran (per-record results in recs[].status), -1 on
   hard error (kf_acquire failed). Records that hit if_not_exists,
   require_existing, or pre_commit returning non-zero get rec.status=-2
   with rec.was_update set from the existing key. */
int slotcask_bulk_upsert_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkOpts *opts);

/* ============================================================ Bulk delete
 *
 * Same shape as bulk_upsert_in_kfshard but for deletes — no NEW value, no
 * seg writes, just indexed forward-diff + kf tombstones + batched seg
 * flag-flips.
 * 4 phases under one held kf wrlock:
 *   1a. kf_lookup per record. Records not found get status=-2.
 *   1b. Batched OLD reads (sorted by old_sid/old_fid) iff
 *       pre_commit_needs_old=1; otherwise skipped entirely.
 *   2.  Indexed windows: marker -> forward index diff -> kf tombstone;
 *       non-indexed callers: per-record pre_commit hook + kf_tombstone.
 *   3.  Post-kf-release: batched seg flag-flips, sorted by (old_sid,
 *       old_fid) — one segcache rdlock per unique seg file.
 *
 * Crash safety: indexed windows write a delete marker before the forward
 * index diff. An apply failure writes a durable abort sidecar and applies
 * the inverse, so OLD remains visible. A crash after the Kf tombstone but
 * before segment cleanup is recovered from the marker; non-indexed deletes
 * retain the simpler kf-tombstone commit point.
 */
typedef int (*slotcask_bulk_del_pre_commit_fn)(const SlotcaskOldRecord *old,
                                                SlotcaskBulkRec *rec);

/* Two-phase, window-scoped hooks for indexed bulk deletes, mirroring the
 * bulk-upsert window contract exactly. prepare_window — fires once per
 * window BEFORE the batch delete marker is durable; performs the forward
 * index diffs (old=OLD, new=NULL) for every active record but must NOT
 * tombstone kf slots (the primitive does that synchronously after a
 * successful apply). On apply_window failure the primitive writes the
 * batch abort sidecar, performs every inverse (old=NULL, new=OLD) while
 * the kf wrlock is held, rejects the records, and returns the original
 * apply error only after the sidecar was fsynced. abort_window frees
 * staging without writing. */
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

int slotcask_bulk_delete_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkDeleteOpts *opts);

/* ============================================================ Bulk lookup
 *
 * For multi-exists / multi-get on slotcask. Same lock-amortisation
 * pattern as bulk_upsert / bulk_delete: one kf rdlock per call (not per
 * record), batched verify_stored_key sorted by (sid, fid) so the
 * segcache rdlock is held once per unique seg file. Caller pre-buckets
 * records so all hash to `kf_shard_id` (use compute_record_shard).
 *
 * bulk_lookup: rec.status = 0 (found+verified), -2 (not found / hash
 * collision miss), -1 (hard error). Reads no value bytes. */
int slotcask_bulk_lookup_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                      SlotcaskBulkRec *recs, size_t n);

/* For shard-id mapping use compute_record_shard(hash, splits) from
   types.h — the single version-aware helper that slotcask itself
   delegates to. */

typedef struct {
    int     was_update;             /* 1 if existing key was updated, 0 if created */
    int     condition_not_met;      /* 1 if check_fn or if_not_exists/require_existing rejected */
    /* On condition_not_met=1, malloc'd copy of the OLD record's value (NULL if
       no old record). Caller frees. */
    uint8_t *current_value;
    size_t   current_vlen;
} SlotcaskUpsertResult;

/* Returns 0 on success (record written), -1 on hard error, -2 on
   condition_not_met. result->was_update / current_value are populated
   regardless of return code (current_value present only on -2 with old). */
int slotcask_upsert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result);

/* INSERT-only fast path with hooks. Skips the kf_lookup-with-verify pass
   that slotcask_upsert_with_hooks pays before deciding insert-vs-update —
   for INSERT semantics the lookup is wasted (we always insert, never
   update). Detection of an existing key happens implicitly via kf_put_new's
   probe: returns -2 with result->was_update=1 if the key already exists.

   Order: seg write → kf_put_new → pre_commit. Running pre_commit AFTER
   kf commit means a duplicate-key rejection (kf_put_new returns 1) bails
   cleanly without leaving stale index entries. The trade-off: if pre_commit
   fails AFTER kf success, the kf entry is committed but the indexes are
   incomplete — caller responsibility, matches existing behavior on rare
   pre_commit failures.

   Use only when the caller intends INSERT (not upsert) — typically when
   if_not_exists=true is set on cmd_insert/cmd_bulk_insert. opts.check and
   opts.if_not_exists/require_existing are honored. opts.pre_commit_needs_old
   is ignored (no OLD record on INSERT). */
int slotcask_insert_with_hooks(SlotcaskDb *db, int stream_id_hint,
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                const SlotcaskUpsertOpts *opts,
                                SlotcaskUpsertResult *result);

/* Two-phase, single-record hooks for indexed deletes, mirroring the upsert
   window contract. prepare_commit fires after the delete marker is durable,
   before the kf tombstone, and performs the forward index diff
   (old=OLD, new=NULL); it must not tombstone the kf slot — the primitive
   does that synchronously after a successful apply. On apply failure the
   primitive writes the abort sidecar, performs the inverse (old=NULL,
   new=OLD) while the kf wrlock is held, rejects the record, and returns the
   original apply error only after the sidecar was fsynced. abort_commit
   frees staged resources without writing (optional). Both share
   pre_commit_ctx. */
typedef int (*slotcask_delete_prepare_fn)(const SlotcaskOldRecord *old,
                                          uint32_t kf_slot, void *ctx);
typedef int (*slotcask_delete_apply_fn)(const SlotcaskOldRecord *old,
                                        uint32_t kf_slot, void *ctx);
typedef void (*slotcask_delete_abort_fn)(void *ctx);

typedef struct {
    slotcask_check_fn   check;
    void               *check_ctx;
    /* Fires with the old record under the kf wrlock, after kf_lookup but
       before kf flag=2. Caller removes index entries. Return 0 to commit
       the deletion; non-zero to abort (kf untouched, no tombstone). */
    int (*pre_commit)(const SlotcaskOldRecord *old, void *ctx);
    void               *pre_commit_ctx;
    /* Set to 1 to opt OUT of the per-record read_record_value when
       neither `check` nor `pre_commit` will dereference the OLD record
       (e.g. non-indexed delete with no CAS). Default 0 = preserve the
       original behavior (always read OLD when a hook is set). pre_commit
       still fires; old is passed as NULL when this flag is on. */
    int                 skip_old_read;
    /* Optional out-params: when non-NULL, slotcask writes the kf shard
       index + the slot of the record being deleted here BEFORE invoking
       pre_commit. Used by bitmap-index updates which key by (shard,
       slot). Existing callers leave these NULL — no behaviour change. */
    int                *out_kf_shard;
    uint32_t           *out_kf_slot;
    /* Two-phase hooks for indexed deletes, mirroring the upsert window
       contract. prepare_commit — fires after OLD lookup but BEFORE the
       delete marker is durable, and may reject/stage policy checks without
       mutating indexes. apply_commit fires after the marker is durable and
       performs the forward index diff (old=OLD, new=NULL). It must not
       tombstone the kf slot; the primitive does that synchronously after a
       successful apply. On apply failure the primitive writes the abort
       sidecar, performs the inverse (old=NULL, new=OLD) while holding the
       kf wrlock, rejects the record, and only then returns the original
       apply error. abort_commit mirrors slotcask_abort_commit_fn (optional).
       When has_indexed_fields is set, apply_commit is mandatory and there is
       no legacy single-phase path. */
    slotcask_delete_prepare_fn prepare_commit;
    slotcask_delete_apply_fn   apply_commit;
    slotcask_delete_abort_fn   abort_commit;
    int                        has_indexed_fields;
    int                       *out_durability_degraded;
} SlotcaskDeleteOpts;

typedef struct {
    int      not_found;
    int      condition_not_met;
    uint8_t *current_value;
    size_t   current_vlen;
} SlotcaskDeleteResult;

int slotcask_delete_with_hooks(SlotcaskDb *db,
                                const void *key, size_t klen,
                                const SlotcaskDeleteOpts *opts,
                                SlotcaskDeleteResult *result);

/* ============================================================ Registry
 *
 * Process-wide cache of per-object SlotcaskDb handles. Lazy-opened on first
 * slotcask_registry_get() call; remains alive until shutdown or explicit
 * invalidate (drop-object, schema mutation that changes splits/streams/
 * slot_size). Footprint per entry is small (per-stream rotation lock + free
 * pool — keyfile/segment mmaps live in kfcache/segcache, not here).
 *
 * Keyed by (effective_root, object) where effective_root = "$DB_ROOT/<dir>".
 * That matches how the engine's cmd_* functions receive db-root context
 * (server.c builds the effective root once at dispatch time).
 *
 * Returns NULL on open failure (logged via fprintf to stderr).
 *
 * The pointer is BORROWED — never call slotcask_close on it. The registry
 * owns lifetime.
 */
typedef struct {
    int splits;            /* num_shards for the keyfile */
    int slot_size;         /* max per-record byte width (slot_size for fixed, max for variable) */
    int streams;           /* persisted at create time, hardcoded by nproc */
    int format;            /* SLOTCASK_FORMAT_FIXED or SLOTCASK_FORMAT_VARIABLE */
} SlotcaskSchemaInfo;

SlotcaskDb *slotcask_registry_get(const char *effective_root,
                                  const char *object,
                                  const SlotcaskSchemaInfo *info);

void slotcask_registry_invalidate(const char *effective_root,
                                  const char *object);

void slotcask_registry_shutdown(void);

/* ============================================================ Query primitives
 *
 * Below APIs feed the query layer (find/count/aggregate/keys/fetch). Both
 * exist because the engine needs to:
 *   1. Walk every live record across all keyfile shards (full-table scan).
 *   2. Look up records by hash16 alone (index-driven access — the btree
 *      stores hashes, not keys).
 *
 * Both callbacks receive raw payload pointers backed by the segment mmap. The
 * pointers are valid only for the duration of the cb invocation — the
 * underlying segcache rdlock is dropped on return. Callers that need the
 * data past the cb (collecting keys for later emit) must copy it.
 */

/* cb returns 0 to continue, 1 to stop the walk early. */
typedef int (*SlotcaskScanCb)(const uint8_t hash16[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
                               void *ctx);

/* Walk every live (flag=1) record. Parallelized across keyfile shards
   internally; cb may run on multiple threads — caller's ctx is responsible
   for its own synchronization. */
int slotcask_walk_live(SlotcaskDb *db, SlotcaskScanCb cb, void *ctx);

/* Same as slotcask_walk_one_shard but the callback also receives the
   kf slot index. Used by the bitmap-index reindex path which needs to
   key bit positions by (kf_shard, kf_slot).

   Caller must hold `kh` for `kf_shard_id` for the entire call. This
   function neither acquires nor releases it; making ownership explicit
   lets callers establish any outer cross-cache lock order before walking. */
typedef int (*SlotcaskScanSlotCb)(uint32_t slot, const uint8_t hash16[16],
                                   const void *key, size_t klen,
                                   const void *value, size_t vlen,
                                   void *ctx);
int slotcask_walk_one_shard_slots_locked(SlotcaskDb *db, int kf_shard_id,
                                          const SlotcaskKfHandle *kh,
                                          SlotcaskScanSlotCb cb, void *ctx);

/* Pre-grow all kf shards to absorb `total_new` upcoming inserts without
   triggering inline resplits mid-insert. Bulk-insert dispatchers should
   call this once per request, before workers start writing segments. */
int slotcask_pregrow_kf(SlotcaskDb *db, size_t total_new);

/* Per-shard walker, used by the engine to parallelise scans across kf
   shards while keeping engine-side state (thread-local output streams,
   etc.) under the engine's own per-worker control. `stop_flag` is a
   shared int (caller's scope, atomic 0/1) that any worker can flip to
   abort the run; pass NULL to disable. Returns 0 on success or normal
   stop, -1 on bad args. */
int slotcask_walk_one_shard(SlotcaskDb *db, int kf_shard_id,
                             SlotcaskScanCb cb, void *ctx,
                             int *stop_flag);

/* Per-shard streaming walker — fires cb() per record as the kf shard
   is scanned, with no Pass-1 ref-buffer. Trades the per-segment-batched
   acquire optimisation for immediate cb response. Right choice for
   limit-bound queries (KEYS first 100, FIND limit 10) where the caller
   sets stop_flag as soon as enough records have been collected and the
   batched-acquire savings on the rest of the shard would never be
   realised. cb returning non-zero stops this shard's walk; stop_flag
   propagates that to the other shard workers. Returns 0 on success/stop. */
int slotcask_walk_one_shard_streaming(SlotcaskDb *db, int kf_shard_id,
                                       SlotcaskScanCb cb, void *ctx,
                                       int *stop_flag);

/* Skip the first `skip_n` live records without loading their values.
   The cb fires only on records past the skip window. Used by
   cmd_fetch with offset/cursor — eliminates the per-skipped-record
   segcache_acquire that walk_live would otherwise do. */
int slotcask_walk_live_skip(SlotcaskDb *db, int64_t skip_n,
                              SlotcaskScanCb cb, void *ctx);

/* Count live records by walking kf entries only — no seg I/O.
   Used by cmd_recount where the caller only needs the total count,
   not the values. ~50× faster than slotcask_walk_live on a counter
   workload at 1M records. */
int64_t slotcask_count_live(SlotcaskDb *db);

/* Look up by hash16 only (index-driven access). Walks the keyfile shard for
 * `hash16`, invokes cb for each live entry whose hash matches. Almost
 * always 0 or 1 invocation per call (hash collisions are rare). cb returning
 * 1 stops further probing. */
int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                             SlotcaskScanCb cb, void *ctx);

/* ============================================================ Two-phase bulk fetch */

/* Resolve hashes to segment file + offset locations.
   Takes a flat array of hashes (any KF shards).
   Buckets by shard_for_hash internally, probes each KF shard sequentially.
   Returns malloc'd array of SlotcaskResolvedRec. *out_n = count of found records.
   Caller free()s the returned pointer. Returns NULL on error/not found. */
SlotcaskResolvedRec *slotcask_bulk_resolve_hashes(SlotcaskDb *db,
                                                   const uint8_t (*hashes)[16],
                                                   size_t n,
                                                   size_t *out_n);

/* Fetch records from pre-resolved locations.
   Groups input by (sid, fid) and dispatches parallel_for_io across
   unique segment files. Each segment file is opened once via segcache.
   Records are verified via seg_rec_live_with_hash before callback.
   Callback signature matches existing SlotcaskScanCb.
   Returns 0 on success, -1 on error. */
int slotcask_bulk_fetch_resolved(SlotcaskDb *db,
                                  const SlotcaskResolvedRec *recs,
                                  size_t n,
                                  void *ctx,
                                  SlotcaskScanCb cb);

/* Resolve + fetch in one call (wraps both phases above).
   Same callback signature as slotcask_bulk_fetch_resolved.
   This is the primary entry point for callers with hashes. */
int slotcask_bulk_resolve_and_fetch(SlotcaskDb *db,
                                     const uint8_t (*hashes)[16],
                                     size_t n,
                                     void *ctx,
                                     SlotcaskScanCb cb);

/* Probe the keyfile for a live entry matching hash16. Returns 0 and sets
 * *out_slot if found, -1 if not. Does NOT read the segment file — stops
 * after the KF probe. Used by bitmap post-filter to get the slot index
 * needed for bm_test without paying the full record-fetch cost. */
int kf_find_slot_for_hash(const SlotcaskDb *db,
                           const uint8_t hash16[16],
                           uint32_t *out_slot);

/* Compute the starting slot index for hash in a KF of capacity `cap`.
 * Public so hot loops can inline the probe without per-hash acquire. */
size_t kf_slot_for(const uint8_t hash[16], size_t cap);

/* Build the canonical kf shard path under a data_dir.
 * Public so query workers can pre-open KF handles. */
void kf_path_for(char out[PATH_MAX], const char *data_dir, int shard_id);

#endif
