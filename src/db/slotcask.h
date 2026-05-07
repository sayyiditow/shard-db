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
#include <pthread.h>
#include <linux/limits.h>

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
    SlotcaskKfEntry *map;
    size_t  map_size;        /* bytes; map_size / 24 = capacity */
    size_t  capacity;        /* slots in this shard */
} SlotcaskKfHandle;

void kfcache_init(int cap);
void kfcache_shutdown(void);
/* Acquire (open or hit) a keyfile shard. writer=1 takes wrlock + creates if
   absent (sized to slots_capacity * 24B). writer=0 takes rdlock; fails if
   absent. Returns 0 on success, -1 on error. */
int  kfcache_acquire(SlotcaskKfHandle *h, const char *path,
                     size_t slots_capacity, int writer);
void kfcache_release(SlotcaskKfHandle *h);

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
                      int create, int writer);
void segcache_release(SlotcaskSegHandle *h);

/* ============================================================ Per-stream pool */

typedef struct {
    uint16_t file_id;
    uint32_t offset;
} SlotcaskFreeSlot;

typedef struct {
    int             stream_id;
    char            stream_dir[PATH_MAX];

    /* Append path */
    pthread_mutex_t rotation_lock;
    uint32_t        active_file_id;
    uint64_t        reserve_off;

    /* Free pool — try_lock pattern; only one consumer at a time */
    pthread_mutex_t pool_lock;
    SlotcaskFreeSlot *free_slots;
    size_t          free_count;
    size_t          free_cap;
} SlotcaskStream;

/* ============================================================ DB handle */

typedef struct SlotcaskDb {
    char    data_dir[PATH_MAX];
    int     num_shards;
    int     num_streams;
    int     slot_size;       /* fixed; set at open time from schema or arg */
    size_t  slots_per_shard;

    SlotcaskStream *streams;
} SlotcaskDb;

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

/* Return 0 to commit; non-zero to abort. NULL = always commit. */
typedef int (*slotcask_pre_commit_fn)(const SlotcaskOldRecord *old,
                                       const uint8_t *new_value, size_t new_vlen,
                                       int is_update, void *ctx);

typedef struct {
    int                       if_not_exists;     /* fail if key exists (insert path) */
    int                       require_existing;  /* fail if key missing (update path) */
    slotcask_check_fn         check;
    void                     *check_ctx;
    slotcask_pre_commit_fn    pre_commit;
    void                     *pre_commit_ctx;
} SlotcaskUpsertOpts;

/* ============================================================ Bulk upsert
 *
 * Per-record kfcache_acquire/release pairs were the dominant cost in
 * bulk-insert-via-slotcask_upsert_with_hooks (4× single-conn regression
 * vs v1's mmap-batch path). The bulk primitive amortises that lock by
 * accepting a batch where every record hashes to the SAME kf-shard,
 * acquiring the wrlock once for the whole batch. Caller pre-buckets
 * records by kf_shard_id (the engine's bulk_insert_shard_worker_v2
 * already does so via compute_addr).
 */
typedef struct {
    /* input: caller fills */
    const void *key;
    size_t      klen;
    const void *value;
    size_t      vlen;
    void       *user_ctx;              /* passed to pre_commit per record */
    /* output: callee fills */
    int         status;                /* 0=ok, -2=cond_not_met, -1=error */
    int         was_update;
} SlotcaskBulkRec;

typedef int (*slotcask_bulk_pre_commit_fn)(const SlotcaskOldRecord *old,
                                            SlotcaskBulkRec *rec,
                                            int is_update);

typedef struct {
    int                          if_not_exists;     /* skip if key exists */
    slotcask_bulk_pre_commit_fn  pre_commit;        /* fired per record under kf wrlock; NULL = no-op */
    /* Set to 1 iff pre_commit will dereference the SlotcaskOldRecord *old
       arg. When 0 (e.g. non-indexed bulk-insert), the primitive skips the
       per-record read_record_value on UPDATE — that's the dominant per-
       record cost on update-heavy workloads. Hook still fires with
       old=NULL. */
    int                          pre_commit_needs_old;
} SlotcaskBulkOpts;

/* Returns 0 if the batch ran (per-record results in recs[].status), -1 on
   hard error (kf_acquire failed). Records that hit if_not_exists or
   pre_commit returning non-zero get rec.status=-2 with rec.was_update
   set from the existing key. */
int slotcask_bulk_upsert_in_kfshard(SlotcaskDb *db, int kf_shard_id,
                                     SlotcaskBulkRec *recs, size_t n,
                                     const SlotcaskBulkOpts *opts);

/* Returns the kf shard id slotcask uses for the given hash. Exposed so
   callers (bulk-insert dispatcher) can pre-bucket records to match the
   bulk primitive's expectation that all records in one batch target the
   same kf shard. */
int slotcask_kf_shard_for_hash(const uint8_t hash[16], int num_shards);

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

typedef struct {
    slotcask_check_fn   check;
    void               *check_ctx;
    /* Fires with the old record under the kf wrlock, after kf_lookup but
       before kf flag=2. Caller removes index entries. Return 0 to commit
       the deletion; non-zero to abort (kf untouched, no tombstone). */
    int (*pre_commit)(const SlotcaskOldRecord *old, void *ctx);
    void               *pre_commit_ctx;
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
 * Returns NULL when info->storage_version != 2 — the caller is expected to
 * fall back to the legacy probe-into-slot path. NULL is also returned on
 * open failure (logged via fprintf to stderr).
 *
 * The pointer is BORROWED — never call slotcask_close on it. The registry
 * owns lifetime.
 */
typedef struct {
    int splits;            /* num_shards for the keyfile */
    int slot_size;         /* fixed per-record byte width */
    int streams;           /* persisted at create time, hardcoded by nproc */
    int storage_version;   /* 1 = legacy (registry returns NULL), 2 = slotcask */
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

/* Look up by hash16 only (index-driven access). Walks the keyfile shard for
   `hash16`, invokes cb for each live entry whose hash matches. Almost
   always 0 or 1 invocation per call (hash collisions are rare). cb returning
   1 stops further probing. */
int slotcask_lookup_by_hash(SlotcaskDb *db, const uint8_t hash16[16],
                             SlotcaskScanCb cb, void *ctx);

#endif
