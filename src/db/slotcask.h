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

/* Default keyfile slots per shard. Pre-sized for 50 % load at the documented
   ~150K-records-per-shard sweet spot from CLAUDE.md. Constant across splits
   because create-object takes no expected_records hint. 12 MB per shard. */
#define SLOTCASK_DEFAULT_SLOTS_PER_SHARD  (512u * 1024)
#define SLOTCASK_MAX_SLOTS_PER_SHARD      (16u * 1024 * 1024)

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
int  segcache_acquire(SlotcaskSegHandle *h, const char *path, int writer);
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

typedef struct {
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
   [SLOTCASK_DEFAULT_SLOTS_PER_SHARD floor, SLOTCASK_MAX_SHARDS]. slot_size is
   the fixed per-record byte width (header + max key + max value, rounded to 8).
   Performs crash recovery if `.dirty` marker is present. */
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

#endif
