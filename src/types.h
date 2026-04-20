#ifndef TYPES_H
#define TYPES_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <signal.h>
#include <dirent.h>
#include <errno.h>
#include <limits.h>
#include <time.h>
#include <pthread.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define XXH_INLINE_ALL
#include "xxhash.h"
#include "btree.h"

#define SLOT_SIZE   8192
#define HEADER_SIZE 24               /* Zone A entry size */
#define SHARD_HDR_SIZE   32          /* ShardHeader at file offset 0 */
#define INITIAL_SLOTS    256         /* starting slots_per_shard for new shards */
#define MIN_SPLITS       4
#define MAX_SPLITS       4096
#define MAX_KEY_CEILING  1024        /* hard upper bound on per-object max_key
                                        (uint16 allows 65535, but keys near that
                                        size bloat slot_size; 1024 is plenty —
                                        UUIDs are 36B, composite keys rarely
                                        exceed 256B) */
#define GROW_LOAD_NUM    1           /* grow when count*DEN >= slots*NUM (50%) */
#define GROW_LOAD_DEN    2
#define SHARD_MAGIC      0x564B4853u /* 'SHKV' little-endian */
#define SHARD_VERSION    1u
#define MAX_FIELDS  256

/* Per-shard header at byte 0 of each shard file. Records growth state so
   we can recover slots_per_shard after a restart without re-deriving it
   from file size (which would break once prealloc_mb pads the tail). */
typedef struct __attribute__((packed)) {
    uint32_t magic;                  /* SHARD_MAGIC */
    uint32_t version;                /* SHARD_VERSION */
    uint32_t slots_per_shard;        /* current power-of-two slot count */
    uint32_t record_count;           /* active (non-tombstoned) records */
    uint8_t  reserved[16];
} ShardHeader;

/* Typed field system */
enum FieldType {
    FT_NONE = 0,
    FT_VARCHAR,     /* varchar:N — fixed N bytes, null-padded */
    FT_LONG,        /* long — 8 bytes int64 big-endian */
    FT_INT,         /* int — 4 bytes int32 big-endian */
    FT_SHORT,       /* short — 2 bytes int16 big-endian */
    FT_DOUBLE,      /* double — 8 bytes IEEE 754 */
    FT_BOOL,        /* bool — 1 byte (0/1) */
    FT_BYTE,        /* byte — 1 byte uint8 */
    FT_NUMERIC,     /* numeric:P,S — 8 bytes int64 × 10^S */
    FT_DATE,        /* date — 4 bytes int32 yyyyMMdd big-endian */
    FT_DATETIME     /* datetime — 6 bytes packed yyyyMMddHHmmss big-endian */
};

enum DefaultKind {
    DK_NONE = 0,
    DK_LITERAL,       /* default=<value> — constant, INSERT only */
    DK_AUTO_CREATE,   /* auto_create — server datetime, INSERT only */
    DK_AUTO_UPDATE,   /* auto_update — server datetime, INSERT + UPDATE */
    DK_SEQ,           /* default=seq(name) — next sequence value, INSERT only */
    DK_UUID,          /* default=uuid() — UUID v4, INSERT only */
    DK_RANDOM         /* default=random(N) — N random bytes hex-encoded, INSERT only */
};

typedef struct {
    char name[256];
    enum FieldType type;
    int size;           /* storage bytes for this field */
    int offset;         /* byte offset within value region */
    int numeric_scale;  /* S for numeric:P,S (decimal places) */
    int removed;        /* 1 if tombstoned (fields.conf line ends with :removed);
                           its bytes stay reserved until vacuum compacts them out */
    enum DefaultKind default_kind;
    char default_val[256]; /* literal value, seq name, or random byte count */
} TypedField;

typedef struct {
    TypedField fields[MAX_FIELDS];
    int nfields;
    int total_size;     /* sum of all field sizes = value size */
    int typed;
} TypedSchema;
#define MAX_LINE    65536
/* Probe bound is dynamic per-shard: callers use the shard's current
   slots_per_shard (via FcacheRead.slots_per_shard or ShardHeader). Growth at 50%
   load keeps clusters short, so typical probes stop in 1-5 iterations. */

/* Zone A entry: 24 bytes. Payload (key+value) lives in Zone B at a fixed offset. */
typedef struct __attribute__((packed)) {
    uint8_t  hash[16];
    uint16_t flag;        /* 0=empty, 1=active, 2=deleted */
    uint16_t key_len;
    uint32_t value_len;
} SlotHeader;

typedef struct {
    int splits;
    int max_key;
    int max_value;
    int slot_size;        /* = payload_size per slot (max_key + max_value), 8-aligned */
    int prealloc_mb;
} Schema;

/* Shard file layout:
     [ShardHeader: 32B]
     [Zone A: slots_per_shard * 24B headers]
     [Zone B: slots_per_shard * slot_size payloads]
   Payload holds key+value packed (key_len from header determines value offset).
   slots_per_shard is a per-shard value recorded in ShardHeader. */
static inline size_t zoneA_off(uint32_t slot) {
    return SHARD_HDR_SIZE + (size_t)slot * HEADER_SIZE;
}
static inline size_t zoneB_off(uint32_t slot, uint32_t slots_per_shard, uint32_t slot_size) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * HEADER_SIZE + (size_t)slot * slot_size;
}
static inline size_t shard_zoneA_end(uint32_t slots_per_shard) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * HEADER_SIZE;
}
static inline size_t shard_file_size(uint32_t slots_per_shard, uint32_t slot_size) {
    return SHARD_HDR_SIZE + (size_t)slots_per_shard * (HEADER_SIZE + slot_size);
}

enum SearchOp {
    OP_EQUAL, OP_NOT_EQUAL,
    OP_LESS, OP_GREATER, OP_LESS_EQ, OP_GREATER_EQ,
    OP_LIKE, OP_NOT_LIKE,
    OP_CONTAINS, OP_NOT_CONTAINS,
    OP_STARTS_WITH, OP_ENDS_WITH,
    OP_IN, OP_NOT_IN,
    OP_BETWEEN,
    OP_EXISTS, OP_NOT_EXISTS
};

typedef struct {
    char field[256];
    enum SearchOp op;
    char value[1024];
    char value2[1024];
    char **in_values;
    int in_count;
    int in_cap;
} SearchCriterion;

/* Forward decl — opaque outside query.c. Created by compile_criteria() and
   consumed by match_typed() in the scan hot path. */
typedef struct CompiledCriterion CompiledCriterion;

/* Tree form of criteria — supports AND/OR composition.
   Built by parse_criteria_tree(); flat arrays parse as a single AND root.
   A LEAF carries a plain SearchCriterion; AND/OR branches own their children. */
typedef enum { CNODE_LEAF, CNODE_AND, CNODE_OR } CriteriaNodeKind;

typedef struct CriteriaNode {
    CriteriaNodeKind kind;
    SearchCriterion leaf;            /* valid iff kind == CNODE_LEAF */
    CompiledCriterion *compiled;     /* populated by compile_criteria_tree() */
    struct CriteriaNode **children;  /* valid iff kind == CNODE_AND || CNODE_OR */
    int n_children;
} CriteriaNode;

#define MAX_CRITERIA_DEPTH 16

/* Concurrent hash set of 16-byte xxh128 record-key hashes.
   Used by the OR index-union fast path: multiple threads insert candidate keys
   from their respective B+ tree lookups; duplicates are deduplicated via
   lock-free CAS inserts. Capacity is fixed at construction — callers must
   pre-size large enough to avoid hitting full (probes return failure). */
typedef struct {
    uint8_t  (*keys)[16];    /* cap buckets of 16-byte xxh128 */
    uint32_t *state;          /* 0=empty, 1=filling, 2=filled */
    size_t   cap;             /* power of two */
    size_t   mask;            /* cap - 1 */
    _Atomic size_t n;         /* filled count (approximate until all threads join) */
} KeySet;

KeySet *keyset_new(size_t capacity_hint);
void    keyset_free(KeySet *ks);
/* Returns 1 if newly inserted, 0 if duplicate, -1 if set is full. */
int     keyset_insert(KeySet *ks, const uint8_t hash[16]);
int     keyset_contains(const KeySet *ks, const uint8_t hash[16]);
size_t  keyset_size(const KeySet *ks);
/* Iterate every filled bucket calling cb(hash, ctx). Stops early if cb returns non-zero. */
void    keyset_iter(const KeySet *ks,
                    int (*cb)(const uint8_t hash[16], void *ctx),
                    void *ctx);

typedef struct {
    char **keys;
    int count;
} ExcludedKeys;

/* ========== Global config ========== */
extern uint32_t g_timeout;
extern int g_port;
extern int g_max_threads;
extern int g_workers;
extern int g_index_page_size;
extern int g_global_limit;
extern size_t g_query_buffer_max_bytes;
extern int g_disable_localhost_trust;
extern int g_token_cap;

/* Per-request statement timeout override. Thread-local: the server's dispatch
   sets it from the JSON request's "timeout_ms" field before calling cmd_*,
   then clears it after. When 0, cmd_* falls back to g_timeout * 1000.
   resolve_timeout_ms() is the single read point used everywhere a
   QueryDeadline is constructed. */
extern _Thread_local uint32_t g_request_timeout_ms;
static inline uint32_t resolve_timeout_ms(void) {
    return g_request_timeout_ms > 0 ? g_request_timeout_ms
                                    : (uint32_t)(g_timeout * 1000);
}

/* Token permission levels.
   r   = read-only ops (get/exists/find/count/aggregate/fetch/keys/get-file).
   rw  = all reads + data-write ops (insert/update/delete/bulk/put-file).
   rwx = all writes + admin ops. Admin commands have their own scope (server,
         tenant, or object); token scope must be as broad or broader. */
#define PERM_R   1
#define PERM_RW  2
#define PERM_RWX 3

/* Canonical error message for per-query memory-cap exceeded. Single string so
   callers don't drift apart; emit via OUT() at any of the 7 collection sites. */
#define QUERY_BUFFER_ERR "{\"error\":\"query memory buffer exceeded; narrow criteria, add limit/offset, or stream via fetch+cursor\"}\n"
extern int g_max_request_size;
extern int g_fcache_cap;
extern int g_btcache_cap;
extern char g_db_root[PATH_MAX];
extern char g_log_dir[PATH_MAX];
extern int g_log_level;

/* Per-thread output stream — worker threads set this to their client socket FILE*.
   Defaults to stdout for CLI mode. All command output uses OUT() instead of printf(). */
extern __thread FILE *g_out;
#define OUT(...) fprintf(g_out ? g_out : stdout, __VA_ARGS__)
extern int g_log_retain_days;

/* Monitoring / stats */
extern uint64_t g_ucache_hits;
extern uint64_t g_ucache_misses;
extern uint64_t g_bt_cache_hits;
extern uint64_t g_bt_cache_misses;
extern uint64_t g_server_start_ms;
extern uint64_t g_slow_query_count;
extern int g_slow_query_ms;

#define SLOW_QUERY_RING 64
typedef struct {
    uint64_t ts_ms;           /* monotonic ms */
    uint32_t duration_ms;
    char mode[24];
    char dir[24];
    char object[48];
} SlowQueryEntry;
extern SlowQueryEntry g_slow_queries[SLOW_QUERY_RING];
extern int g_slow_query_head;
extern pthread_mutex_t g_slow_query_lock;

uint64_t now_ms(void);
uint64_t now_ms_coarse(void);

/* Per-query statement-timeout deadline. Shared across worker threads of a
   single query (pass as pointer). Check inside hot loops via the tick macro
   below. `timeout_ms == 0` disables the check. */
typedef struct {
    uint64_t t0_ms;
    uint32_t timeout_ms;
    volatile int timed_out;
} QueryDeadline;

/* Inline check used in scan loops. Counter is caller-local; the real clock
   read only fires every 1024 calls → essentially free on the fast path. */
static inline int query_deadline_tick(QueryDeadline *d, int *counter) {
    if (!d || d->timeout_ms == 0) return 0;
    if (__atomic_load_n(&d->timed_out, __ATOMIC_RELAXED)) return 1;
    if ((++(*counter) & 0x3FF) != 0) return 0;
    uint64_t now = now_ms_coarse();
    if (now - d->t0_ms > (uint64_t)d->timeout_ms) {
        __atomic_store_n(&d->timed_out, 1, __ATOMIC_RELAXED);
        return 1;
    }
    return 0;
}
int parallel_threads(void);
void log_slow_query(const char *mode, const char *dir, const char *object, uint32_t duration_ms);
int ucache_stats(int *used_slots, int *total_slots, size_t *total_bytes);
int bt_cache_stats(int *used_slots, int *total_slots, size_t *total_bytes);

/* ========== Function declarations ========== */

/* util.c */
void mkdirp(const char *path);
char *dirname_of(const char *path);
char *read_file(const char *path, size_t *out_len);
int is_number(const char *s);
ExcludedKeys parse_excluded_keys(const char *csv);
int is_excluded(const ExcludedKeys *ex, const char *key);
void free_excluded(ExcludedKeys *ex);
void rmrf(const char *path);
void sort_dedup_file(const char *path);

/* JSON helpers */
const char *json_skip(const char *p);
const char *json_skip_value(const char *p);

/* ========== Single-pass JSON object parser ==========
   shard-db's request / criterion / join / aggregate spec shapes are all
   known small objects with a handful of keys. Walking the JSON once per
   field extraction is pure waste — especially inside array loops where a
   10-element criteria array + 5 reads per element = 50 walks over the
   same sub-JSON. json_parse_object() walks once and materialises every
   top-level {"name": value, ...} field as a (name, value) span pair into
   a caller-provided JsonObj. Lookups become O(n) over the small array
   (typically <20 entries), avoiding both the walk AND the per-field
   malloc/memcpy that json_get_raw does. Values are spans pointing into
   the original JSON — no allocation, no ownership transfer; the original
   buffer must outlive the JsonObj. */

/* Maximum top-level fields we'll record per JsonObj. Sized at MAX_FIELDS so
   any legit schema's request fits; clients sending more get rejected at
   parse time rather than silently truncated. 8 KB of stack per call. */
#define JSON_OBJ_MAX_FIELDS MAX_FIELDS

typedef struct {
    const char *name; size_t nlen;
    const char *val;  size_t vlen;   /* raw span; includes quotes for strings, brackets for arrays/objects */
} JsonField;

typedef struct {
    JsonField f[JSON_OBJ_MAX_FIELDS];
    int n;
} JsonObj;

/* Parse the top-level object of `s`. Returns number of fields parsed (>=0),
   or -1 on malformed input / overflow. Safe to call on NULL or non-object
   input (returns -1). */
int  json_parse_object(const char *s, size_t slen, JsonObj *out);

/* Lookup by key. Returns 1 and fills (*val, *vlen) with the raw span
   (quoted strings still have quotes). Returns 0 on miss. */
int  json_obj_get(const JsonObj *o, const char *key, const char **val, size_t *vlen);

/* Same as json_obj_get, but strips surrounding quotes if present. Use for
   string / numeric / bool fields. For nested objects/arrays use json_obj_get
   which returns the span with brackets intact for recursive parsing. */
int  json_obj_unquoted(const JsonObj *o, const char *key, const char **val, size_t *vlen);

/* Convenience: parse an integer field. Returns `fallback` on miss. */
int  json_obj_int(const JsonObj *o, const char *key, int fallback);

/* Copy an unquoted string field into a caller-provided buffer (NUL-terminated).
   Returns number of bytes written (0 if missing or empty). Fits most uses that
   previously called json_get_raw + free. */
int  json_obj_copy(const JsonObj *o, const char *key, char *buf, size_t bufsz);

/* malloc-based wrapper for callers that still need a heap-owned NUL-terminated
   string (e.g. for long-lived storage). Returns NULL if missing. Caller frees. */
char *json_obj_strdup(const JsonObj *o, const char *key);

/* Raw span variant used for nested arrays/objects that the caller will parse
   further. Returns a malloc'd NUL-terminated copy including the surrounding
   brackets/quotes. Same ownership semantics as json_get_field with strip=0. */
char *json_obj_strdup_raw(const JsonObj *o, const char *key);

/* String-or-array field → comma-separated malloc'd string, or NULL on miss. */
char *json_obj_string_or_array(const JsonObj *o, const char *key);
int json_get_fields(const char *json, const char **keys, int nkeys, char **out_values);
char *extract_field_value(const char *json, const char *field_name);

/* Base64 (util.c, RFC 4648 standard alphabet) */
size_t b64_encoded_size(size_t raw_len);
size_t b64_decoded_maxsize(size_t b64_len);
void b64_encode(const uint8_t *raw, size_t raw_len, char *out);
int b64_decode(const char *b64, size_t b64_len, uint8_t *out, size_t *out_len);

/* Filename sanitizer — rejects /, \, .., control chars, empty, >255 bytes */
int valid_filename(const char *name);

/* config.c */
int load_db_root(char *out, size_t outlen);
Schema load_schema(const char *effective_root, const char *object);
int load_splits(const char *db_root, const char *object);
int load_index_fields(const char *db_root, const char *object, char fields[][256], int max_fields);
void invalidate_idx_cache(const char *object);
void load_dirs(void);
int is_valid_dir(const char *dir);
void build_effective_root(char *out, size_t outlen, const char *dir);

/* Typed field system */
TypedSchema *load_typed_schema(const char *db_root, const char *object);
int typed_encode(const TypedSchema *ts, const char *json, uint8_t *out, int out_size);
int typed_encode_defaults(const TypedSchema *ts, const char *json, uint8_t *out,
                          int out_size, const char *db_root, const char *object);
char *typed_decode(const TypedSchema *ts, const uint8_t *data, int data_len);
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data, int field_idx);
void encode_field(const TypedField *f, const char *val, uint8_t *out);
void encode_field_len(const TypedField *f, const char *val, size_t vlen, uint8_t *out);
int typed_field_index(const TypedSchema *ts, const char *name);
void parse_field_type(const char *spec, TypedField *f);

/* storage.c */
void compute_hash_raw(const char *key, size_t key_len, uint8_t hash_out[16]);
void addr_from_hash(const uint8_t hash[16], int splits, int *shard_id, int *slot);
void compute_addr(const char *key, size_t key_len, int splits, uint8_t hash_out[16], int *shard_id, int *slot);
void build_shard_path(char *buf, size_t buflen, const char *db_root, const char *object, int shard_id);
void build_shard_filename(char *buf, size_t buflen, const char *data_dir, int shard_id);
uint8_t *mmap_with_hints(void *addr, size_t len, int prot, int flags, int fd, off_t off);

/* Unified shard cache (ucache) — persistent MAP_SHARED mmap per shard.
   Per-entry rwlock: shared for reads, exclusive for writes.
   FcacheRead handle used for both read and write operations. */
typedef struct {
    uint8_t *map;   /* NULL on failure */
    size_t   size;
    uint32_t slots_per_shard;  /* captured at open time */
    int      slot;  /* cache slot index, -1 = invalid */
} FcacheRead;

typedef struct UCacheEntry {
    char     path[PATH_MAX];
    int      fd;
    uint8_t *map;
    size_t   map_size;
    uint32_t slots_per_shard;   /* current — updated by grow */
    pthread_rwlock_t rwlock;
    int      used;
    uint8_t *slot_bits;
    int      max_dirty_slot;
    int      dirty;
    uint64_t last_access;   /* monotonic counter for LRU eviction */
    /* Old mapping retained after a grow so concurrent readers holding
       fc.map pointers stay valid; freed on next grow or shutdown. */
    uint8_t *retired_map;
    size_t   retired_size;
    int      retired_fd;
} UCacheEntry;

void       fcache_init(int cap);
void       fcache_shutdown(void);
FcacheRead fcache_get_read(const char *path);
void       fcache_release(FcacheRead h);
/* Open (or create) a shard for writing. slot_size > 0 creates the file with
   INITIAL_SLOTS slots if missing; slot_size == 0 opens-only (fails if absent). */
FcacheRead ucache_get_write(const char *path, int slot_size, int prealloc_mb);
void       ucache_write_release(FcacheRead h);
/* Double slots_per_shard for this shard: rehash live records into a new file,
   atomic rename, swap mapping. Caller must NOT hold the entry wrlock. */
int        ucache_grow_shard(const char *path, int slot_size, int prealloc_mb);
/* Post-insert threshold check — calls ucache_grow_shard if load >= 50%. */
void       ucache_maybe_grow(int ucache_slot, int slot_size, int prealloc_mb);
/* Sweep stale shard.new files after a crash during grow. Called at startup. */
void       grow_recovery(const char *db_root);
UCacheEntry *ucache_entry(int slot);
int        ucache_slot_count(void);
void       fcache_invalidate(const char *path_prefix);
/* Adjust a shard's record_count in the ShardHeader (caller holds wrlock). */
void       ucache_bump_record_count(int ucache_slot, int delta);
void update_count(const char *db_root, const char *object, int delta);
void set_count(const char *db_root, const char *object, int count);
void update_deleted_count(const char *db_root, const char *object, int delta);
void reset_deleted_count(const char *db_root, const char *object);
int get_deleted_count(const char *db_root, const char *object);
int get_live_count(const char *db_root, const char *object);
int cmd_get(const char *db_root, const char *object, const char *key);
int cmd_insert(const char *db_root, const char *object, const char *key, const char *value,
               const char *if_json, int if_not_exists);
int cmd_update(const char *db_root, const char *object, const char *key, const char *partial_json,
               const char *if_json, int dry_run);
int cmd_delete(const char *db_root, const char *object, const char *key, const char *if_json,
               int dry_run);
int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter);
int cmd_exists_multi(const char *db_root, const char *object, const char *keys_json,
                     const char *format, const char *delimiter);

/* CSV helpers exposed so storage.c (cmd_get_multi, cmd_exists_multi) can use them. */
void csv_emit_cell(const char *val, char delim);
char parse_csv_delim(const char *s);
int cmd_not_exists(const char *db_root, const char *object, const char *keys_json);

/* index.c */
void write_index_entry(const char *db_root, const char *object, const char *field, const char *attr_val, const uint8_t hash16[16]);
void delete_index_entry(const char *db_root, const char *object, const char *field, const char *attr_val, const uint8_t hash16[16]);
void index_parallel(const char *db_root, const char *object, const char *value, const uint8_t hash16[16], char fields[][256], int nfields);
int cmd_add_index(const char *db_root, const char *object, const char *field, int force);
int cmd_add_indexes(const char *db_root, const char *object, const char *fields_json, int force);
int cmd_remove_index(const char *db_root, const char *object, const char *field);
int cmd_remove_indexes(const char *db_root, const char *object, const char *fields_json);

/* Field schema context — typed binary via fields.conf */
typedef struct {
    char fields[MAX_FIELDS][256];
    int nfields;
    TypedSchema *ts;  /* non-NULL = typed binary mode */
} FieldSchema;
void init_field_schema(FieldSchema *fs, const char *db_root, const char *object);
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs);
char *decode_value(const char *raw, size_t raw_len, FieldSchema *fs);

/* Typed-binary fast path: compile criteria against a TypedSchema once, then
   call match_typed() per record during scan. Composite/unknown fields fall
   back to decode_field + match_criterion internally. */
CompiledCriterion *compile_criteria(const SearchCriterion *in, int n, const TypedSchema *ts);
void free_compiled_criteria(CompiledCriterion *arr, int n);
int  match_typed(const uint8_t *rec, const CompiledCriterion *cc, FieldSchema *fs);
int  match_criterion(const char *val_str, const SearchCriterion *c);

/* query.c */
extern volatile int g_scan_stop; /* set to 1 to abort all in-flight shard scans */
typedef int (*scan_callback)(const SlotHeader *hdr, const uint8_t *block, void *ctx); /* return 0=continue, 1=stop */
void scan_shards(const char *data_dir, int slot_size, scan_callback cb, void *ctx);
int fetch_record_by_hash(const char *db_root, const char *object, const Schema *sch, const uint8_t hash16[16], int *printed, void *fs);
int cmd_size(const char *db_root, const char *object);
int cmd_count(const char *db_root, const char *object, const char *criteria_json);
int cmd_exists(const char *db_root, const char *object, const char *key);
int cmd_keys(const char *db_root, const char *object, int offset, int limit, const char *format, const char *delimiter);
int cmd_fetch(const char *db_root, const char *object, int offset, int limit, const char *proj_str, const char *cursor, const char *format, const char *delimiter);
int cmd_find(const char *db_root, const char *object, const char *criteria_json, int offset, int limit, const char *proj_str, const char *excluded_csv, const char *format, const char *delimiter, const char *join_json, const char *order_by, const char *order_dir);
int cmd_bulk_insert(const char *db_root, const char *object, const char *input);
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str);
int cmd_bulk_insert_delimited(const char *db_root, const char *object, const char *filepath, char delimiter);
int cmd_bulk_delete(const char *db_root, const char *object, const char *input);
int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    int limit, int dry_run);
int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, int limit, int dry_run);
int cmd_vacuum(const char *db_root, const char *object,
               int compact, int new_splits);
int rebuild_object(const char *db_root, const char *object,
                   int new_splits, int drop_tombstoned,
                   char added_lines[][256], int n_added);
int cmd_recount(const char *db_root, const char *object);
int cmd_shard_stats(const char *db_root, const char *object, int as_table);
int cmd_truncate(const char *db_root, const char *object);
int cmd_backup(const char *db_root, const char *object);
int cmd_sequence(const char *db_root, const char *object, const char *seq_name, const char *action, int batch_size);
int cmd_aggregate(const char *db_root, const char *object,
                  const char *criteria_json, const char *group_by_json,
                  const char *aggregates_json, const char *having_json,
                  const char *order_by, int order_desc, int limit,
                  const char *format, const char *delimiter);
int cmd_put_file(const char *db_root, const char *object, const char *src);
int cmd_get_file_path(const char *db_root, const char *object, const char *filename);
int cmd_put_file_b64(const char *db_root, const char *object,
                     const char *filename, const char *b64_data, size_t b64_len,
                     int if_not_exists);
int cmd_get_file_b64(const char *db_root, const char *object, const char *filename);
int cmd_delete_file(const char *db_root, const char *object, const char *filename);
int cmd_create_object(const char *db_root, const char *dir, const char *object,
                      const char *fields_json, const char *indexes_json,
                      int splits, int max_key);

/* Schema mutations */
int cmd_rename_field(const char *db_root, const char *object,
                     const char *old_name, const char *new_name);
int cmd_remove_fields(const char *db_root, const char *object,
                      char names[][256], int nnames);
int cmd_add_fields(const char *db_root, const char *object,
                   char lines[][256], int nlines);
void invalidate_schema_caches(const char *db_root, const char *object);

/* objlock.c — per-object rwlock + rebuild crash recovery */
void objlock_init(void);
void objlock_rdlock(const char *db_root, const char *object);
void objlock_rdunlock(const char *db_root, const char *object);
void objlock_wrlock(const char *db_root, const char *object);
void objlock_wrunlock(const char *db_root, const char *object);
void rebuild_recovery(const char *db_root);

/* logging */
void log_init(const char *db_root);
void log_shutdown(void);
void log_msg(int level, const char *fmt, ...);

/* match / criteria / CAS */
int match_criterion(const char *val_str, const SearchCriterion *c);
enum SearchOp parse_op(const char *s);
int parse_criteria_json(const char *json, SearchCriterion **out, int *count);
void free_criteria(SearchCriterion *c, int count);

/* Tree parser — returns NULL for empty/no-criteria (not an error) or on parse
   failure (err_out set to a string literal). Caller owns the returned tree and
   must free via free_criteria_tree(). */
CriteriaNode *parse_criteria_tree(const char *json, const char **err_out);
void free_criteria_tree(CriteriaNode *root);

/* Pre-compile every leaf in the tree against the schema so match_typed() has
   zero-malloc-per-record cost during scans. Safe to call multiple times — leaves
   already compiled are skipped. */
void compile_criteria_tree(CriteriaNode *root, const TypedSchema *ts);

/* Recursive match: AND short-circuits on first false, OR on first true. Passing
   NULL means "no criteria" → match-all (returns 1). */
int criteria_match_tree(const uint8_t *rec, const CriteriaNode *node, FieldSchema *fs);
int cas_check(TypedSchema *ts, const uint8_t *value_ptr, SearchCriterion *crit, int ncrit);


/* config.c globals */
#define DIRS_BUCKETS 2048
extern char g_dirs[DIRS_BUCKETS][256];
extern int g_dirs_used[DIRS_BUCKETS];
extern int g_dirs_count;
extern pthread_mutex_t g_dirs_lock;

/* server.c */
int cmd_server(const char *db_root, int daemonize);
int cmd_stop(const char *db_root);
int cmd_status(const char *db_root);
int cmd_query(int port, int argc, char **argv);
int cmd_query_json(int port, const char *json);
int cmd_put_file_tcp(int port, const char *dir, const char *object,
                     const char *local_path, int if_not_exists);
int cmd_get_file_tcp(int port, const char *dir, const char *object,
                     const char *filename, const char *out_path);
int read_server_port(const char *db_root);
void dispatch_json_query(const char *raw_db_root, const char *json, const char *client_ip);
void server_process_fast(const char *db_root, const char *line, const char *client_ip);
void load_allowed_ips_conf(const char *db_root);
void load_tokens_conf(const char *db_root);
int is_ip_trusted(const char *ip);
int is_token_valid(const char *token);

/* index.c — comparators */
int cmp_btentry_fn(const void *a, const void *b);
#endif
