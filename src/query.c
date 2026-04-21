#include "types.h"
#include "yyjson.h"

/* ========== Probing helpers ========== */

/* Check if slot at given offset matches our key. Returns:
   1 = exact match (same key, active)
   0 = empty slot (can write here)
  -1 = tombstone (deleted, can write here on insert, skip on get)
  -2 = occupied by different key (continue probing) */
volatile int g_scan_stop = 0; /* shared stop flag for parallel scan */

void scan_one_shard(const char *binpath, int slot_size,
                           scan_callback cb, void *ctx, pthread_mutex_t *out_lock) {
    /* Full-shard scans bypass the ucache (MADV_RANDOM hint is wrong for linear
       scans). Open direct with MADV_SEQUENTIAL for kernel readahead. */
    int fd = open(binpath, O_RDONLY);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); return; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return;
    madvise(map, st.st_size, MADV_SEQUENTIAL);
    size_t file_size = st.st_size;

    /* Read ShardHeader to learn this shard's slots_per_shard. */
    if (file_size < SHARD_HDR_SIZE) { munmap(map, file_size); return; }
    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) { munmap(map, file_size); return; }
    uint32_t shard_slots = sh->slots_per_shard;
    if (file_size < shard_zoneA_end(shard_slots)) { munmap(map, file_size); return; }

    /* Find last used Zone A slot (metadata-only tail trim — tiny region). */
    size_t scan_end = shard_slots;
    while (scan_end > 0) {
        const SlotHeader *h = (const SlotHeader *)(map + zoneA_off(scan_end - 1));
        if (h->flag != 0 || h->key_len != 0) break;
        scan_end--;
    }

    for (size_t i = 0; i < scan_end; i++) {
        if (g_scan_stop) break;
        const SlotHeader *hdr = (const SlotHeader *)(map + zoneA_off(i));
        if (hdr->flag == 1) {
            const uint8_t *block = map + zoneB_off(i, shard_slots, slot_size);
            int stop;
            if (out_lock) {
                pthread_mutex_lock(out_lock);
                stop = cb(hdr, block, ctx);
                pthread_mutex_unlock(out_lock);
            } else {
                stop = cb(hdr, block, ctx);
            }
            if (stop) { g_scan_stop = 1; break; }
        }
    }
    munmap(map, file_size);
}

typedef struct {
    const char *path;
    int slot_size;
    scan_callback cb;
    void *ctx;
    pthread_mutex_t *out_lock;
    FILE *parent_out;  /* inherit g_out from parent thread */
} ScanWorkerArg;

void *scan_worker(void *arg) {
    ScanWorkerArg *w = (ScanWorkerArg *)arg;
    if (g_scan_stop) return NULL;
    g_out = w->parent_out ? w->parent_out : stdout;
    scan_one_shard(w->path, w->slot_size, w->cb, w->ctx, w->out_lock);
    return NULL;
}

void scan_shards(const char *data_dir, int slot_size, scan_callback cb, void *ctx) {
    g_scan_stop = 0; /* reset stop flag */
    /* Collect all shard file paths */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(paths); return; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (path_count >= path_cap) {
            path_cap *= 2;
            paths = realloc(paths, path_cap * sizeof(char *));
        }
        char binpath[PATH_MAX];
        snprintf(binpath, sizeof(binpath), "%s/%s", data_dir, e1->d_name);
        paths[path_count++] = strdup(binpath);
    }
    closedir(d1);

    if (path_count == 0) { free(paths); return; }

    pthread_mutex_t out_lock = PTHREAD_MUTEX_INITIALIZER;
    ScanWorkerArg *args = malloc(path_count * sizeof(ScanWorkerArg));
    for (int i = 0; i < path_count; i++) {
        args[i] = (ScanWorkerArg){ paths[i], slot_size, cb, ctx, &out_lock, g_out };
    }
    parallel_for(scan_worker, args, path_count, sizeof(ScanWorkerArg));
    free(args);
    pthread_mutex_destroy(&out_lock);

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
}

/* ========== SIZE ========== */

int cmd_size(const char *db_root, const char *object) {
    int count = get_live_count(db_root, object);
    int deleted = get_deleted_count(db_root, object);
    if (deleted > 0)
        OUT("{\"count\":%d,\"orphaned\":%d}\n", count, deleted);
    else
        OUT("{\"count\":%d}\n", count);
    return 0;
}

/* ========== FIELD DECODE ========== */

void init_field_schema(FieldSchema *fs, const char *db_root, const char *object) {
    fs->ts = load_typed_schema(db_root, object);
    fs->nfields = 0;
}

/* Decode stored value to JSON. */
char *decode_value(const char *raw, size_t raw_len, FieldSchema *fs) {
    return typed_decode(fs->ts, (const uint8_t *)raw, raw_len);
}

/* Extract a field value from stored data as string.
   Returns malloc'd string. Caller must free. */
char *decode_field(const char *raw, size_t raw_len, const char *field, FieldSchema *fs) {
    if (fs && fs->ts) {
        /* Typed binary: handle composite fields */
        if (strchr(field, '+')) {
            char fb[256]; strncpy(fb, field, 255); fb[255] = '\0';
            char cat[4096]; int cp = 0;
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok) {
                int idx = typed_field_index(fs->ts, tok);
                if (idx >= 0) {
                    char *v = typed_get_field_str(fs->ts, (const uint8_t *)raw, idx);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            return cp > 0 ? strdup(cat) : NULL;
        }
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, idx);
    }
    return NULL;
}

/* ========== Compiled criteria & Joins structs (definitions below) ========== */

enum JoinType { JOIN_INNER = 0, JOIN_LEFT = 1 };

typedef struct {
    char object[256];
    char local_field[256];
    char remote_field[256];       /* "key" OR indexed field name */
    char as_name[256];
    int  type;                    /* JOIN_INNER | JOIN_LEFT */

    char proj_fields[MAX_FIELDS][256];
    int  proj_count;              /* 0 filled by resolver if no explicit fields list */
    int  include_remote_key;      /* user requested "key" in fields array */

    /* Resolved at parse time */
    Schema       remote_sch;
    FieldSchema  remote_fs;
    int          remote_is_key;
    char         remote_idx_path[PATH_MAX];

    /* Pre-resolved TypedFields for projection (NULL → composite/unknown fallback) */
    const TypedField *proj_tfs[MAX_FIELDS];

    /* Driver-side local field (NULL iff composite or unknown) */
    const TypedField *local_tf;
    int local_is_composite;
} JoinSpec;

enum LikeKind { LK_EXACT, LK_PREFIX, LK_CONTAINS };

struct CompiledCriterion {
    const TypedField *tf;       /* resolved; NULL iff composite or unknown */
    enum SearchOp op;
    enum FieldType ftype;       /* cached when tf != NULL */
    int composite;              /* 1 if field name contains '+' */
    const SearchCriterion *raw; /* kept for fallback path + OP_IN varchar */

    /* Pre-parsed scalar rvalues (interpretation depends on ftype) */
    int64_t  i1, i2;            /* LONG/INT/SHORT/NUMERIC: native value.
                                   DATE: 4-byte BE int32 date value.
                                   DATETIME: i1=date int32, i2=unused */
    double   d1, d2;            /* DOUBLE */
    uint16_t t1, t2;            /* DATETIME packed time seconds */
    uint8_t  b1;                /* BOOL/BYTE */

    /* Varchar + LIKE rvalues (case-insensitive needles pre-lowered) */
    char    *s1;                /* c->value: raw copy (for eq/between) */
    size_t   s1_len;
    char    *s2;                /* c->value2 */
    size_t   s2_len;
    char    *needle_lc;         /* lowercased s1 for LIKE/CONTAINS/STARTS/ENDS */
    size_t   needle_len;
    enum LikeKind like_kind;

    /* IN/NOT_IN lists (pre-parsed for numeric types) */
    int64_t  *in_i64;
    double   *in_f64;
    int       in_count;
};

/* Fetch a record by its hex hash and print as JSON. Returns 1 if found. */
int fetch_record_by_hash(const char *db_root, const char *object,
                                const Schema *sch, const uint8_t hash16[16], int *printed,
                                void *fs) {
    int r_shard_id, r_slot;
    addr_from_hash(hash16, sch->splits, &r_shard_id, &r_slot);
    char r_shard[PATH_MAX];
    build_shard_path(r_shard, sizeof(r_shard), db_root, object, r_shard_id);

    FcacheRead fc = fcache_get_read(r_shard);
    if (!fc.map) return 0;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    int found = 0;
    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)r_slot + p) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash16, 16) != 0) continue;

        char *rkey = malloc(h->key_len + 1);
        memcpy(rkey, fc.map + zoneB_off(s, slots, sch->slot_size), h->key_len);
        rkey[h->key_len] = '\0';
        const char *raw = (const char *)(fc.map + zoneB_off(s, slots, sch->slot_size) + h->key_len);
        char *rval = decode_value(raw, h->value_len, fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}", *printed ? "," : "", rkey, rval);
        free(rkey); free(rval);
        (*printed)++;
        found = 1;
        break;
    }
    fcache_release(fc);
    return found;
}

/* ========== Parallel fetch: collect hashes → group by shard → parallel fetch ========== */

/* Shared hash entry for collected B+ tree results (used by find) */
typedef struct {
    uint8_t hash[16];
    int shard_id;
    int start_slot;
} CollectedHash;

static int cmp_by_shard(const void *a, const void *b) {
    return ((const CollectedHash *)a)->shard_id - ((const CollectedHash *)b)->shard_id;
}

/* Collecting callback for B+ tree traversal */
typedef struct {
    CollectedHash *entries;
    size_t count;
    size_t cap;
    int splits;
    size_t limit;   /* max results to collect, 0 = unlimited */
} SearchCollectCtx;

static int search_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    SearchCollectCtx *sc = (SearchCollectCtx *)ctx;
    if (sc->limit > 0 && sc->count >= sc->limit) return 1; /* stop */
    if (sc->count >= sc->cap) {
        sc->cap *= 2;
        sc->entries = realloc(sc->entries, sc->cap * sizeof(CollectedHash));
    }
    CollectedHash *e = &sc->entries[sc->count++];
    memcpy(e->hash, hash16, 16);
    addr_from_hash(hash16, sc->splits, &e->shard_id, &e->start_slot);
    return 0;
}

/* Per-shard fetch worker (no secondary filter, just decode + output) */
#define SEARCH_MODE_FULL  0  /* key + value JSON */
#define SEARCH_MODE_KEYS  1  /* keys only */

typedef struct {
    char *key;
    char *value;  /* NULL for keys-only mode */
} SearchResult;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    FieldSchema *fs;
    int mode;
    SearchResult *results;
    int result_count;
} SearchShardWork;

static void *search_shard_worker(void *arg) {
    SearchShardWork *sw = (SearchShardWork *)arg;
    if (sw->entry_count == 0) return NULL;

    int sid = sw->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, sid);

    /* Use fcache to avoid re-mmapping the same shard file across multiple searches */
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sw->entry_count; ei++) {
        CollectedHash *e = &sw->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;

            SearchResult *r = &sw->results[sw->result_count++];
            r->key = malloc(h->key_len + 1);
            memcpy(r->key, fc.map + zoneB_off(s, slots, sw->sch->slot_size), h->key_len);
            r->key[h->key_len] = '\0';

            if (sw->mode == SEARCH_MODE_FULL) {
                const char *raw = (const char *)(fc.map + zoneB_off(s, slots, sw->sch->slot_size) + h->key_len);
                r->value = decode_value(raw, h->value_len, sw->fs);
            } else {
                r->value = NULL;
            }
            break;
        }
    }
    fcache_release(fc);
    return NULL;
}

/* Parallel fetch: collect hashes, group by shard, parallel shard reads, output */
static int parallel_fetch_and_print(const char *db_root, const char *object,
                                    const Schema *sch, CollectedHash *entries, size_t count,
                                    FieldSchema *fs, int mode) {
    if (count == 0) return 0;

    /* For small result sets, sequential is faster (no thread overhead) */
    if (count <= 64 && mode == SEARCH_MODE_FULL) {
        int printed = 0;
        for (size_t i = 0; i < count; i++)
            fetch_record_by_hash(db_root, object, sch, entries[i].hash, &printed, fs);
        return printed;
    }

    /* Sort by shard_id */
    qsort(entries, count, sizeof(CollectedHash), cmp_by_shard);

    /* Group by shard */
    int nshard_groups = 0;
    int *group_starts = malloc((count + 1) * sizeof(int));
    int *group_sizes = malloc((count + 1) * sizeof(int));
    int prev_sid = -1;
    for (size_t i = 0; i < count; i++) {
        if (entries[i].shard_id != prev_sid) {
            group_starts[nshard_groups] = i;
            if (nshard_groups > 0) group_sizes[nshard_groups - 1] = i - group_starts[nshard_groups - 1];
            prev_sid = entries[i].shard_id;
            nshard_groups++;
        }
    }
    if (nshard_groups > 0) group_sizes[nshard_groups - 1] = count - group_starts[nshard_groups - 1];

    /* Allocate per-shard workers */
    SearchShardWork *workers = calloc(nshard_groups, sizeof(SearchShardWork));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &entries[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].fs = fs;
        workers[g].mode = mode;
        workers[g].results = malloc(group_sizes[g] * sizeof(SearchResult));
        workers[g].result_count = 0;
    }

    parallel_for(search_shard_worker, workers, nshard_groups, sizeof(SearchShardWork));

    /* Output results */
    int printed = 0;
    for (int g = 0; g < nshard_groups; g++) {
        for (int r = 0; r < workers[g].result_count; r++) {
            SearchResult *sr = &workers[g].results[r];
            if (mode == SEARCH_MODE_FULL)
                OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", sr->key, sr->value);
            else
                OUT("%s\"%s\"", printed ? "," : "", sr->key);
            printed++;
            free(sr->key);
            free(sr->value);
        }
        free(workers[g].results);
    }
    free(workers);
    free(group_starts);
    free(group_sizes);
    return printed;
}

/* ========== BULK INSERT ========== */

/* Bulk ops use ucache (unified shard cache in storage.c) */

/* ---- Shared worker types for parallel activation and index builds ---- */
typedef struct { UCacheEntry *e; int slot_size; } ActivateArg;
static void *activate_worker(void *arg) {
    ActivateArg *a = (ActivateArg *)arg;
    pthread_rwlock_wrlock(&a->e->rwlock);
    if (a->e->map) {
        uint32_t shard_slots = a->e->slots_per_shard;
        uint32_t activated = 0;
        size_t slot_bits_cap = (shard_slots + 7) / 8;
        if (a->e->slot_bits && a->e->max_dirty_slot >= 0) {
            /* Fast path: only visit slots recorded in the bitmap. */
            size_t max_byte = (size_t)a->e->max_dirty_slot / 8;
            if (max_byte >= slot_bits_cap) max_byte = slot_bits_cap - 1;
            for (size_t b = 0; b <= max_byte; b++) {
                uint8_t bv = a->e->slot_bits[b];
                if (!bv) continue;
                for (int bit = 0; bit < 8; bit++) {
                    if (!(bv & (1 << bit))) continue;
                    uint32_t s = (uint32_t)(b * 8 + bit);
                    if (s >= shard_slots) continue;
                    SlotHeader *h = (SlotHeader *)(a->e->map + zoneA_off(s));
                    if (h->flag == 0 && h->key_len > 0) { h->flag = 1; activated++; }
                }
            }
            memset(a->e->slot_bits, 0, slot_bits_cap);
            a->e->max_dirty_slot = -1;
        } else {
            /* Fallback: full Zone A scan (used if slot_bits not allocated) */
            for (uint32_t s = 0; s < shard_slots; s++) {
                SlotHeader *h = (SlotHeader *)(a->e->map + zoneA_off(s));
                if (h->flag == 0 && h->key_len > 0) { h->flag = 1; activated++; }
            }
        }
        if (activated > 0) {
            ShardHeader *sh = (ShardHeader *)a->e->map;
            if (sh->magic == SHARD_MAGIC) sh->record_count += activated;
        }
    }
    a->e->dirty = 0;
    pthread_rwlock_unlock(&a->e->rwlock);
    return NULL;
}

typedef struct { char ipath[PATH_MAX]; BtEntry *pairs; size_t pair_count; } IdxBuildArg;
static void *idx_build_worker(void *arg) {
    IdxBuildArg *ib = (IdxBuildArg *)arg;
    /* Merge-rebuild: sort new entries, merge with existing tree, rebuild from scratch.
       Much faster than btree_insert_batch for large datasets because it uses sequential
       I/O (leaf scan + bulk_build) instead of random B+ tree insertions. */
    btree_bulk_merge(ib->ipath, ib->pairs, ib->pair_count);
    return NULL;
}

/* ---- Fast bulk insert using mmap ---- */

/* Bump/arena allocator for phase-1 record buffers. Replaces 2 mallocs per
   record (id + typed payload) with O(1) pointer-advance into pre-allocated
   slabs. Freed as a whole after phase-2 workers join — arena pointers must
   not outlive arena_free(). Chain grows by doubling when the current slab
   fills; initial slab is 256 KB so tiny inputs don't over-reserve and big
   inputs reach ~1 GB in ~12 doublings (≪ the malloc count avoided). */
typedef struct BulkArena {
    struct BulkArena *next;
    uint8_t *base;
    size_t used;
    size_t cap;
} BulkArena;

static BulkArena *arena_new(size_t cap) {
    BulkArena *a = malloc(sizeof(BulkArena));
    if (!a) return NULL;
    a->base = malloc(cap);
    if (!a->base) { free(a); return NULL; }
    a->next = NULL;
    a->used = 0;
    a->cap = cap;
    return a;
}

static void *arena_alloc(BulkArena **head, size_t n) {
    n = (n + 7) & ~(size_t)7;  /* 8-byte align */
    BulkArena *a = *head;
    if (a->used + n > a->cap) {
        size_t new_cap = a->cap * 2;
        if (n > new_cap) new_cap = n;
        BulkArena *na = arena_new(new_cap);
        if (!na) return NULL;
        na->next = a;
        *head = na;
        a = na;
    }
    void *p = a->base + a->used;
    a->used += n;
    return p;
}

static char *arena_strndup(BulkArena **head, const char *src, size_t n) {
    char *p = arena_alloc(head, n + 1);
    if (!p) return NULL;
    memcpy(p, src, n);
    p[n] = '\0';
    return p;
}

static void arena_free(BulkArena *a) {
    while (a) {
        BulkArena *next = a->next;
        free(a->base);
        free(a);
        a = next;
    }
}

/* Per-record buffered state collected in phase 1 (parse) and consumed in
   phase 2 (write). `id` and `payload` are pointers into a BulkArena owned
   by the caller of phase-1; records stay valid until arena_free() runs,
   which happens after all phase-2 workers have joined. */
typedef struct {
    char     *id;           /* arena-owned null-terminated key */
    uint8_t  *payload;      /* arena-owned typed payload (ts->total_size bytes) */
    size_t    klen;
    uint8_t   hash[16];
    int       start_slot;
    int       shard_id;
} BulkInsRecord;

/* Per-shard bucket + worker arguments. Each bucket targets exactly one
   shard so the worker can take the ucache wrlock **once**, write every
   record in the bucket, and release **once** — avoiding per-record
   acquire/release churn. Idx entries are collected into per-worker arrays
   and merged into the caller's global arrays after the worker returns
   (same shape bulk-delete's bulk_del_shard_worker uses). */
typedef struct {
    const char     *db_root;
    const char     *object;
    const Schema   *sch;
    const TypedSchema *ts;
    int             shard_id;
    /* Per-record data — all records target sw->shard_id */
    BulkInsRecord  *records;
    size_t          count;
    /* Index metadata (read-only, shared across workers) */
    int             nidx;
    const char    (*idx_fields)[256];
    const int     (*idx_field_indices)[16];
    const int      *idx_field_counts;
    const int      *idx_is_composite;
    /* Results (written by worker) */
    int             inserted;   /* new keys — updates do NOT increment */
    int             errors;
    /* Per-worker index entry collection; merged post-phase into caller's
       global arrays. Each BtEntry.value is an owned malloc'd string. */
    BtEntry       **idx_pairs;        /* [nidx] */
    size_t         *idx_pair_counts;  /* [nidx] */
    size_t         *idx_pair_caps;    /* [nidx] */
} BulkInsShardWork;

/* Probe + write every record in one shard's bucket under a single ucache
   wrlock held from start to finish. On shard-full, release the lock, grow
   the shard, reacquire, and retry the **same** record index — avoids
   per-record churn. Collects index entries into sw->idx_pairs for later
   merge/bulk-build. pthread-compatible signature: workers in different
   shard buckets never touch each other's shards, so the wrlocks are
   disjoint and no cross-worker coordination is needed. */
static void *bulk_insert_shard_worker(void *arg) {
    BulkInsShardWork *sw = (BulkInsShardWork *)arg;
    if (sw->count == 0) return NULL;

    char shard_path[PATH_MAX];
    build_shard_path(shard_path, sizeof(shard_path), sw->db_root, sw->object, sw->shard_id);

    FcacheRead wh = ucache_get_write(shard_path, sw->sch->slot_size, sw->sch->prealloc_mb);
    if (!wh.map) {
        log_msg(1, "INSERT_DROP shard=%d (cannot open shard, dropping %zu records)",
                sw->shard_id, sw->count);
        sw->errors += (int)sw->count;
        return NULL;
    }

    size_t i = 0;
    while (i < sw->count && wh.map) {
        BulkInsRecord *r = &sw->records[i];
        uint8_t *map = wh.map;
        UCacheEntry *e = ucache_entry(wh.slot);
        uint32_t slots = wh.slots_per_shard;
        uint32_t mask = slots - 1;

        /* Probe for slot */
        int slot = -1, first_tomb = -1;
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)r->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) { slot = (int)s; break; }
            if (h->flag == 2) { if (first_tomb < 0) first_tomb = (int)s; continue; }
            if (memcmp(h->hash, r->hash, 16) == 0 &&
                h->key_len == r->klen &&
                memcmp(map + zoneB_off(s, slots, sw->sch->slot_size), r->id, r->klen) == 0) {
                slot = (int)s; break;
            }
        }
        if (slot < 0 && first_tomb >= 0) slot = first_tomb;

        if (slot < 0) {
            /* Shard full — release lock so grow can take it, grow, reacquire, retry */
            ucache_write_release(wh);
            if (ucache_grow_shard(shard_path, sw->sch->slot_size, sw->sch->prealloc_mb) < 0) {
                /* All remaining records in this bucket fail */
                for (size_t j = i; j < sw->count; j++)
                    log_msg(1, "INSERT_DROP shard=%d key=%s (shard full: cannot grow)",
                            sw->shard_id, sw->records[j].id);
                sw->errors += (int)(sw->count - i);
                wh.map = NULL;
                break;
            }
            wh = ucache_get_write(shard_path, sw->sch->slot_size, sw->sch->prealloc_mb);
            if (!wh.map) {
                /* Grow succeeded but reacquire failed — e.g. EMFILE hit on open.
                   Count the remaining records as errors so the caller doesn't
                   silently lose them. */
                log_msg(1, "INSERT_DROP shard=%d (reacquire after grow failed, dropping %zu records)",
                        sw->shard_id, sw->count - i);
                sw->errors += (int)(sw->count - i);
                break;
            }
            continue; /* re-probe same record index */
        }

        /* Write header + payload */
        SlotHeader *existing = (SlotHeader *)(map + zoneA_off(slot));
        int is_update = (existing->flag == 1 &&
                         memcmp(existing->hash, r->hash, 16) == 0);
        SlotHeader *hdr = (SlotHeader *)(map + zoneA_off(slot));
        memset(hdr, 0, HEADER_SIZE);
        memcpy(hdr->hash, r->hash, 16);
        hdr->flag = 0;
        hdr->key_len = (uint16_t)r->klen;
        hdr->value_len = (uint32_t)sw->ts->total_size;
        uint8_t *payload = map + zoneB_off(slot, slots, sw->sch->slot_size);
        memcpy(payload, r->id, r->klen);
        memcpy(payload + r->klen, r->payload, sw->ts->total_size);
        e->dirty = 1;

        if ((uint32_t)slot < slots) {
            size_t bits_cap = (slots + 7) / 8;
            if (!e->slot_bits) e->slot_bits = calloc(bits_cap, 1);
            if (e->slot_bits) e->slot_bits[slot / 8] |= (uint8_t)(1 << (slot % 8));
            if (slot > e->max_dirty_slot) e->max_dirty_slot = slot;
        }

        if (!is_update) sw->inserted++;

        /* Collect index entries from the typed payload */
        for (int fi = 0; fi < sw->nidx; fi++) {
            char *idx_val = NULL;
            if (sw->idx_is_composite[fi]) {
                char cat[4096]; int cp = 0; int ok = 1;
                for (int si = 0; si < sw->idx_field_counts[fi]; si++) {
                    int tidx = sw->idx_field_indices[fi][si];
                    char *v = (tidx >= 0) ? typed_get_field_str(sw->ts, r->payload, tidx) : NULL;
                    if (!v || !v[0]) { ok = 0; free(v); break; }
                    int sl = strlen(v);
                    if (cp + sl < (int)sizeof(cat)) { memcpy(cat + cp, v, sl); cp += sl; }
                    free(v);
                }
                cat[cp] = '\0';
                if (ok && cp > 0) {
                    idx_val = malloc(cp + 1);
                    memcpy(idx_val, cat, cp);
                    idx_val[cp] = '\0';
                }
            } else {
                int tidx = sw->idx_field_indices[fi][0];
                if (tidx >= 0) idx_val = typed_get_field_str(sw->ts, r->payload, tidx);
            }
            if (idx_val && idx_val[0]) {
                if (sw->idx_pair_counts[fi] >= sw->idx_pair_caps[fi]) {
                    sw->idx_pair_caps[fi] = sw->idx_pair_caps[fi] ? sw->idx_pair_caps[fi] * 2 : 64;
                    sw->idx_pairs[fi] = realloc(sw->idx_pairs[fi],
                                                sw->idx_pair_caps[fi] * sizeof(BtEntry));
                }
                BtEntry *bp = &sw->idx_pairs[fi][sw->idx_pair_counts[fi]++];
                bp->value = idx_val;
                bp->vlen  = strlen(idx_val);
                memcpy(bp->hash, r->hash, 16);
            } else {
                free(idx_val);
            }
        }

        i++;
    }

    if (wh.map) ucache_write_release(wh);
    return NULL;
}

/* Internal: bulk insert from a json string already in memory (no file I/O) */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str);

int cmd_bulk_insert(const char *db_root, const char *object, const char *input) {
    size_t len;
    char *json;
    int json_mmaped = 0;
    if (input) {
        /* mmap the file instead of malloc — OS pages in/out as we scan */
        int ifd = open(input, O_RDONLY);
        if (ifd < 0) { fprintf(stderr, "Error: Cannot open %s\n", input); return 1; }
        struct stat st;
        fstat(ifd, &st);
        len = st.st_size;
        if (len == 0) { close(ifd); fprintf(stderr, "Error: Empty input\n"); return 1; }
        json = mmap(NULL, len + 1, PROT_READ | PROT_WRITE, MAP_PRIVATE, ifd, 0);
        if (json == MAP_FAILED) {
            /* Fallback: allocate and read */
            json = malloc(len + 1);
            if (!json) { close(ifd); fprintf(stderr, "Error: Cannot allocate\n"); return 1; }
            lseek(ifd, 0, SEEK_SET);
            size_t rd = 0;
            while (rd < len) {
                ssize_t n = read(ifd, json + rd, len - rd);
                if (n <= 0) break;
                rd += n;
            }
            json[len] = '\0';
        } else {
            json_mmaped = 1;
            madvise(json, len, MADV_SEQUENTIAL);
            /* Null-terminate — MAP_PRIVATE so write is COW on last page only */
            json[len] = '\0';
        }
        close(ifd);
    } else {
        size_t cap = 65536, pos = 0;
        json = malloc(cap);
        int c;
        while ((c = fgetc(stdin)) != EOF) {
            if (pos >= cap - 1) { cap *= 2; json = realloc(json, cap); }
            json[pos++] = c;
        }
        json[pos] = '\0'; len = pos;
    }
    if (!json) { fprintf(stderr, "Error: Cannot read input\n"); return 1; }

    /* Load config ONCE */
    Schema sc = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    const char *field_ptrs[MAX_FIELDS];
    for (int i = 0; i < nfields; i++) field_ptrs[i] = idx_fields[i];

    TypedSchema *ts = load_typed_schema(db_root, object);
    uint8_t *typed_tmp = ts ? malloc(ts->total_size) : NULL;

    /* ucache handles shard caching */

    /* Pre-allocate BtEntry collectors for bulk B+ tree build at end */
    BtEntry **idx_pairs = calloc(nfields, sizeof(BtEntry *));
    size_t *idx_pair_counts = calloc(nfields, sizeof(size_t));
    size_t *idx_pair_caps = calloc(nfields, sizeof(size_t));
    for (int i = 0; i < nfields; i++) {
        idx_pair_caps[i] = 4096;
        idx_pairs[i] = malloc(idx_pair_caps[i] * sizeof(BtEntry));
    }

    /* Pre-resolve index field indices so the per-record loop doesn't re-scan
       the schema for every record × every index. Saves O(records × indexes ×
       nfields) strcmps on large bulk inserts. Composite indexes: up to 16
       sub-fields each. */
    int idx_field_indices[MAX_FIELDS][16];
    int idx_field_counts[MAX_FIELDS];
    int idx_is_composite[MAX_FIELDS];
    for (int fi = 0; fi < nfields; fi++) {
        idx_is_composite[fi] = (strchr(idx_fields[fi], '+') != NULL);
        idx_field_counts[fi] = 0;
        if (idx_is_composite[fi]) {
            char fb[256]; strncpy(fb, idx_fields[fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && idx_field_counts[fi] < 16) {
                idx_field_indices[fi][idx_field_counts[fi]++] =
                    ts ? typed_field_index(ts, tok) : -1;
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            idx_field_indices[fi][0] = ts ? typed_field_index(ts, idx_fields[fi]) : -1;
            idx_field_counts[fi] = 1;
        }
    }

    /* ===== Phase 1: parse with yyjson (SIMD, ~10-15x faster than the
       hand-rolled walker on invoice-shaped JSON). One doc for the whole
       file; per-record work is tree traversal via yyjson_arr_iter /
       yyjson_obj_iter. Supported inputs:
         - Array:  [{"id":"k1","data":{...}}, ...]
         - Object: {"k1":{...},"k2":{...}}  (key = id, value = data) */
    yyjson_doc *doc = yyjson_read(json, len, 0);
    if (!doc) {
        fprintf(stderr, "Error: malformed JSON (yyjson parse failed)\n");
        if (json_mmaped) munmap(json, len + 1); else free(json);
        free(typed_tmp);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        return 1;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    int is_array_format = yyjson_is_arr(root);
    int is_object_format = yyjson_is_obj(root);
    if (!is_array_format && !is_object_format) {
        fprintf(stderr, "Error: Expected JSON object or array\n");
        yyjson_doc_free(doc);
        if (json_mmaped) munmap(json, len + 1); else free(json);
        free(typed_tmp);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        return 1;
    }

    int count = 0, errors = 0;

    /* Per-record `id` + `payload` bump-allocated from one arena. */
    BulkArena *arena = arena_new(256 * 1024);
    size_t rec_cap = 1024, rec_count = 0;
    BulkInsRecord *records = malloc(rec_cap * sizeof(BulkInsRecord));

    /* Shared lambda: consume one (id_val, data_val) pair into records[]. */
    #define EMIT_RECORD(id_str, id_len, data_val) do {                       \
        if ((id_str) && (data_val)) {                                        \
            typed_encode_defaults_yy(ts, (data_val), typed_tmp,              \
                                     ts->total_size, db_root, object);       \
            if ((int)(id_len) > sc.max_key ||                                \
                (int)ts->total_size > sc.max_value) {                        \
                errors++;                                                    \
            } else {                                                         \
                char *id_copy = arena_strndup(&arena, (id_str), (id_len));   \
                uint8_t *payload = arena_alloc(&arena, ts->total_size);      \
                memcpy(payload, typed_tmp, ts->total_size);                  \
                if (rec_count >= rec_cap) {                                  \
                    rec_cap *= 2;                                            \
                    records = realloc(records,                               \
                                      rec_cap * sizeof(BulkInsRecord));      \
                }                                                            \
                BulkInsRecord *r = &records[rec_count++];                    \
                r->id = id_copy;                                             \
                r->payload = payload;                                        \
                r->klen = (id_len);                                          \
                compute_addr(id_copy, (id_len), sc.splits,                   \
                             r->hash, &r->shard_id, &r->start_slot);         \
            }                                                                \
        }                                                                    \
    } while (0)

    if (is_array_format) {
        yyjson_arr_iter arr_it = yyjson_arr_iter_with(root);
        yyjson_val *rec;
        while ((rec = yyjson_arr_iter_next(&arr_it))) {
            if (!yyjson_is_obj(rec)) continue;
            yyjson_val *idv = yyjson_obj_get(rec, "id");
            yyjson_val *datav = yyjson_obj_get(rec, "data");
            const char *id_str = idv ? yyjson_get_str(idv) : NULL;
            size_t id_len = idv ? yyjson_get_len(idv) : 0;
            EMIT_RECORD(id_str, id_len, datav);
        }
    } else {
        /* Object format: outer keys are ids, values are data objects. */
        yyjson_obj_iter obj_it = yyjson_obj_iter_with(root);
        yyjson_val *key;
        while ((key = yyjson_obj_iter_next(&obj_it))) {
            yyjson_val *datav = yyjson_obj_iter_get_val(key);
            const char *id_str = yyjson_get_str(key);
            size_t id_len = yyjson_get_len(key);
            EMIT_RECORD(id_str, id_len, datav);
        }
    }
    #undef EMIT_RECORD
    yyjson_doc_free(doc);

    /* ===== Phase 1.5: bucket records by shard_id so each worker owns one shard's
       writes and can hold the ucache wrlock once for the entire bucket. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].shard_id = s;
                workers[g].records = malloc(shard_counts[s] * sizeof(BulkInsRecord));
                workers[g].count = 0;
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        workers[w].records[workers[w].count++] = records[i];  /* shallow copy, ownership transferred */
    }
    free(records);
    free(shard_counts);
    free(shard_to_worker);

    /* Wire read-only worker context + allocate per-worker idx-entry collectors. */
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        ws->db_root = db_root;
        ws->object = object;
        ws->sch = &sc;
        ws->ts = ts;
        ws->nidx = nfields;
        ws->idx_fields = (const char (*)[256])idx_fields;
        ws->idx_field_indices = (const int (*)[16])idx_field_indices;
        ws->idx_field_counts = idx_field_counts;
        ws->idx_is_composite = idx_is_composite;
        ws->inserted = 0;
        ws->errors = 0;
        ws->idx_pairs = nfields > 0 ? calloc(nfields, sizeof(BtEntry *)) : NULL;
        ws->idx_pair_counts = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
        ws->idx_pair_caps = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
    }

    /* ===== Phase 2: run shard workers in parallel. Each worker owns one shard's
       writes so ucache wrlocks are disjoint across workers — no cross-worker
       coordination needed. Batched pthread_create/join pattern matches
       bulk_del_shard_worker. Serial fallback when thread count ≤ 1 or workload
       is small enough that spawn/join overhead would dominate. */
    parallel_for(bulk_insert_shard_worker, workers, nshard_groups,
                 sizeof(BulkInsShardWork));

    /* Merge per-worker results into the caller's global counters and index arrays,
       then release per-worker scratch. BtEntry.value ownership transfers into
       the global idx_pairs — freed later by the idx-build cleanup. */
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;

        for (int fi = 0; fi < nfields; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                while (idx_pair_counts[fi] + add > idx_pair_caps[fi]) idx_pair_caps[fi] *= 2;
                idx_pairs[fi] = realloc(idx_pairs[fi], idx_pair_caps[fi] * sizeof(BtEntry));
            }
            memcpy(idx_pairs[fi] + idx_pair_counts[fi],
                   ws->idx_pairs[fi], add * sizeof(BtEntry));
            idx_pair_counts[fi] += add;
            free(ws->idx_pairs[fi]);
        }

        /* records[ri].id / .payload live in the arena — freed in bulk below */
        free(ws->records);
        free(ws->idx_pairs);
        free(ws->idx_pair_counts);
        free(ws->idx_pair_caps);
    }
    free(workers);
    arena_free(arena);

    /* Activate all dirty shards for THIS object — filter by path prefix */
    {
        char obj_prefix[PATH_MAX];
        snprintf(obj_prefix, sizeof(obj_prefix), "%s/%s/data/", db_root, object);
        size_t prefix_len = strlen(obj_prefix);

        /* Scan ucache for dirty entries matching this object */
        int ucap = ucache_slot_count();
        ActivateArg *act_args = malloc(ucap * sizeof(ActivateArg));
        int act_count = 0;
        for (int i = 0; i < ucap; i++) {
            UCacheEntry *e = ucache_entry(i);
            if (!e || !e->used || !e->dirty) continue;
            if (strncmp(e->path, obj_prefix, prefix_len) != 0) continue;
            act_args[act_count].e = e;
            act_args[act_count].slot_size = sc.slot_size;
            act_count++;
        }
        parallel_for(activate_worker, act_args, act_count, sizeof(ActivateArg));
        free(act_args);
    }

    /* ucache keeps mmaps open — OS flushes dirty pages */
    if (json_mmaped) munmap(json, len + 1);  /* len+1 matches mmap size */
    else free(json);
    free(typed_tmp);

    /* Bulk write indexes — parallel across index fields */
    if (nfields > 0) {
        IdxBuildArg *ib_args = malloc(nfields * sizeof(IdxBuildArg));
        int ib_count = 0;
        for (int fi = 0; fi < nfields; fi++) {
            if (idx_pair_counts[fi] == 0) { free(idx_pairs[fi]); continue; }
            snprintf(ib_args[ib_count].ipath, sizeof(ib_args[ib_count].ipath),
                     "%s/%s/indexes/%s.idx", db_root, object, idx_fields[fi]);
            mkdirp(dirname_of(ib_args[ib_count].ipath));
            ib_args[ib_count].pairs = idx_pairs[fi];
            ib_args[ib_count].pair_count = idx_pair_counts[fi];
            ib_count++;
        }

        parallel_for(idx_build_worker, ib_args, ib_count, sizeof(IdxBuildArg));

        for (int i = 0; i < ib_count; i++) {
            for (size_t ei = 0; ei < ib_args[i].pair_count; ei++)
                free((char *)ib_args[i].pairs[ei].value);
            free(ib_args[i].pairs);
        }
        free(ib_args);
    }
    free(idx_pairs);
    free(idx_pair_counts);
    free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);

    if (errors) {
        OUT("{\"count\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n", count, errors);
        fprintf(stderr, "%d errors during bulk insert (see info log for dropped keys)\n", errors);
    } else {
        OUT("{\"count\":%d}\n", count);
    }
    return errors > 0 ? 1 : 0;
}

/* Bulk insert from a string already in memory — no temp file needed */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str) {
    /* Write to a memfd (in-memory file) so cmd_bulk_insert can mmap it */
    size_t slen = strlen(json_str);
    int memfd = memfd_create("shard-db_bulk", 0);
    if (memfd < 0) {
        /* Fallback: temp file */
        char tmp[PATH_MAX];
        snprintf(tmp, sizeof(tmp), "/tmp/shard-db_bulk_%d_%d.json", getpid(), (int)pthread_self());
        FILE *tf = fopen(tmp, "w");
        if (tf) { fwrite(json_str, 1, slen, tf); fclose(tf); }
        int r = cmd_bulk_insert(db_root, object, tmp);
        unlink(tmp);
        return r;
    }
    ftruncate(memfd, slen);
    write(memfd, json_str, slen);
    char fdpath[64];
    snprintf(fdpath, sizeof(fdpath), "/proc/self/fd/%d", memfd);
    int r = cmd_bulk_insert(db_root, object, fdpath);
    close(memfd);
    return r;
}

/* ========== BULK INSERT (DELIMITED TEXT FILE) ========== */

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter) {
    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }

    /* Must have typed schema — delimited values map to fields.conf order */
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Delimited import requires typed fields (fields.conf)\"}\n");
        return 1;
    }

    Schema sc = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);

    /* mmap the file */
    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st; fstat(ifd, &st);
    if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
    const char *data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, ifd, 0);
    int data_mmaped = 1;
    if (data == MAP_FAILED) {
        char *buf = malloc(st.st_size);
        if (!buf) { close(ifd); return 1; }
        lseek(ifd, 0, SEEK_SET);
        size_t rd = 0;
        while (rd < (size_t)st.st_size) {
            ssize_t n = read(ifd, buf + rd, st.st_size - rd);
            if (n <= 0) break;
            rd += n;
        }
        data = buf;
        data_mmaped = 0;
    } else {
        madvise((void *)data, st.st_size, MADV_SEQUENTIAL);
    }
    close(ifd);

    /* Init */
    uint8_t *typed_tmp = malloc(ts->total_size);

    BtEntry **idx_pairs = calloc(nidx, sizeof(BtEntry *));
    size_t *idx_pair_counts = calloc(nidx, sizeof(size_t));
    size_t *idx_pair_caps = calloc(nidx, sizeof(size_t));
    for (int i = 0; i < nidx; i++) {
        idx_pair_caps[i] = 4096;
        idx_pairs[i] = malloc(idx_pair_caps[i] * sizeof(BtEntry));
    }

    /* Pre-resolve index field indices */
    int idx_field_indices[MAX_FIELDS][16]; /* sub-field indices for each index */
    int idx_field_counts[MAX_FIELDS];
    int idx_is_composite[MAX_FIELDS];
    for (int fi = 0; fi < nidx; fi++) {
        idx_is_composite[fi] = (strchr(idx_fields[fi], '+') != NULL);
        idx_field_counts[fi] = 0;
        if (idx_is_composite[fi]) {
            char fb[256]; strncpy(fb, idx_fields[fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && idx_field_counts[fi] < 16) {
                idx_field_indices[fi][idx_field_counts[fi]++] = typed_field_index(ts, tok);
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            idx_field_indices[fi][0] = typed_field_index(ts, idx_fields[fi]);
            idx_field_counts[fi] = 1;
        }
    }

    int count = 0, errors = 0;
    const char *rp = data;                /* read pointer — never written to */
    const char *data_end = data + st.st_size;

    /* Compute active-field mapping once — tombstone set is fixed for the
       lifetime of this bulk insert. Common case (no tombstones) hits the
       fast path that matches direct positional encoding. */
    int active_indices[MAX_FIELDS];
    int active_count = 0;
    int has_tombstones = 0;
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) has_tombstones = 1;
        else active_indices[active_count++] = i;
    }

    /* Length-based parsing: we work directly against the mmap'd page cache
       with (ptr, len) spans and never copy the line body. This removes the
       ~100 B-per-line memcpy that dominated memory bandwidth under parallel
       load (multiple workers each churning ~1 GB of memcpy on a 10 M-record
       CSV). encode_field_len / memcpy-with-length handle the null-terminator-
       free parse on the numeric / varchar / index sides. */
    struct { const char *ptr; size_t len; } vals[MAX_FIELDS];

    /* ===== Phase 1: parse every CSV line into a buffered records[] array. The
       (ptr, len) span path stays zero-copy for the CSV body; `id` + typed
       `payload` come from a single BulkArena (bump alloc, 8-byte aligned) so
       the 2-mallocs-per-record cost disappears. Arena freed after Phase-2
       workers join. */
    BulkArena *arena = arena_new(256 * 1024);
    size_t rec_cap = 1024, rec_count = 0;
    BulkInsRecord *records = malloc(rec_cap * sizeof(BulkInsRecord));

    while (rp < data_end) {
        /* Find end of line without modifying the buffer */
        const char *eol = rp;
        while (eol < data_end && *eol != '\n' && *eol != '\r') eol++;
        size_t line_len = eol - rp;
        if (line_len == 0) { /* empty line */
            rp = eol + 1;
            if (rp < data_end && *(rp - 1) == '\r' && *rp == '\n') rp++;
            continue;
        }

        const char *line_start = rp;
        const char *line_end   = eol;

        /* Advance read pointer past this line (consume \r\n / \n / \r). */
        rp = eol;
        if (rp < data_end && *rp == '\r') rp++;
        if (rp < data_end && *rp == '\n') rp++;

        /* First field is the key; remaining fields in fields.conf order. */
        const char *key_end = line_start;
        while (key_end < line_end && *key_end != delimiter) key_end++;
        if (key_end == line_end) continue;  /* no delimiter — skip line */
        const char *id_start = line_start;
        size_t klen = key_end - line_start;

        /* Walk remaining spans into vals[] without copying. */
        memset(typed_tmp, 0, ts->total_size);
        int nvals = 0;
        const char *vp = key_end + 1;
        while (nvals < active_count) {
            const char *v_start = vp;
            while (vp < line_end && *vp != delimiter) vp++;
            vals[nvals].ptr = v_start;
            vals[nvals].len = vp - v_start;
            nvals++;
            if (vp < line_end) vp++;  /* skip delimiter */
            else if (nvals < active_count) {
                /* Line has fewer values than schema expects — pad the
                   remainder with empty spans (legacy behaviour). */
                while (nvals < active_count) {
                    vals[nvals].ptr = line_end;
                    vals[nvals].len = 0;
                    nvals++;
                }
            }
        }

        if (!has_tombstones) {
            for (int i = 0; i < active_count && i < nvals; i++) {
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,
                                     typed_tmp + ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < active_count && i < nvals; i++) {
                int fi = active_indices[i];
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                     typed_tmp + ts->fields[fi].offset);
            }
        }

        if ((int)klen > sc.max_key || ts->total_size > sc.max_value) {
            errors++;
            continue;
        }

        /* Arena-allocated key + typed payload — survive until arena_free() post-Phase-2. */
        char *id = arena_strndup(&arena, id_start, klen);
        uint8_t *payload = arena_alloc(&arena, ts->total_size);
        memcpy(payload, typed_tmp, ts->total_size);

        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            records = realloc(records, rec_cap * sizeof(BulkInsRecord));
        }
        BulkInsRecord *r = &records[rec_count++];
        r->id = id;
        r->payload = payload;
        r->klen = klen;
        compute_addr(id, klen, sc.splits, r->hash, &r->shard_id, &r->start_slot);
    }

    /* ===== Phase 1.5: bucket by shard — identical to the JSON path. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].shard_id = s;
                workers[g].records = malloc(shard_counts[s] * sizeof(BulkInsRecord));
                workers[g].count = 0;
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        workers[w].records[workers[w].count++] = records[i];
    }
    free(records);
    free(shard_counts);
    free(shard_to_worker);

    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        ws->db_root = db_root;
        ws->object = object;
        ws->sch = &sc;
        ws->ts = ts;
        ws->nidx = nidx;
        ws->idx_fields = (const char (*)[256])idx_fields;
        ws->idx_field_indices = (const int (*)[16])idx_field_indices;
        ws->idx_field_counts = idx_field_counts;
        ws->idx_is_composite = idx_is_composite;
        ws->inserted = 0;
        ws->errors = 0;
        ws->idx_pairs = nidx > 0 ? calloc(nidx, sizeof(BtEntry *)) : NULL;
        ws->idx_pair_counts = nidx > 0 ? calloc(nidx, sizeof(size_t)) : NULL;
        ws->idx_pair_caps = nidx > 0 ? calloc(nidx, sizeof(size_t)) : NULL;
    }

    /* ===== Phase 2: parallel shard workers via shared pool.
       All concurrent callers share one pool sized to ~4× cores by default;
       oversubscription hides shard-rwlock stalls. */
    parallel_for(bulk_insert_shard_worker, workers, nshard_groups,
                 sizeof(BulkInsShardWork));

    /* Merge per-worker results into caller's counters + index arrays. */
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;

        for (int fi = 0; fi < nidx; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                while (idx_pair_counts[fi] + add > idx_pair_caps[fi]) idx_pair_caps[fi] *= 2;
                idx_pairs[fi] = realloc(idx_pairs[fi], idx_pair_caps[fi] * sizeof(BtEntry));
            }
            memcpy(idx_pairs[fi] + idx_pair_counts[fi],
                   ws->idx_pairs[fi], add * sizeof(BtEntry));
            idx_pair_counts[fi] += add;
            free(ws->idx_pairs[fi]);
        }

        /* records[ri].id / .payload live in the arena — freed in bulk below */
        free(ws->records);
        free(ws->idx_pairs);
        free(ws->idx_pair_counts);
        free(ws->idx_pair_caps);
    }
    free(workers);
    arena_free(arena);

    /* Activate — parallel across shards for THIS object */
    {
        char obj_prefix[PATH_MAX];
        snprintf(obj_prefix, sizeof(obj_prefix), "%s/%s/data/", db_root, object);
        size_t prefix_len = strlen(obj_prefix);

        int ucap = ucache_slot_count();
        ActivateArg *act_args = malloc(ucap * sizeof(ActivateArg));
        int act_count = 0;
        for (int i = 0; i < ucap; i++) {
            UCacheEntry *e = ucache_entry(i);
            if (!e || !e->used || !e->dirty) continue;
            if (strncmp(e->path, obj_prefix, prefix_len) != 0) continue;
            act_args[act_count].e = e;
            act_args[act_count].slot_size = sc.slot_size;
            act_count++;
        }
        parallel_for(activate_worker, act_args, act_count, sizeof(ActivateArg));
        free(act_args);
    }

    if (data_mmaped) munmap((void *)data, st.st_size);
    else free((void *)data);
    free(typed_tmp);

    /* Parallel index builds — uses file-scope idx_build_worker */
    if (nidx > 0) {
        IdxBuildArg *ib_args = malloc(nidx * sizeof(IdxBuildArg));
        int ib_count = 0;
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_pair_counts[fi] == 0) { free(idx_pairs[fi]); continue; }
            snprintf(ib_args[ib_count].ipath, PATH_MAX, "%s/%s/indexes/%s.idx", db_root, object, idx_fields[fi]);
            mkdirp(dirname_of(ib_args[ib_count].ipath));
            ib_args[ib_count].pairs = idx_pairs[fi];
            ib_args[ib_count].pair_count = idx_pair_counts[fi];
            ib_count++;
        }
        parallel_for(idx_build_worker, ib_args, ib_count, sizeof(IdxBuildArg));
        for (int i = 0; i < ib_count; i++) {
            for (size_t ei = 0; ei < ib_args[i].pair_count; ei++) free((char *)ib_args[i].pairs[ei].value);
            free(ib_args[i].pairs);
        }
        free(ib_args);
    }
    free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);
    if (errors) {
        OUT("{\"count\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n", count, errors);
        fprintf(stderr, "%d errors during delimited import (see info log for dropped keys)\n", errors);
    } else {
        OUT("{\"count\":%d}\n", count);
    }
    return errors > 0 ? 1 : 0;
}

/* ========== BULK DELETE ========== */

/* Per-shard bulk delete worker */
typedef struct {
    const char *db_root;
    const char *object;
    Schema *sch;
    /* Keys for this shard */
    char **keys;
    uint8_t (*hashes)[16];
    int *shard_slots;
    int key_count;
    /* Index info */
    char (*idx_fields)[256];
    int nidx;
    TypedSchema *ts;
    /* Results */
    int deleted;
    /* Collected index deletions: [nidx][key_count] */
    char ***idx_vals; /* idx_vals[idx_i][key_j] = field value or NULL */
} BulkDelShardWork;

static void *bulk_del_shard_worker(void *arg) {
    BulkDelShardWork *sw = (BulkDelShardWork *)arg;
    if (sw->key_count == 0) return NULL;

    int sid = sw->shard_slots[0]; /* all keys in this worker share shard_id */
    /* Actually shard_slots stores start_slot, we need shard_id from first key's hash */
    int shard_id, dummy;
    addr_from_hash(sw->hashes[0], sw->sch->splits, &shard_id, &dummy);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, shard_id);
    int fd = open(shard, O_RDWR);
    if (fd < 0) return NULL;
    flock(fd, LOCK_EX);
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { flock(fd, LOCK_UN); close(fd); return NULL; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { flock(fd, LOCK_UN); close(fd); return NULL; }

    ShardHeader *sh = (ShardHeader *)map;
    if ((size_t)st.st_size < SHARD_HDR_SIZE || sh->magic != SHARD_MAGIC) {
        munmap(map, st.st_size); flock(fd, LOCK_UN); close(fd); return NULL;
    }
    uint32_t slots = sh->slots_per_shard;
    uint32_t mask = slots - 1;
    uint32_t tombstoned_here = 0;

    for (int ki = 0; ki < sw->key_count; ki++) {
        int start_slot = sw->shard_slots[ki];
        size_t klen = strlen(sw->keys[ki]);

        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, sw->hashes[ki], 16) == 0 &&
                h->key_len == klen &&
                memcmp(map + zoneB_off(s, slots, sw->sch->slot_size), sw->keys[ki], klen) == 0) {

                /* Extract index values before tombstoning */
                if (sw->nidx > 0 && sw->ts) {
                    const uint8_t *raw = map + zoneB_off(s, slots, sw->sch->slot_size) + h->key_len;
                    for (int fi = 0; fi < sw->nidx; fi++) {
                        if (strchr(sw->idx_fields[fi], '+')) {
                            char fb[256]; strncpy(fb, sw->idx_fields[fi], 255); fb[255] = '\0';
                            char cat[4096]; int cp = 0; int ok = 1;
                            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                            while (tok) {
                                int fidx = typed_field_index(sw->ts, tok);
                                if (fidx >= 0) {
                                    char *v = typed_get_field_str(sw->ts, raw, fidx);
                                    if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                                    else { ok = 0; break; }
                                } else { ok = 0; break; }
                                tok = strtok_r(NULL, "+", &_tok_save);
                            }
                            cat[cp] = '\0';
                            sw->idx_vals[fi][ki] = (ok && cp > 0) ? strdup(cat) : NULL;
                        } else {
                            int fidx = typed_field_index(sw->ts, sw->idx_fields[fi]);
                            sw->idx_vals[fi][ki] = typed_get_field_str(sw->ts, raw, fidx);
                        }
                    }
                }

                h->flag = 2; /* tombstone */
                sw->deleted++;
                tombstoned_here++;
                break;
            }
        }
    }

    if (tombstoned_here > 0) {
        sh->record_count = (sh->record_count > tombstoned_here)
            ? sh->record_count - tombstoned_here : 0;
    }
    munmap(map, st.st_size);
    flock(fd, LOCK_UN);
    close(fd);
    return NULL;
}

/* Per-index bulk delete worker — deletes all collected values from one index */
typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    char **vals;
    uint8_t (*hashes)[16];
    int count;
} BulkDelIdxWork;

static void *bulk_del_idx_worker(void *arg) {
    BulkDelIdxWork *iw = (BulkDelIdxWork *)arg;
    for (int i = 0; i < iw->count; i++) {
        if (iw->vals[i] && iw->vals[i][0])
            delete_index_entry(iw->db_root, iw->object, iw->field, iw->vals[i], iw->hashes[i]);
    }
    return NULL;
}

int cmd_bulk_delete(const char *db_root, const char *object, const char *input) {
    size_t len;
    char *raw;
    if (input) {
        raw = read_file(input, &len);
    } else {
        size_t cap = 65536, pos = 0;
        raw = malloc(cap);
        int c;
        while ((c = fgetc(stdin)) != EOF) {
            if (pos >= cap - 1) { cap *= 2; raw = realloc(raw, cap); }
            raw[pos++] = c;
        }
        raw[pos] = '\0'; len = pos;
    }
    if (!raw) { fprintf(stderr, "Error: Cannot read input\n"); return 1; }

    /* Parse all keys */
    char **keys = NULL;
    int key_count = 0, key_cap = 1024;
    keys = malloc(key_cap * sizeof(char *));

    const char *p = json_skip(raw);
    if (*p == '[') {
        p++;
        while (*p) {
            p = json_skip(p);
            if (*p == ']') break;
            if (*p == ',') { p++; continue; }
            if (*p == '"') {
                p++;
                const char *start = p;
                while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
                size_t klen = p - start;
                if (key_count >= key_cap) { key_cap *= 2; keys = realloc(keys, key_cap * sizeof(char *)); }
                keys[key_count] = malloc(klen + 1);
                memcpy(keys[key_count], start, klen);
                keys[key_count][klen] = '\0';
                key_count++;
                if (*p == '"') p++;
            } else p++;
        }
    } else {
        char *_line_save = NULL; char *line = strtok_r(raw, "\n", &_line_save);
        while (line) {
            if (line[0] != '\0') {
                if (key_count >= key_cap) { key_cap *= 2; keys = realloc(keys, key_cap * sizeof(char *)); }
                keys[key_count++] = strdup(line);
            }
            line = strtok_r(NULL, "\n", &_line_save);
        }
    }

    if (key_count == 0) { free(keys); free(raw); return 0; }

    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Compute hashes and group by shard */
    uint8_t (*hashes)[16] = malloc(key_count * sizeof(uint8_t[16]));
    int *shard_ids = malloc(key_count * sizeof(int));
    int *start_slots = malloc(key_count * sizeof(int));
    int *order = malloc(key_count * sizeof(int)); /* original index for regrouping */

    for (int i = 0; i < key_count; i++) {
        compute_addr(keys[i], strlen(keys[i]), sch.splits, hashes[i], &shard_ids[i], &start_slots[i]);
        order[i] = i;
    }

    /* Sort by shard_id */
    /* Simple insertion sort (stable, fine for <100K keys) */
    for (int i = 1; i < key_count; i++) {
        int j = i;
        while (j > 0 && shard_ids[order[j-1]] > shard_ids[order[j]]) {
            int tmp = order[j]; order[j] = order[j-1]; order[j-1] = tmp;
            j--;
        }
    }

    /* Group by shard */
    int nshard_groups = 0;
    int group_starts[4096], group_counts[4096]; /* max shard groups */
    int prev_sid = -1;
    for (int i = 0; i < key_count && nshard_groups < 4096; i++) {
        int si = shard_ids[order[i]];
        if (si != prev_sid) {
            group_starts[nshard_groups] = i;
            if (nshard_groups > 0) group_counts[nshard_groups-1] = i - group_starts[nshard_groups-1];
            prev_sid = si;
            nshard_groups++;
        }
    }
    if (nshard_groups > 0) group_counts[nshard_groups-1] = key_count - group_starts[nshard_groups-1];

    /* Build per-shard workers */
    BulkDelShardWork *workers = calloc(nshard_groups, sizeof(BulkDelShardWork));
    for (int g = 0; g < nshard_groups; g++) {
        int cnt = group_counts[g];
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = &sch;
        workers[g].keys = malloc(cnt * sizeof(char *));
        workers[g].hashes = malloc(cnt * sizeof(uint8_t[16]));
        workers[g].shard_slots = malloc(cnt * sizeof(int));
        workers[g].key_count = cnt;
        workers[g].idx_fields = idx_fields;
        workers[g].nidx = nidx;
        workers[g].ts = ts;
        workers[g].deleted = 0;
        /* Allocate idx_vals */
        workers[g].idx_vals = calloc(nidx, sizeof(char **));
        for (int fi = 0; fi < nidx; fi++)
            workers[g].idx_vals[fi] = calloc(cnt, sizeof(char *));

        for (int i = 0; i < cnt; i++) {
            int oi = order[group_starts[g] + i];
            workers[g].keys[i] = keys[oi];
            memcpy(workers[g].hashes[i], hashes[oi], 16);
            workers[g].shard_slots[i] = start_slots[oi];
        }
    }

    /* Phase 1: Parallel shard tombstoning */
    parallel_for(bulk_del_shard_worker, workers, nshard_groups, sizeof(BulkDelShardWork));

    /* Phase 2: Parallel index cleanup — one thread per index */
    int total_deleted = 0;
    for (int g = 0; g < nshard_groups; g++) total_deleted += workers[g].deleted;

    if (nidx > 0 && total_deleted > 0) {
        /* Flatten idx_vals across all shard groups: for each index, collect all (val, hash) pairs */
        BulkDelIdxWork *idx_workers = malloc(nidx * sizeof(BulkDelIdxWork));
        for (int fi = 0; fi < nidx; fi++) {
            idx_workers[fi].db_root = db_root;
            idx_workers[fi].object = object;
            idx_workers[fi].field = idx_fields[fi];
            idx_workers[fi].vals = malloc(key_count * sizeof(char *));
            idx_workers[fi].hashes = malloc(key_count * sizeof(uint8_t[16]));
            idx_workers[fi].count = 0;
            for (int g = 0; g < nshard_groups; g++) {
                for (int ki = 0; ki < workers[g].key_count; ki++) {
                    if (workers[g].idx_vals[fi][ki]) {
                        int c = idx_workers[fi].count;
                        idx_workers[fi].vals[c] = workers[g].idx_vals[fi][ki];
                        memcpy(idx_workers[fi].hashes[c], workers[g].hashes[ki], 16);
                        idx_workers[fi].count++;
                    }
                }
            }
        }

        parallel_for(bulk_del_idx_worker, idx_workers, nidx, sizeof(BulkDelIdxWork));

        for (int fi = 0; fi < nidx; fi++) {
            for (int i = 0; i < idx_workers[fi].count; i++) free(idx_workers[fi].vals[i]);
            free(idx_workers[fi].vals);
            free(idx_workers[fi].hashes);
        }
        free(idx_workers);
    }

    /* Cleanup workers */
    for (int g = 0; g < nshard_groups; g++) {
        free(workers[g].keys);
        free(workers[g].hashes);
        free(workers[g].shard_slots);
        for (int fi = 0; fi < nidx; fi++) free(workers[g].idx_vals[fi]);
        free(workers[g].idx_vals);
    }
    free(workers);

    if (total_deleted > 0) {
        update_count(db_root, object, -total_deleted);
        update_deleted_count(db_root, object, total_deleted);
    }

    OUT("{\"deleted\":%d}\n", total_deleted);
    for (int i = 0; i < key_count; i++) free(keys[i]);
    free(keys); free(hashes); free(shard_ids); free(start_slots); free(order); free(raw);
    return 0;
}

/* ========== BULK-UPDATE / BULK-DELETE WITH CRITERIA ========== */

/* Scan callback: collect matching keys for bulk-update/delete */
typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    /* Collected keys */
    char **keys;
    int count;
    int cap;
    int limit;
    QueryDeadline *deadline;
    int dl_counter;
    size_t buffer_bytes;    /* running total for QUERY_BUFFER_MB cap */
    int budget_exceeded;
} BulkCriteriaCtx;

static int bulk_criteria_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)ctx;
    if (bc->budget_exceeded) return 1;
    if (bc->limit > 0 && bc->count >= bc->limit) return 1;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 1;

    const uint8_t *raw = block + hdr->key_len;

    if (!criteria_match_tree(raw, bc->tree, bc->fs)) return 0;

    size_t key_bytes = sizeof(char *) + hdr->key_len + 1;
    if (bc->buffer_bytes + key_bytes > g_query_buffer_max_bytes) {
        bc->budget_exceeded = 1;
        return 1;
    }
    bc->buffer_bytes += key_bytes;

    /* Match — collect the key */
    if (bc->count >= bc->cap) {
        bc->cap = bc->cap ? bc->cap * 2 : 1024;
        bc->keys = realloc(bc->keys, bc->cap * sizeof(char *));
    }
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';
    bc->keys[bc->count++] = key;
    return 0;
}

/* Per-record state for bulk-update's Phase 2 shard-grouped workers. */
typedef struct {
    const char *key;
    size_t      klen;
    uint8_t     hash[16];
    int         start_slot;
    int         shard_id;
} BulkUpdRec;

typedef struct {
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    TypedSchema   *ts;
    CriteriaNode  *tree;
    FieldSchema   *fs;
    const char    *value_json;
    const char   (*idx_fields)[256];
    int            nidx;
    int            shard_id;
    BulkUpdRec    *recs;
    int            count;
    /* Results */
    int            updated;
    int            skipped;
} BulkUpdShardWork;

/* Bulk-update phase 2 worker — one per shard, holds the ucache wrlock once
   for the whole bucket. Index updates (btree_insert/btree_delete) are
   serialised by bt_cache_lock inside the btree layer, so concurrent workers
   hitting the same index file are safe. */
static void *bulk_upd_shard_worker(void *arg) {
    BulkUpdShardWork *w = (BulkUpdShardWork *)arg;
    if (w->count == 0) return NULL;

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), w->db_root, w->object, w->shard_id);
    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) { w->skipped += w->count; return NULL; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdRec *rec = &w->recs[ki];
        int slot = -1;
        for (uint32_t si = 0; si < slots; si++) {
            uint32_t s = ((uint32_t)rec->start_slot + si) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, rec->hash, 16) == 0 &&
                h->key_len == rec->klen &&
                memcmp(map + zoneB_off(s, slots, w->sch->slot_size), rec->key, rec->klen) == 0) {
                slot = (int)s; break;
            }
        }
        if (slot < 0) { w->skipped++; continue; }

        SlotHeader *hdr = (SlotHeader *)(map + zoneA_off(slot));
        uint8_t *value_ptr = map + zoneB_off(slot, slots, w->sch->slot_size) + hdr->key_len;

        if (!criteria_match_tree(value_ptr, w->tree, w->fs)) { w->skipped++; continue; }

        /* Collect old index values */
        char *old_idx_vals[MAX_FIELDS];
        memset(old_idx_vals, 0, sizeof(old_idx_vals));
        for (int fi = 0; fi < w->nidx; fi++) {
            if (strchr(w->idx_fields[fi], '+')) {
                char fb[256]; strncpy(fb, w->idx_fields[fi], 255); fb[255] = '\0';
                char cat[4096]; int cp = 0; int ok = 1;
                char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                while (tok) {
                    int fidx = typed_field_index(w->ts, tok);
                    if (fidx >= 0) {
                        char *v = typed_get_field_str(w->ts, value_ptr, fidx);
                        if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                        else { ok = 0; break; }
                    } else { ok = 0; break; }
                    tok = strtok_r(NULL, "+", &_tok_save);
                }
                cat[cp] = '\0';
                old_idx_vals[fi] = (ok && cp > 0) ? strdup(cat) : NULL;
            } else {
                int fidx = typed_field_index(w->ts, w->idx_fields[fi]);
                old_idx_vals[fi] = typed_get_field_str(w->ts, value_ptr, fidx);
            }
        }

        /* Apply partial update */
        const char *field_names[MAX_FIELDS];
        char *field_vals[MAX_FIELDS];
        for (int fi = 0; fi < w->ts->nfields; fi++) field_names[fi] = w->ts->fields[fi].name;
        json_get_fields(w->value_json, field_names, w->ts->nfields, field_vals);

        for (int fi = 0; fi < w->ts->nfields; fi++) {
            if (field_vals[fi]) {
                if (!w->ts->fields[fi].removed)
                    encode_field(&w->ts->fields[fi], field_vals[fi], value_ptr + w->ts->fields[fi].offset);
                free(field_vals[fi]);
            }
        }

        /* auto_update fields */
        for (int fi = 0; fi < w->ts->nfields; fi++) {
            if (w->ts->fields[fi].removed) continue;
            if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
                char tbuf[20];
                time_t now = time(NULL);
                struct tm tm;
                localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec);
                encode_field(&w->ts->fields[fi], tbuf, value_ptr + w->ts->fields[fi].offset);
            }
        }

        /* Update indexes for changed values */
        if (w->nidx > 0) {
            uint8_t *new_val = map + zoneB_off(slot, slots, w->sch->slot_size) + rec->klen;
            for (int fi = 0; fi < w->nidx; fi++) {
                char *new_v = NULL;
                if (strchr(w->idx_fields[fi], '+')) {
                    char fb[256]; strncpy(fb, w->idx_fields[fi], 255); fb[255] = '\0';
                    char cat[4096]; int cp = 0; int ok = 1;
                    char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                    while (tok) {
                        int fidx = typed_field_index(w->ts, tok);
                        if (fidx >= 0) {
                            char *v = typed_get_field_str(w->ts, new_val, fidx);
                            if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                            else { ok = 0; break; }
                        } else { ok = 0; break; }
                        tok = strtok_r(NULL, "+", &_tok_save);
                    }
                    cat[cp] = '\0';
                    if (ok && cp > 0) new_v = strdup(cat);
                } else {
                    int fidx = typed_field_index(w->ts, w->idx_fields[fi]);
                    new_v = typed_get_field_str(w->ts, new_val, fidx);
                }
                int changed = 0;
                if (!old_idx_vals[fi] && new_v) changed = 1;
                else if (old_idx_vals[fi] && !new_v) changed = 1;
                else if (old_idx_vals[fi] && new_v && strcmp(old_idx_vals[fi], new_v) != 0) changed = 1;
                if (changed) {
                    if (old_idx_vals[fi]) delete_index_entry(w->db_root, w->object, w->idx_fields[fi], old_idx_vals[fi], rec->hash);
                    if (new_v) write_index_entry(w->db_root, w->object, w->idx_fields[fi], new_v, rec->hash);
                }
                free(old_idx_vals[fi]);
                free(new_v);
            }
        }

        w->updated++;
    }

    ucache_write_release(wh);
    return NULL;
}

int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        OUT("{\"error\":\"Missing criteria\"}\n");
        return 1;
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0 };
    scan_shards(data_dir, sch.slot_size, bulk_criteria_scan_cb, &ctx);
    int matched = ctx.count;

    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"updated\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — bucket matched keys by shard and fan out one worker
       per shard. Each worker takes the ucache wrlock **once** per shard,
       walks its bucket end-to-end, and releases **once** — matching the
       bulk-insert pattern. Index updates (btree_insert/btree_delete) are
       serialised internally by bt_cache_lock, so concurrent workers are
       safe. */
    TypedSchema *ts = load_typed_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    int updated = 0, skipped = 0;

    /* Pre-compute each matched key's hash + shard placement so bucketing
       is a single pass over ctx.keys[]. */
    BulkUpdRec *all = matched > 0 ? malloc(matched * sizeof(BulkUpdRec)) : NULL;
    for (int i = 0; i < matched; i++) {
        all[i].key = ctx.keys[i];
        all[i].klen = strlen(ctx.keys[i]);
        compute_addr(all[i].key, all[i].klen, sch.splits,
                     all[i].hash, &all[i].shard_id, &all[i].start_slot);
    }

    /* Bucket by shard_id. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    for (int i = 0; i < matched; i++) shard_counts[all[i].shard_id]++;
    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdShardWork)) : NULL;
    int *shard_to_worker = malloc(sch.splits * sizeof(int));
    {
        int g = 0;
        for (int s = 0; s < sch.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].shard_id = s;
                workers[g].recs = malloc(shard_counts[s] * sizeof(BulkUpdRec));
                workers[g].count = 0;
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < matched; i++) {
        int w = shard_to_worker[all[i].shard_id];
        workers[w].recs[workers[w].count++] = all[i];
    }
    free(shard_counts);
    free(shard_to_worker);
    free(all);

    for (int wi = 0; wi < nshard_groups; wi++) {
        workers[wi].db_root = db_root;
        workers[wi].object = object;
        workers[wi].sch = &sch;
        workers[wi].ts = ts;
        workers[wi].tree = tree;
        workers[wi].fs = &fs;
        workers[wi].value_json = value_json;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
        workers[wi].nidx = nidx;
        workers[wi].updated = 0;
        workers[wi].skipped = 0;
    }

    parallel_for(bulk_upd_shard_worker, workers, nshard_groups, sizeof(BulkUpdShardWork));

    for (int wi = 0; wi < nshard_groups; wi++) {
        updated += workers[wi].updated;
        skipped += workers[wi].skipped;
        free(workers[wi].recs);
    }
    free(workers);

    log_msg(3, "BULK-UPDATE %s matched=%d updated=%d skipped=%d", object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);

    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}

/* ========== BULK UPDATE (DELIMITED TEXT FILE) ========== */
/* Per-key partial update. Row shape: key<DELIM>v1<DELIM>v2<DELIM>... in
   fields.conf active-field order (same as bulk-insert-delimited). Semantics
   per 9a spec: update-only (key must exist — missing keys counted as
   skipped); blank cell = leave that field alone; non-blank cell overwrites.
   NOT an upsert. Phase 1 parses rows into records[] with line-span pointers
   into the mmap'd CSV; Phase 2 runs shard-grouped parallel workers that
   probe, patch, and update affected indexes under a single wrlock per
   shard. Indexes are updated only when their value actually changed. */
typedef struct {
    char       *key;           /* owned null-terminated */
    size_t      klen;
    uint8_t     hash[16];
    int         start_slot;
    int         shard_id;
    const char *line_end;      /* points into mmap'd CSV; valid until munmap */
    const char *body_start;    /* span after key_end + delimiter */
} BulkUpdDelimRec;

typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    TypedSchema      *ts;
    const char      (*idx_fields)[256];
    int               nidx;
    const int        *active_indices;
    int               active_count;
    int               has_tombstones;
    char              delimiter;
    int               shard_id;
    BulkUpdDelimRec  *recs;
    int               count;
    /* Results */
    int               updated;
    int               skipped;
} BulkUpdDelimShardWork;

static void *bulk_upd_delim_shard_worker(void *arg) {
    BulkUpdDelimShardWork *w = (BulkUpdDelimShardWork *)arg;
    if (w->count == 0) return NULL;

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), w->db_root, w->object, w->shard_id);
    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) { w->skipped += w->count; return NULL; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    struct { const char *ptr; size_t len; } vals[MAX_FIELDS];

    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdDelimRec *rec = &w->recs[ki];

        /* Probe for the record — must already exist */
        int slot = -1;
        for (uint32_t si = 0; si < slots; si++) {
            uint32_t s = ((uint32_t)rec->start_slot + si) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, rec->hash, 16) == 0 &&
                h->key_len == rec->klen &&
                memcmp(map + zoneB_off(s, slots, w->sch->slot_size), rec->key, rec->klen) == 0) {
                slot = (int)s; break;
            }
        }
        if (slot < 0) { w->skipped++; continue; }

        SlotHeader *hdr = (SlotHeader *)(map + zoneA_off(slot));
        uint8_t *value_ptr = map + zoneB_off(slot, slots, w->sch->slot_size) + hdr->key_len;

        /* Re-parse field spans from the mmap'd CSV line. Cheaper than buffering
           all spans in phase 1 (N*M*16 bytes for invoice-size schemas). */
        int nvals = 0;
        const char *vp = rec->body_start;
        const char *line_end = rec->line_end;
        while (nvals < w->active_count) {
            const char *v_start = vp;
            while (vp < line_end && *vp != w->delimiter) vp++;
            vals[nvals].ptr = v_start;
            vals[nvals].len = vp - v_start;
            nvals++;
            if (vp < line_end) vp++;
            else if (nvals < w->active_count) {
                while (nvals < w->active_count) {
                    vals[nvals].ptr = line_end;
                    vals[nvals].len = 0;
                    nvals++;
                }
            }
        }

        /* Capture old index values before patching. */
        char *old_idx_vals[MAX_FIELDS];
        memset(old_idx_vals, 0, sizeof(old_idx_vals));
        for (int fi = 0; fi < w->nidx; fi++) {
            if (strchr(w->idx_fields[fi], '+')) {
                char fb[256]; strncpy(fb, w->idx_fields[fi], 255); fb[255] = '\0';
                char cat[4096]; int cp = 0; int ok = 1;
                char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                while (tok) {
                    int fidx = typed_field_index(w->ts, tok);
                    if (fidx >= 0) {
                        char *v = typed_get_field_str(w->ts, value_ptr, fidx);
                        if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                        else { ok = 0; break; }
                    } else { ok = 0; break; }
                    tok = strtok_r(NULL, "+", &_tok_save);
                }
                cat[cp] = '\0';
                old_idx_vals[fi] = (ok && cp > 0) ? strdup(cat) : NULL;
            } else {
                int fidx = typed_field_index(w->ts, w->idx_fields[fi]);
                old_idx_vals[fi] = typed_get_field_str(w->ts, value_ptr, fidx);
            }
        }

        /* Patch each non-blank field into the typed payload at its fixed offset.
           Blank cells are intentionally left untouched — 9a "update-only, blank
           cell leaves alone" semantics. */
        if (!w->has_tombstones) {
            for (int i = 0; i < w->active_count && i < nvals; i++) {
                if (vals[i].len > 0)
                    encode_field_len(&w->ts->fields[i], vals[i].ptr, vals[i].len,
                                     value_ptr + w->ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < w->active_count && i < nvals; i++) {
                int fi = w->active_indices[i];
                if (vals[i].len > 0)
                    encode_field_len(&w->ts->fields[fi], vals[i].ptr, vals[i].len,
                                     value_ptr + w->ts->fields[fi].offset);
            }
        }

        /* auto_update fields — refresh timestamp on every successful update */
        for (int fi = 0; fi < w->ts->nfields; fi++) {
            if (w->ts->fields[fi].removed) continue;
            if (w->ts->fields[fi].default_kind == DK_AUTO_UPDATE) {
                char tbuf[20];
                time_t now = time(NULL);
                struct tm tm;
                localtime_r(&now, &tm);
                if (w->ts->fields[fi].type == FT_DATE)
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
                else
                    snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                             tm.tm_hour, tm.tm_min, tm.tm_sec);
                encode_field(&w->ts->fields[fi], tbuf, value_ptr + w->ts->fields[fi].offset);
            }
        }

        /* Compare old vs new index values and apply delta only where changed.
           This is the "idx_changed_bitmap" optimisation from 9a — a blank CSV
           cell on an indexed field leaves the field unchanged and therefore
           does zero B+ tree work for that index. */
        for (int fi = 0; fi < w->nidx; fi++) {
            char *new_v = NULL;
            if (strchr(w->idx_fields[fi], '+')) {
                char fb[256]; strncpy(fb, w->idx_fields[fi], 255); fb[255] = '\0';
                char cat[4096]; int cp = 0; int ok = 1;
                char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                while (tok) {
                    int fidx = typed_field_index(w->ts, tok);
                    if (fidx >= 0) {
                        char *v = typed_get_field_str(w->ts, value_ptr, fidx);
                        if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                        else { ok = 0; break; }
                    } else { ok = 0; break; }
                    tok = strtok_r(NULL, "+", &_tok_save);
                }
                cat[cp] = '\0';
                if (ok && cp > 0) new_v = strdup(cat);
            } else {
                int fidx = typed_field_index(w->ts, w->idx_fields[fi]);
                new_v = typed_get_field_str(w->ts, value_ptr, fidx);
            }
            int changed = 0;
            if (!old_idx_vals[fi] && new_v) changed = 1;
            else if (old_idx_vals[fi] && !new_v) changed = 1;
            else if (old_idx_vals[fi] && new_v && strcmp(old_idx_vals[fi], new_v) != 0) changed = 1;
            if (changed) {
                if (old_idx_vals[fi]) delete_index_entry(w->db_root, w->object, w->idx_fields[fi], old_idx_vals[fi], rec->hash);
                if (new_v) write_index_entry(w->db_root, w->object, w->idx_fields[fi], new_v, rec->hash);
            }
            free(old_idx_vals[fi]);
            free(new_v);
        }

        w->updated++;
    }

    ucache_write_release(wh);
    return NULL;
}

int cmd_bulk_update_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter) {
    if (!filepath) { OUT("{\"error\":\"file is required\"}\n"); return 1; }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Delimited update requires typed fields (fields.conf)\"}\n");
        return 1;
    }

    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);

    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st; fstat(ifd, &st);
    if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
    const char *data = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, ifd, 0);
    int data_mmaped = 1;
    if (data == MAP_FAILED) {
        char *buf = malloc(st.st_size);
        if (!buf) { close(ifd); return 1; }
        lseek(ifd, 0, SEEK_SET);
        size_t rd = 0;
        while (rd < (size_t)st.st_size) {
            ssize_t n = read(ifd, buf + rd, st.st_size - rd);
            if (n <= 0) break;
            rd += n;
        }
        data = buf;
        data_mmaped = 0;
    } else {
        madvise((void *)data, st.st_size, MADV_SEQUENTIAL);
    }
    close(ifd);

    /* Active-field mapping — same as bulk-insert-delimited. */
    int active_indices[MAX_FIELDS];
    int active_count = 0;
    int has_tombstones = 0;
    for (int i = 0; i < ts->nfields; i++) {
        if (ts->fields[i].removed) has_tombstones = 1;
        else active_indices[active_count++] = i;
    }

    /* ===== Phase 1: parse every CSV row into records[]. Each record carries
       an owned key and a (body_start, line_end) span into the mmap'd CSV so
       phase 2 can re-scan the field values without buffering every span. */
    int matched = 0, skipped = 0;
    size_t rec_cap = 1024, rec_count = 0;
    BulkUpdDelimRec *records = malloc(rec_cap * sizeof(BulkUpdDelimRec));
    const char *rp = data;
    const char *data_end = data + st.st_size;

    while (rp < data_end) {
        const char *eol = rp;
        while (eol < data_end && *eol != '\n' && *eol != '\r') eol++;
        if (eol == rp) {
            rp = eol + 1;
            if (rp < data_end && *(rp - 1) == '\r' && *rp == '\n') rp++;
            continue;
        }
        const char *line_start = rp;
        const char *line_end   = eol;
        rp = eol;
        if (rp < data_end && *rp == '\r') rp++;
        if (rp < data_end && *rp == '\n') rp++;

        const char *key_end = line_start;
        while (key_end < line_end && *key_end != delimiter) key_end++;
        if (key_end == line_end) continue;

        size_t klen = key_end - line_start;
        if ((int)klen > sch.max_key) { skipped++; continue; }

        matched++;

        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            records = realloc(records, rec_cap * sizeof(BulkUpdDelimRec));
        }
        BulkUpdDelimRec *r = &records[rec_count++];
        r->key = malloc(klen + 1);
        memcpy(r->key, line_start, klen); r->key[klen] = '\0';
        r->klen = klen;
        r->body_start = key_end + 1;
        r->line_end   = line_end;
        compute_addr(r->key, klen, sch.splits, r->hash, &r->shard_id, &r->start_slot);
    }

    /* ===== Phase 1.5: bucket by shard_id. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;
    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdDelimShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdDelimShardWork)) : NULL;
    int *shard_to_worker = malloc(sch.splits * sizeof(int));
    {
        int g = 0;
        for (int s = 0; s < sch.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].shard_id = s;
                workers[g].recs = malloc(shard_counts[s] * sizeof(BulkUpdDelimRec));
                workers[g].count = 0;
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        workers[w].recs[workers[w].count++] = records[i];
    }
    free(records);
    free(shard_counts);
    free(shard_to_worker);

    for (int wi = 0; wi < nshard_groups; wi++) {
        workers[wi].db_root = db_root;
        workers[wi].object = object;
        workers[wi].sch = &sch;
        workers[wi].ts = ts;
        workers[wi].idx_fields = (const char (*)[256])idx_fields;
        workers[wi].nidx = nidx;
        workers[wi].active_indices = active_indices;
        workers[wi].active_count = active_count;
        workers[wi].has_tombstones = has_tombstones;
        workers[wi].delimiter = delimiter;
        workers[wi].updated = 0;
        workers[wi].skipped = 0;
    }

    /* ===== Phase 2: parallel shard workers. */
    parallel_for(bulk_upd_delim_shard_worker, workers, nshard_groups,
                 sizeof(BulkUpdDelimShardWork));

    int updated = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        updated += workers[wi].updated;
        skipped += workers[wi].skipped;
        for (int i = 0; i < workers[wi].count; i++) free(workers[wi].recs[i].key);
        free(workers[wi].recs);
    }
    free(workers);

    if (data_mmaped) munmap((void *)data, st.st_size);
    else free((void *)data);

    log_msg(3, "BULK-UPDATE-DELIM %s matched=%d updated=%d skipped=%d",
            object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);
    return 0;
}

int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, int limit, int dry_run) {
    Schema sch = load_schema(db_root, object);
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        OUT("{\"error\":\"Missing criteria\"}\n");
        return 1;
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0 };
    scan_shards(data_dir, sch.slot_size, bulk_criteria_scan_cb, &ctx);
    int matched = ctx.count;

    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return 0;
    }

    /* Phase 2: Write — for each key, acquire wrlock, re-verify, tombstone */
    TypedSchema *ts = load_typed_schema(db_root, object);
    int deleted = 0, skipped = 0;

    for (int i = 0; i < matched; i++) {
        const char *key = ctx.keys[i];
        size_t klen = strlen(key);
        uint8_t hash[16]; int shard_id, start_slot;
        compute_addr(key, klen, sch.splits, hash, &shard_id, &start_slot);

        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

        FcacheRead wh = ucache_get_write(shard, 0, 0);
        if (!wh.map) { skipped++; continue; }
        uint8_t *map = wh.map;
        uint32_t slots = wh.slots_per_shard;
        uint32_t mask = slots - 1;

        /* Find the record */
        int slot = -1;
        for (uint32_t si = 0; si < slots; si++) {
            uint32_t s = ((uint32_t)start_slot + si) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag == 2) continue;
            if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
                h->key_len == klen &&
                memcmp(map + zoneB_off(s, slots, sch.slot_size), key, klen) == 0) {
                slot = (int)s; break;
            }
        }

        if (slot < 0) { ucache_write_release(wh); skipped++; continue; }

        SlotHeader *h = (SlotHeader *)(map + zoneA_off(slot));
        uint8_t *value_ptr = map + zoneB_off(slot, slots, sch.slot_size) + h->key_len;

        /* Re-check criteria tree under wrlock (AND/OR supported) */
        if (!criteria_match_tree(value_ptr, tree, &fs)) {
            ucache_write_release(wh); skipped++; continue;
        }

        /* Extract indexed field values BEFORE tombstoning */
        char idx_fields[MAX_FIELDS][256];
        int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
        char *idx_vals[MAX_FIELDS];
        memset(idx_vals, 0, sizeof(idx_vals));
        if (nidx > 0) {
            for (int fi = 0; fi < nidx; fi++) {
                if (strchr(idx_fields[fi], '+')) {
                    char fb[256]; strncpy(fb, idx_fields[fi], 255); fb[255] = '\0';
                    char cat[4096]; int cp = 0; int ok = 1;
                    char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
                    while (tok) {
                        int fidx = typed_field_index(ts, tok);
                        if (fidx >= 0) {
                            char *v = typed_get_field_str(ts, value_ptr, fidx);
                            if (v) { int sl = strlen(v); memcpy(cat+cp, v, sl); cp += sl; free(v); }
                            else { ok = 0; break; }
                        } else { ok = 0; break; }
                        tok = strtok_r(NULL, "+", &_tok_save);
                    }
                    cat[cp] = '\0';
                    idx_vals[fi] = (ok && cp > 0) ? strdup(cat) : NULL;
                } else {
                    int fidx = typed_field_index(ts, idx_fields[fi]);
                    idx_vals[fi] = typed_get_field_str(ts, value_ptr, fidx);
                }
            }
        }

        /* Tombstone */
        h->flag = 2;
        ucache_bump_record_count(wh.slot, -1);
        ucache_write_release(wh);

        /* Index cleanup */
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_vals[fi] && idx_vals[fi][0])
                delete_index_entry(db_root, object, idx_fields[fi], idx_vals[fi], hash);
            free(idx_vals[fi]);
        }

        deleted++;
    }

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }

    log_msg(3, "BULK-DELETE %s matched=%d deleted=%d skipped=%d", object, matched, deleted, skipped);
    OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n", matched, deleted, skipped);

    for (int i = 0; i < matched; i++) free(ctx.keys[i]);
    free(ctx.keys); free_criteria_tree(tree);
    return 0;
}

/* ========== VACUUM ========== */

/* ========== rebuild_object ==========
   Full rewrite of an object's data files. Used by vacuum --compact,
   vacuum --splits, and add-field (task #6). Creates data.new/ with
   live records re-slotted according to the new schema/splits, then
   atomically swaps it in for data/.

   Same-splits, same-layout rebuild preserves hash16 → (shard, slot)
   mapping, so existing index files survive the swap intact.

   Caller must hold objlock_wrlock on the object. */

/* Update the splits field for one object's line in $g_db_root/schema.conf.
   Format: dir:object:splits:max_key:prealloc_mb. Serialised by locking
   schema.conf itself via flock since this file is shared by all objects. */
static int update_schema_conf_splits(const char *db_root, const char *object,
                                     int new_splits) {
    /* Derive dir name from db_root: db_root is $g_db_root/<dir> */
    const char *dir = db_root + strlen(g_db_root);
    if (*dir == '/') dir++;

    char conf[PATH_MAX], tmp[PATH_MAX];
    snprintf(conf, sizeof(conf), "%s/schema.conf", g_db_root);
    snprintf(tmp,  sizeof(tmp),  "%s/schema.conf.tmp.%d", g_db_root, (int)getpid());

    char prefix[512];
    int pfxlen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);

    FILE *fin = fopen(conf, "r");
    if (!fin) return -1;
    int lockfd = fileno(fin);
    flock(lockfd, LOCK_EX);

    FILE *fout = fopen(tmp, "w");
    if (!fout) { flock(lockfd, LOCK_UN); fclose(fin); return -1; }

    char line[512];
    int replaced = 0;
    while (fgets(line, sizeof(line), fin)) {
        if (strncmp(line, prefix, pfxlen) == 0 && !replaced) {
            /* splits:max_key:prealloc_mb — keep max_key and prealloc, replace splits */
            int cur_splits = 0, max_key = 0, prealloc = 0;
            sscanf(line + pfxlen, "%d:%d:%d", &cur_splits, &max_key, &prealloc);
            fprintf(fout, "%s%d:%d:%d\n", prefix, new_splits, max_key, prealloc);
            replaced = 1;
        } else {
            fputs(line, fout);
        }
    }
    fclose(fout);
    int ok = (rename(tmp, conf) == 0);
    flock(lockfd, LOCK_UN);
    fclose(fin);
    return ok ? 0 : -1;
}

/* Parse a single fields.conf-style line "name:type[:param]" into a TypedField.
   Returns 1 on success, 0 on failure (bad line). Does NOT set offset. */
static int parse_field_line(const char *line, TypedField *out) {
    const char *colon = strchr(line, ':');
    if (!colon || colon == line) return 0;
    size_t nlen = colon - line;
    if (nlen >= 256) nlen = 255;
    memcpy(out->name, line, nlen);
    out->name[nlen] = '\0';
    parse_field_type(colon + 1, out);
    return out->type != FT_NONE && out->size > 0;
}

int rebuild_object(const char *db_root, const char *object,
                   int new_splits_arg, int drop_tombstoned,
                   char added_lines[][256], int n_added) {
    Schema old_sch = load_schema(db_root, object);
    if (old_sch.splits <= 0) {
        OUT("{\"error\":\"Object [%s] not found\"}\n", object);
        return 1;
    }
    TypedSchema *old_ts = load_typed_schema(db_root, object);
    if (!old_ts) {
        OUT("{\"error\":\"fields.conf missing\"}\n");
        return 1;
    }

    int old_splits = old_sch.splits;
    int new_splits = new_splits_arg > 0 ? new_splits_arg : old_splits;
    if (new_splits < MIN_SPLITS) new_splits = MIN_SPLITS;
    if (new_splits > MAX_SPLITS) new_splits = MAX_SPLITS;
    int splits_changed = (new_splits != old_splits);

    /* Build new TypedSchema:
         1. Copy existing fields (skip tombstoned if drop_tombstoned).
         2. Append any added fields at the end. */
    TypedSchema new_ts;
    memset(&new_ts, 0, sizeof(new_ts));
    new_ts.typed = 1;
    int new_to_old[MAX_FIELDS];
    int noff = 0;
    for (int i = 0; i < old_ts->nfields; i++) {
        if (drop_tombstoned && old_ts->fields[i].removed) continue;
        new_to_old[new_ts.nfields] = i;
        new_ts.fields[new_ts.nfields] = old_ts->fields[i];
        new_ts.fields[new_ts.nfields].offset = noff;
        noff += old_ts->fields[i].size;
        new_ts.nfields++;
    }
    /* Append newly-added fields — they start zero-valued in existing records. */
    for (int a = 0; a < n_added; a++) {
        if (new_ts.nfields >= MAX_FIELDS) {
            OUT("{\"error\":\"Too many fields (max %d)\"}\n", MAX_FIELDS);
            return 1;
        }
        TypedField tf;
        memset(&tf, 0, sizeof(tf));
        if (!parse_field_line(added_lines[a], &tf)) {
            OUT("{\"error\":\"Invalid field line: %s\"}\n", added_lines[a]);
            return 1;
        }
        /* Reject duplicate names against existing active fields */
        for (int i = 0; i < new_ts.nfields; i++) {
            if (strcmp(new_ts.fields[i].name, tf.name) == 0) {
                OUT("{\"error\":\"Field [%s] already exists\"}\n", tf.name);
                return 1;
            }
        }
        tf.offset = noff;
        tf.removed = 0;
        new_to_old[new_ts.nfields] = -1;  /* -1 = not present in old layout */
        new_ts.fields[new_ts.nfields] = tf;
        noff += tf.size;
        new_ts.nfields++;
    }
    new_ts.total_size = noff;

    Schema new_sch = old_sch;
    new_sch.splits = new_splits;
    new_sch.max_value = new_ts.total_size;
    new_sch.slot_size = new_sch.max_key + new_sch.max_value;
    new_sch.slot_size = (new_sch.slot_size + 7) & ~7;

    int slot_changed = (new_sch.slot_size != old_sch.slot_size);

    /* Nothing to do — caller probably called rebuild without flags */
    if (!splits_changed && !slot_changed && n_added == 0) {
        OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
        return 0;
    }

    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", obj_dir);
    char new_data_dir[PATH_MAX];
    snprintf(new_data_dir, sizeof(new_data_dir), "%s/data.new", obj_dir);

    /* Clean any stale artifacts from a previous crashed rebuild. */
    rmrf(new_data_dir);
    mkdirp(new_data_dir);

    /* Iterate current shards, copy live records into data.new/. */
    int live_count = 0;
    for (int olds = 0; olds < old_splits; olds++) {
        char old_path[PATH_MAX];
        build_shard_filename(old_path, sizeof(old_path), data_dir, olds);
        int ofd = open(old_path, O_RDONLY);
        if (ofd < 0) continue;
        struct stat st;
        if (fstat(ofd, &st) < 0 || st.st_size == 0) { close(ofd); continue; }
        uint8_t *omap = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, ofd, 0);
        close(ofd);
        if (omap == MAP_FAILED) continue;

        if ((size_t)st.st_size < SHARD_HDR_SIZE) { munmap(omap, st.st_size); continue; }
        ShardHeader *oshdr = (ShardHeader *)omap;
        if (oshdr->magic != SHARD_MAGIC || oshdr->slots_per_shard == 0) { munmap(omap, st.st_size); continue; }
        uint32_t old_slots = oshdr->slots_per_shard;
        if ((size_t)st.st_size < shard_zoneA_end(old_slots)) { munmap(omap, st.st_size); continue; }
        for (uint32_t s = 0; s < old_slots; s++) {
            SlotHeader *h = (SlotHeader *)(omap + zoneA_off(s));
            if (h->flag != 1) continue;  /* skip empty and tombstoned */

            const uint8_t *okey = omap + zoneB_off(s, old_slots, old_sch.slot_size);
            const uint8_t *oval = okey + h->key_len;

            int new_shard = (((unsigned)h->hash[0] << 8) | h->hash[1]) % new_splits;
            uint32_t start_slot_raw = ((uint32_t)h->hash[2] << 24) | ((uint32_t)h->hash[3] << 16)
                                    | ((uint32_t)h->hash[4] << 8)  |  (uint32_t)h->hash[5];

            char npath[PATH_MAX];
            build_shard_filename(npath, sizeof(npath), new_data_dir, new_shard);

            FcacheRead wh = ucache_get_write(npath, new_sch.slot_size, new_sch.prealloc_mb);
            if (!wh.map) continue;
            uint8_t *map = wh.map;
            uint32_t new_slots = wh.slots_per_shard;
            uint32_t new_mask = new_slots - 1;

            int slot = -1;
            for (uint32_t i = 0; i < new_slots; i++) {
                uint32_t probe = (start_slot_raw + i) & new_mask;
                SlotHeader *nh = (SlotHeader *)(map + zoneA_off(probe));
                if (nh->flag == 0 && nh->key_len == 0) { slot = (int)probe; break; }
            }
            if (slot < 0) {
                /* Need to grow this new shard — release, grow, retry. */
                ucache_write_release(wh);
                if (ucache_grow_shard(npath, new_sch.slot_size, new_sch.prealloc_mb) > 0) {
                    wh = ucache_get_write(npath, new_sch.slot_size, new_sch.prealloc_mb);
                    if (!wh.map) continue;
                    map = wh.map;
                    new_slots = wh.slots_per_shard;
                    new_mask = new_slots - 1;
                    for (uint32_t i = 0; i < new_slots; i++) {
                        uint32_t probe = (start_slot_raw + i) & new_mask;
                        SlotHeader *nh = (SlotHeader *)(map + zoneA_off(probe));
                        if (nh->flag == 0 && nh->key_len == 0) { slot = (int)probe; break; }
                    }
                    if (slot < 0) {
                        ucache_write_release(wh);
                        log_msg(1, "REBUILD %s/%s: no free slot in new shard %d after grow", db_root, object, new_shard);
                        continue;
                    }
                } else {
                    log_msg(1, "REBUILD %s/%s: grow failed for new shard %d", db_root, object, new_shard);
                    continue;
                }
            }

            SlotHeader *nh = (SlotHeader *)(map + zoneA_off(slot));
            memset(nh, 0, HEADER_SIZE);
            memcpy(nh->hash, h->hash, 16);
            nh->key_len = h->key_len;
            uint8_t *npay = map + zoneB_off(slot, new_slots, new_sch.slot_size);
            memcpy(npay, okey, h->key_len);

            uint8_t *nval = npay + h->key_len;
            if (!slot_changed) {
                memcpy(nval, oval, h->value_len);
                nh->value_len = h->value_len;
            } else {
                /* Slot layout changed (compact and/or added fields):
                   copy each kept active field at its new offset; added
                   fields (new_to_old==-1) stay zero from the memset above. */
                for (int k = 0; k < new_ts.nfields; k++) {
                    int oi = new_to_old[k];
                    if (oi < 0) continue; /* newly-added field, zero */
                    memcpy(nval + new_ts.fields[k].offset,
                           oval + old_ts->fields[oi].offset,
                           old_ts->fields[oi].size);
                }
                nh->value_len = new_ts.total_size;
            }
            nh->flag = 1;
            ucache_bump_record_count(wh.slot, 1);
            ucache_write_release(wh);
            live_count++;
        }
        munmap(omap, st.st_size);
    }

    /* Stage fields.conf.new if compacting (drop tombstoned lines). */
    char fpath[PATH_MAX], fpath_new[PATH_MAX], fpath_old[PATH_MAX];
    snprintf(fpath,     sizeof(fpath),     "%s/fields.conf", obj_dir);
    snprintf(fpath_new, sizeof(fpath_new), "%s/fields.conf.new", obj_dir);
    snprintf(fpath_old, sizeof(fpath_old), "%s/fields.conf.old", obj_dir);

    int fields_changed = drop_tombstoned || n_added > 0;
    if (fields_changed) {
        FILE *fin = fopen(fpath, "r");
        FILE *fout = fopen(fpath_new, "w");
        if (!fin || !fout) {
            if (fin) fclose(fin);
            if (fout) fclose(fout);
            rmrf(new_data_dir);
            OUT("{\"error\":\"Failed to stage fields.conf.new\"}\n");
            return 1;
        }
        char line[512];
        while (fgets(line, sizeof(line), fin)) {
            char stripped[512];
            strncpy(stripped, line, sizeof(stripped) - 1);
            stripped[sizeof(stripped) - 1] = '\0';
            stripped[strcspn(stripped, "\n")] = '\0';
            if (stripped[0] == '\0' || stripped[0] == '#') { fputs(line, fout); continue; }
            if (drop_tombstoned && strstr(stripped, ":removed")) continue;
            fputs(line, fout);
        }
        /* Append new fields */
        for (int a = 0; a < n_added; a++) {
            fprintf(fout, "%s\n", added_lines[a]);
        }
        fclose(fin);
        fclose(fout);
    }

    /* ===== Atomic swap window ===== */
    fcache_invalidate(data_dir);

    char data_old[PATH_MAX];
    snprintf(data_old, sizeof(data_old), "%s/data.old", obj_dir);
    rmrf(data_old);

    if (rename(data_dir, data_old) != 0) {
        rmrf(new_data_dir);
        if (fields_changed) unlink(fpath_new);
        OUT("{\"error\":\"Failed to rename data → data.old\"}\n");
        return 1;
    }
    if (rename(new_data_dir, data_dir) != 0) {
        rename(data_old, data_dir); /* try to roll back */
        if (fields_changed) unlink(fpath_new);
        OUT("{\"error\":\"Failed to rename data.new → data\"}\n");
        return 1;
    }

    if (fields_changed) {
        rename(fpath, fpath_old);
        rename(fpath_new, fpath);
    }

    if (splits_changed) {
        update_schema_conf_splits(db_root, object, new_splits);
    }

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(object);
    reset_deleted_count(db_root, object);
    set_count(db_root, object, live_count);

    /* Async-ish cleanup of .old artifacts */
    rmrf(data_old);
    if (fields_changed) unlink(fpath_old);

    log_msg(3, "REBUILD %s/%s: live=%d, splits=%d→%d, slot_size=%d→%d, compact=%d",
            db_root, object, live_count, old_splits, new_splits,
            old_sch.slot_size, new_sch.slot_size, drop_tombstoned);
    OUT("{\"status\":\"rebuilt\",\"live\":%d,\"splits\":%d,\"slot_size\":%d,\"compact\":%s}\n",
        live_count, new_splits, new_sch.slot_size, drop_tombstoned ? "true" : "false");
    return 0;
}

/* Per-shard vacuum worker */
typedef struct {
    char path[PATH_MAX];
    int slot_size;
    int cleaned;
} VacuumWork;

static void *vacuum_worker(void *arg) {
    VacuumWork *vw = (VacuumWork *)arg;
    FcacheRead wh = ucache_get_write(vw->path, 0, 0);
    if (!wh.map) return NULL;
    uint32_t slots = wh.slots_per_shard;
    if (wh.size < shard_zoneA_end(slots)) { ucache_write_release(wh); return NULL; }
    for (uint32_t i = 0; i < slots; i++) {
        SlotHeader *h = (SlotHeader *)(wh.map + zoneA_off(i));
        if (h->flag == 2) {
            memset(h, 0, HEADER_SIZE);
            memset(wh.map + zoneB_off(i, slots, vw->slot_size), 0, vw->slot_size);
            vw->cleaned++;
        }
    }
    ucache_write_release(wh);
    return NULL;
}

int cmd_vacuum(const char *db_root, const char *object,
               int compact, int new_splits) {
    /* Compact rewrite path: delegate to rebuild_object (task #4).
       Triggered by --compact or --splits. */
    if (compact || new_splits > 0) {
        return rebuild_object(db_root, object, new_splits, compact, NULL, 0);
    }

    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Collect all shard paths */
    VacuumWork *shards = NULL;
    int shard_count = 0, shard_cap = 256;
    shards = malloc(shard_cap * sizeof(VacuumWork));

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(shards); fprintf(stderr, "Error: No data directory for [%s]\n", object); return 1; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (shard_count >= shard_cap) {
            shard_cap *= 2;
            shards = realloc(shards, shard_cap * sizeof(VacuumWork));
        }
        snprintf(shards[shard_count].path, PATH_MAX, "%s/%s", data_dir, e1->d_name);
        shards[shard_count].slot_size = sch.slot_size;
        shards[shard_count].cleaned = 0;
        shard_count++;
    }
    closedir(d1);

    /* Parallel vacuum across all shards */
    parallel_for(vacuum_worker, shards, shard_count, sizeof(VacuumWork));

    int cleaned = 0;
    for (int i = 0; i < shard_count; i++)
        cleaned += shards[i].cleaned;
    free(shards);

    reset_deleted_count(db_root, object);
    OUT("{\"status\":\"vacuumed\",\"cleaned\":%d}\n", cleaned);
    return 0;
}

int is_number(const char *s) {
    if (*s == '-') s++;
    if (*s == '\0') return 0;
    int has_dot = 0;
    while (*s) {
        if (*s == '.' && !has_dot) { has_dot = 1; s++; continue; }
        if (*s < '0' || *s > '9') return 0;
        s++;
    }
    return 1;
}

/* ========== EXISTS ========== */

int cmd_exists(const char *db_root, const char *object, const char *key) {
    Schema sc = load_schema(db_root, object);
    uint8_t hash[16]; int shard_id, start_slot;
    size_t klen = strlen(key);
    compute_addr(key, klen, sc.splits, hash, &shard_id, &start_slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);

    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) { OUT("{\"exists\":false}\n"); return 1; }
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    for (uint32_t i = 0; i < slots; i++) {
        uint32_t s = ((uint32_t)start_slot + i) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag == 2) continue;
        if (h->flag == 1 && memcmp(h->hash, hash, 16) == 0 &&
            h->key_len == klen &&
            memcmp(fc.map + zoneB_off(s, slots, sc.slot_size), key, klen) == 0) {
            fcache_release(fc);
            OUT("{\"exists\":true}\n");
            return 0;
        }
    }
    fcache_release(fc);
    OUT("{\"exists\":false}\n");
    return 1;
}


/* ========== KEYS ========== */

typedef struct {
    int offset; int limit; int count; int printed;
    char csv_delim;   /* 0 = JSON mode; else CSV (delim-less, single column) */
} KeysCtx;

int keys_cb(const SlotHeader *hdr, const uint8_t *block,
                    void *ctx) {
    KeysCtx *kc = (KeysCtx *)ctx;
    if (kc->limit > 0 && kc->printed >= kc->limit) return 1; /* stop */
    kc->count++;
    if (kc->count <= kc->offset) return 0;
    char kbuf[1024];
    size_t kl = hdr->key_len < sizeof(kbuf) - 1 ? hdr->key_len : sizeof(kbuf) - 1;
    memcpy(kbuf, block, kl); kbuf[kl] = '\0';
    if (kc->csv_delim) {
        csv_emit_cell(kbuf, kc->csv_delim);
        OUT("\n");
    } else {
        OUT("%s\"%s\"", kc->printed ? "," : "", kbuf);
    }
    kc->printed++;
    return 0;
}

int cmd_keys(const char *db_root, const char *object, int offset, int limit,
             const char *format, const char *delimiter) {
    if (limit <= 0) limit = g_global_limit;
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    KeysCtx ctx = { offset, limit, 0, 0, csv_delim };
    if (csv_delim) OUT("key\n");  /* header */
    else OUT("[");
    scan_shards(data_dir, sch.slot_size, keys_cb, &ctx);
    if (!csv_delim) OUT("]\n");
    return 0;
}

/* ========== FETCH (paginated scan with optional field projection) ========== */

/* Print a record as JSON with optional projection */
void print_record_json(const SlotHeader *hdr, const uint8_t *block,
                              const char **proj_fields, int proj_count,
                              int *printed, FieldSchema *fs) {
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s{\"key\":\"%s\",\"value\":", *printed ? "," : "", key);
    if (proj_count > 0) {
        OUT("{");
        int first = 1;
        for (int i = 0; i < proj_count; i++) {
            char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[i], pv);
            first = 0; free(pv);
        }
        OUT("}}");
    } else {
        char *val = decode_value(raw, hdr->value_len, fs);
        OUT("%s}", val);
        free(val);
    }
    free(key);
    (*printed)++;
}

/* Emit a record as a JSON array row: ["key","v1","v2",...] */
void print_record_row(const SlotHeader *hdr, const uint8_t *block,
                      const char **proj_fields, int proj_count,
                      int *printed, FieldSchema *fs) {
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s[\"%s\"", *printed ? "," : "", key);
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char *pv = typed_get_field_str(fs->ts, (const uint8_t *)raw, i);
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    }
    OUT("]");
    free(key);
    (*printed)++;
}

/* Emit the "columns" header for rows format */
void emit_rows_columns(const char **proj_fields, int proj_count, FieldSchema *fs) {
    OUT("{\"columns\":[\"key\"");
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++)
            OUT(",\"%s\"", proj_fields[i]);
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            OUT(",\"%s\"", fs->ts->fields[i].name);
        }
    }
    OUT("],\"rows\":[");
}

/* ========== CSV output ==========
   Pure text body (no JSON wrapping). One physical line per row — values
   containing newlines get their `\n`/`\r` replaced with a single space
   before escaping so the export stays grep/awk/spreadsheet-friendly.
   Delimiter + `"` inside values → wrap the whole cell in `"` with internal
   `"` doubled (RFC 4180 minus multi-line support). */

/* Write one CSV cell to g_out, applying delimiter-aware quoting. */
void csv_emit_cell(const char *val, char delim) {
    if (!val || !val[0]) return;  /* empty cell */

    /* First pass: scan for characters that force quoting and build a
       whitespace-normalized copy (newline → space) on the heap so we can
       stream it back out. */
    int needs_quote = 0;
    size_t len = strlen(val);
    char stack[1024];
    char *buf;
    int heap = 0;
    if (len + 1 <= sizeof(stack)) {
        buf = stack;
    } else {
        buf = malloc(len + 1);
        if (!buf) return;
        heap = 1;
    }
    for (size_t i = 0; i < len; i++) {
        char c = val[i];
        if (c == '\n' || c == '\r') { buf[i] = ' '; continue; }
        if (c == delim || c == '"') needs_quote = 1;
        buf[i] = c;
    }
    buf[len] = '\0';

    if (!needs_quote) {
        OUT("%s", buf);
    } else {
        OUT("\"");
        for (size_t i = 0; i < len; i++) {
            if (buf[i] == '"') OUT("\"\"");
            else { char c[2] = { buf[i], '\0' }; OUT("%s", c); }
        }
        OUT("\"");
    }

    if (heap) free(buf);
}

/* Emit a CSV header row ("key,<field>,<field>,...") terminated by \n. */
static void csv_emit_header(const char **proj_fields, int proj_count,
                            FieldSchema *fs, char delim) {
    OUT("key");
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char d[2] = { delim, '\0' };
            OUT("%s", d);
            csv_emit_cell(proj_fields[i], delim);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char d[2] = { delim, '\0' };
            OUT("%s", d);
            csv_emit_cell(fs->ts->fields[i].name, delim);
        }
    }
    OUT("\n");
}

/* Emit one data row from a typed record. `raw` points at the value payload.
   `val_len` is the stored value length (for decode_field on composite/
   untyped fallback). Terminated by \n. */
static void csv_emit_row(const char *key, const uint8_t *raw, size_t val_len,
                         const char **proj_fields, int proj_count,
                         FieldSchema *fs, char delim) {
    csv_emit_cell(key, delim);
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char d[2] = { delim, '\0' }; OUT("%s", d);
            char *v = decode_field((const char *)raw, val_len, proj_fields[i], fs);
            csv_emit_cell(v, delim);
            free(v);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char d[2] = { delim, '\0' }; OUT("%s", d);
            char *v = typed_get_field_str(fs->ts, raw, i);
            csv_emit_cell(v, delim);
            free(v);
        }
    }
    OUT("\n");
}

/* Parse a delimiter string from the request. Supports `\t` literal for tabs.
   Returns the delimiter char, or 0 if the string is NULL/empty/invalid. */
char parse_csv_delim(const char *s) {
    if (!s || !s[0]) return ',';
    if (s[0] == '\\' && s[1] == 't' && s[2] == '\0') return '\t';
    if (s[1] != '\0') return ',';  /* multi-char → default to comma */
    return s[0];
}

/* Cursor-based fetch — scans from cursor position, returns next cursor.
   Cursor format: "shard_path_idx:slot_idx" or empty for start.
   Response: {"results":[...],"cursor":"..."} */
int cmd_fetch(const char *db_root, const char *object,
                     int offset, int limit, const char *proj_str,
                     const char *cursor, const char *format,
                     const char *delimiter) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    if (limit <= 0) limit = g_global_limit;
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    FieldSchema fs_fetch;
    init_field_schema(&fs_fetch, db_root, object);

    /* Parse projection fields */
    const char *proj_fields[MAX_FIELDS];
    char proj_buf[MAX_LINE];
    int proj_count = 0;
    if (proj_str && proj_str[0]) {
        snprintf(proj_buf, sizeof(proj_buf), "%s", proj_str);
        char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
        while (tok && proj_count < MAX_FIELDS) {
            proj_fields[proj_count++] = tok;
            tok = strtok_r(NULL, ",", &_tok_save);
        }
    }

    /* Collect shard paths (sorted for deterministic order) */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));
    DIR *d1 = opendir(data_dir);
    if (d1) {
        struct dirent *e1;
        while ((e1 = readdir(d1))) {
            if (e1->d_name[0] == '.') continue;
            size_t nlen = strlen(e1->d_name);
            if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
            if (path_count >= path_cap) { path_cap *= 2; paths = realloc(paths, path_cap * sizeof(char *)); }
            char bp[PATH_MAX];
            snprintf(bp, sizeof(bp), "%s/%s", data_dir, e1->d_name);
            paths[path_count++] = strdup(bp);
        }
        closedir(d1);
    }

    /* Parse cursor: "path_idx:slot_idx" */
    int start_path = 0;
    size_t start_slot = 0;
    if (cursor && cursor[0]) {
        sscanf(cursor, "%d:%zu", &start_path, &start_slot);
        start_slot++; /* resume AFTER the last returned slot */
    }

    int printed = 0;
    int next_path = -1;
    size_t next_slot = 0;

    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;

    if (csv_delim)
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count, fs_ptr, csv_delim);
    else if (rows_fmt)
        emit_rows_columns(proj_fields, proj_count, fs_ptr);
    else
        OUT("{\"results\":[");

    for (int pi = start_path; pi < path_count && printed < limit; pi++) {
        FcacheRead fc = fcache_get_read(paths[pi]);
        if (!fc.map) continue;
        uint32_t shard_slots = fc.slots_per_shard;
        if (fc.size < shard_zoneA_end(shard_slots)) { fcache_release(fc); continue; }
        size_t slot_start = (pi == start_path) ? start_slot : 0;

        for (size_t si = slot_start; si < shard_slots && printed < limit; si++) {
            const SlotHeader *hdr = (const SlotHeader *)(fc.map + zoneA_off(si));
            if (hdr->flag != 1) continue;

            if (!cursor || !cursor[0]) {
                offset--;
                if (offset >= 0) continue;
            }

            const uint8_t *block = fc.map + zoneB_off(si, shard_slots, sch.slot_size);
            if (csv_delim) {
                char kbuf[1024];
                size_t kl = hdr->key_len < sizeof(kbuf) - 1 ? hdr->key_len : sizeof(kbuf) - 1;
                memcpy(kbuf, block, kl); kbuf[kl] = '\0';
                csv_emit_row(kbuf, block + hdr->key_len, hdr->value_len,
                             proj_count > 0 ? proj_fields : NULL,
                             proj_count, fs_ptr, csv_delim);
                printed++;
            } else if (rows_fmt)
                print_record_row(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
            else
                print_record_json(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
            next_path = pi;
            next_slot = si;
        }
        fcache_release(fc);
    }

    /* Build next cursor — CSV mode omits cursor (streaming export). */
    if (csv_delim) {
        /* Nothing more to append; body already ends with \n per row. */
    } else if (printed >= limit && next_path >= 0) {
        OUT("],\"cursor\":\"%d:%zu\"}\n", next_path, next_slot);
    } else {
        OUT("],\"cursor\":null}\n");
    }

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
    return 0;
}

/* ========== EXCLUDED KEYS HELPER ========== */


ExcludedKeys parse_excluded_keys(const char *csv) {
    ExcludedKeys ex = { NULL, 0 };
    if (!csv || !csv[0]) return ex;
    char *copy = strdup(csv);
    /* Count commas */
    int cap = 16;
    ex.keys = malloc(cap * sizeof(char *));
    char *_tok_save = NULL; char *tok = strtok_r(copy, ",", &_tok_save);
    while (tok) {
        if (ex.count >= cap) { cap *= 2; ex.keys = realloc(ex.keys, cap * sizeof(char *)); }
        ex.keys[ex.count++] = strdup(tok);
        tok = strtok_r(NULL, ",", &_tok_save);
    }
    free(copy);
    return ex;
}

int is_excluded(const ExcludedKeys *ex, const char *key) {
    for (int i = 0; i < ex->count; i++)
        if (strcmp(ex->keys[i], key) == 0) return 1;
    return 0;
}

void free_excluded(ExcludedKeys *ex) {
    for (int i = 0; i < ex->count; i++) free(ex->keys[i]);
    free(ex->keys);
    ex->keys = NULL; ex->count = 0;
}

/* ========== ADVANCED SEARCH (multi-criteria, all operators) ========== */

typedef struct {
    CriteriaNode *tree;
    int offset;
    int limit;
    int count;
    int printed;
    const char **proj_fields;
    int proj_count;
    ExcludedKeys excluded;
    FieldSchema *fs;
    int rows_fmt;
    char csv_delim;       /* 0 = not CSV; else delimiter char, overrides rows_fmt */
    /* Joins (when njoins > 0, output is always tabular even if rows_fmt==0) */
    const char *driver_object;
    JoinSpec *joins;
    int njoins;
    const char *db_root;
    QueryDeadline *deadline;
    int dl_counter;
} AdvSearchCtx;

int match_criterion(const char *val_str, const SearchCriterion *c) {
    if (!val_str && c->op != OP_NOT_EXISTS && c->op != OP_EXISTS) return 0;

    switch (c->op) {
        case OP_EXISTS:
            return val_str != NULL;
        case OP_NOT_EXISTS:
            return val_str == NULL;
        case OP_EQUAL:
            return strcmp(val_str, c->value) == 0;
        case OP_NOT_EQUAL:
            return strcmp(val_str, c->value) != 0;
        case OP_LESS:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) < atof(c->value);
            return strcmp(val_str, c->value) < 0;
        case OP_GREATER:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) > atof(c->value);
            return strcmp(val_str, c->value) > 0;
        case OP_LESS_EQ:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) <= atof(c->value);
            return strcmp(val_str, c->value) <= 0;
        case OP_GREATER_EQ:
            if (is_number(val_str) && is_number(c->value))
                return atof(val_str) >= atof(c->value);
            return strcmp(val_str, c->value) >= 0;
        case OP_BETWEEN:
            if (is_number(val_str) && is_number(c->value) && is_number(c->value2)) {
                double v = atof(val_str);
                return v >= atof(c->value) && v <= atof(c->value2);
            }
            return strcmp(val_str, c->value) >= 0 && strcmp(val_str, c->value2) <= 0;
        case OP_LIKE: {
            /* Simple LIKE: % = any, _ = single char */
            /* Convert to basic substring check for %val% pattern */
            const char *pat = c->value;
            if (pat[0] == '%' && pat[strlen(pat)-1] == '%') {
                char sub[MAX_LINE];
                snprintf(sub, sizeof(sub), "%s", pat + 1);
                sub[strlen(sub)-1] = '\0';
                return strcasestr(val_str, sub) != NULL;
            }
            if (pat[0] == '%') return strcasestr(val_str, pat + 1) != NULL;
            if (pat[strlen(pat)-1] == '%') {
                return strncasecmp(val_str, pat, strlen(pat)-1) == 0;
            }
            return strcasecmp(val_str, pat) == 0;
        }
        case OP_NOT_LIKE: {
            SearchCriterion tmp = *c;
            tmp.op = OP_LIKE;
            return !match_criterion(val_str, &tmp);
        }
        case OP_CONTAINS:
            return strcasestr(val_str, c->value) != NULL;
        case OP_NOT_CONTAINS:
            return strcasestr(val_str, c->value) == NULL;
        case OP_STARTS_WITH:
            return strncasecmp(val_str, c->value, strlen(c->value)) == 0;
        case OP_ENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(c->value);
            if (vl < cl) return 0;
            return strcasecmp(val_str + vl - cl, c->value) == 0;
        }
        case OP_IN:
            for (int i = 0; i < c->in_count; i++)
                if (strcmp(val_str, c->in_values[i]) == 0) return 1;
            return 0;
        case OP_NOT_IN:
            for (int i = 0; i < c->in_count; i++)
                if (strcmp(val_str, c->in_values[i]) == 0) return 0;
            return 1;
    }
    return 0;
}

/* ========== Typed-binary compiled criteria (fast path) ==========
 *
 * Replaces per-record malloc+snprintf+strdup+atof+free in scan callbacks
 * by pre-resolving each SearchCriterion against the TypedSchema and
 * pre-parsing rvalues into target binary form. Runtime match compares
 * raw bytes directly against known offsets.
 *
 * Composite fields (contain '+') and unknown fields fall back to the
 * legacy string path via decode_field + match_criterion. */

/* Sign-extending big-endian loaders (storage format per src/config.c) */
static inline int64_t ld_be_i64(const uint8_t *p) {
    uint64_t u = ((uint64_t)p[0] << 56) | ((uint64_t)p[1] << 48) |
                 ((uint64_t)p[2] << 40) | ((uint64_t)p[3] << 32) |
                 ((uint64_t)p[4] << 24) | ((uint64_t)p[5] << 16) |
                 ((uint64_t)p[6] << 8)  |  (uint64_t)p[7];
    return (int64_t)u;
}
static inline int32_t ld_be_i32(const uint8_t *p) {
    uint32_t u = ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
                 ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
    return (int32_t)u;
}
static inline int16_t ld_be_i16(const uint8_t *p) {
    uint16_t u = ((uint16_t)p[0] << 8) | (uint16_t)p[1];
    return (int16_t)u;
}
static inline uint16_t ld_be_u16(const uint8_t *p) {
    return ((uint16_t)p[0] << 8) | (uint16_t)p[1];
}

/* Effective varchar length: read BE uint16 prefix. `size` is on-disk field size
   (content + 2). Content starts at p + 2. Defensively clamps if prefix is bogus. */
static inline int varchar_eff_len(const uint8_t *p, int size) {
    int len = ((int)p[0] << 8) | (int)p[1];
    int content_max = size - 2;
    if (len > content_max) len = content_max;
    if (len < 0) len = 0;
    return len;
}

/* Case-insensitive substring search. needle_lc must already be lowercase. */
static const char *memcasemem(const char *h, size_t hl,
                              const char *needle_lc, size_t nl) {
    if (nl == 0) return h;
    if (hl < nl) return NULL;
    for (size_t i = 0; i + nl <= hl; i++) {
        size_t j = 0;
        for (; j < nl; j++) {
            char c = h[i + j];
            if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
            if (c != needle_lc[j]) break;
        }
        if (j == nl) return h + i;
    }
    return NULL;
}

/* Parse decimal date string "yyyyMMdd" (tolerant of separators) → int32. */
static int32_t parse_date_i32(const char *s) {
    char clean[16]; int ci = 0;
    for (const char *c = s; *c && ci < 8; c++)
        if (*c >= '0' && *c <= '9') clean[ci++] = *c;
    clean[ci] = '\0';
    return (int32_t)atoi(clean);
}

/* Parse "yyyyMMddHHmmss" (tolerant) → date (int32) + time (uint16 seconds). */
static void parse_datetime(const char *s, int32_t *out_date, uint16_t *out_time) {
    char clean[16]; int ci = 0;
    for (const char *c = s; *c && ci < 14; c++)
        if (*c >= '0' && *c <= '9') clean[ci++] = *c;
    while (ci < 14) clean[ci++] = '0';
    clean[14] = '\0';
    char dbuf[9]; memcpy(dbuf, clean, 8); dbuf[8] = '\0';
    *out_date = (int32_t)atoi(dbuf);
    int hh = (clean[8]-'0')*10 + (clean[9]-'0');
    int mm = (clean[10]-'0')*10 + (clean[11]-'0');
    int ss = (clean[12]-'0')*10 + (clean[13]-'0');
    *out_time = (uint16_t)(hh * 3600 + mm * 60 + ss);
}

/* Parse decimal string → int64 scaled by 10^scale (matches encode_field FT_NUMERIC). */
static int64_t parse_numeric_i64(const char *s, int scale) {
    double dv = atof(s);
    int64_t mul = 1;
    for (int i = 0; i < scale; i++) mul *= 10;
    return (int64_t)(dv * mul + (dv >= 0 ? 0.5 : -0.5));
}

/* Lowercase duplicate (ASCII). Caller frees. */
static char *strdup_lower(const char *s, size_t len) {
    char *r = malloc(len + 1);
    for (size_t i = 0; i < len; i++) {
        char c = s[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c + 32);
        r[i] = c;
    }
    r[len] = '\0';
    return r;
}

static void compile_one(CompiledCriterion *cc, const SearchCriterion *c,
                        const TypedSchema *ts) {
    memset(cc, 0, sizeof(*cc));
    cc->op = c->op;
    cc->raw = c;

    if (strchr(c->field, '+')) { cc->composite = 1; return; }
    int idx = ts ? typed_field_index(ts, c->field) : -1;
    if (idx < 0) { cc->tf = NULL; return; }
    cc->tf = &ts->fields[idx];
    cc->ftype = cc->tf->type;

    /* Copy string forms (needed for varchar ops and fallback BETWEEN strings) */
    cc->s1_len = strlen(c->value);
    cc->s1 = malloc(cc->s1_len + 1);
    memcpy(cc->s1, c->value, cc->s1_len + 1);
    cc->s2_len = strlen(c->value2);
    cc->s2 = malloc(cc->s2_len + 1);
    memcpy(cc->s2, c->value2, cc->s2_len + 1);

    /* Type-specific parsing of scalar rvalue */
    switch (cc->ftype) {
    case FT_LONG:
    case FT_INT:
    case FT_SHORT:
        cc->i1 = (int64_t)strtoll(c->value, NULL, 10);
        cc->i2 = (int64_t)strtoll(c->value2, NULL, 10);
        break;
    case FT_NUMERIC:
        cc->i1 = parse_numeric_i64(c->value, cc->tf->numeric_scale);
        cc->i2 = parse_numeric_i64(c->value2, cc->tf->numeric_scale);
        break;
    case FT_DOUBLE:
        cc->d1 = atof(c->value);
        cc->d2 = atof(c->value2);
        break;
    case FT_BOOL:
        cc->b1 = (c->value[0] == 't' || c->value[0] == 'T' || c->value[0] == '1') ? 1 : 0;
        break;
    case FT_BYTE:
        cc->b1 = (uint8_t)atoi(c->value);
        break;
    case FT_DATE:
        cc->i1 = parse_date_i32(c->value);
        cc->i2 = parse_date_i32(c->value2);
        break;
    case FT_DATETIME: {
        int32_t d; uint16_t t;
        parse_datetime(c->value, &d, &t); cc->i1 = d; cc->t1 = t;
        parse_datetime(c->value2, &d, &t); cc->i2 = d; cc->t2 = t;
        break;
    }
    case FT_VARCHAR:
    default:
        break;
    }

    /* LIKE pattern classification for varchar ops */
    if (cc->ftype == FT_VARCHAR &&
        (cc->op == OP_LIKE || cc->op == OP_NOT_LIKE ||
         cc->op == OP_CONTAINS || cc->op == OP_NOT_CONTAINS ||
         cc->op == OP_STARTS_WITH || cc->op == OP_ENDS_WITH ||
         cc->op == OP_EQUAL || cc->op == OP_NOT_EQUAL)) {
        const char *pat = c->value;
        size_t pl = cc->s1_len;
        /* LIKE unpacks the %; other ops use literal value */
        if (cc->op == OP_LIKE || cc->op == OP_NOT_LIKE) {
            if (pl >= 2 && pat[0] == '%' && pat[pl-1] == '%') {
                cc->like_kind = LK_CONTAINS;
                cc->needle_lc = strdup_lower(pat + 1, pl - 2);
                cc->needle_len = pl - 2;
            } else if (pl >= 1 && pat[0] == '%') {
                /* Preserves current match_criterion behavior: %foo → substring */
                cc->like_kind = LK_CONTAINS;
                cc->needle_lc = strdup_lower(pat + 1, pl - 1);
                cc->needle_len = pl - 1;
            } else if (pl >= 1 && pat[pl-1] == '%') {
                cc->like_kind = LK_PREFIX;
                cc->needle_lc = strdup_lower(pat, pl - 1);
                cc->needle_len = pl - 1;
            } else {
                cc->like_kind = LK_EXACT;
                cc->needle_lc = strdup_lower(pat, pl);
                cc->needle_len = pl;
            }
        } else {
            /* CONTAINS/STARTS_WITH/ENDS_WITH use c->value as-is (case-insensitive) */
            cc->needle_lc = strdup_lower(pat, pl);
            cc->needle_len = pl;
        }
    }

    /* IN/NOT_IN list pre-parsing (numerics only — varchar uses raw strings) */
    if ((cc->op == OP_IN || cc->op == OP_NOT_IN) && c->in_count > 0) {
        cc->in_count = c->in_count;
        switch (cc->ftype) {
        case FT_LONG: case FT_INT: case FT_SHORT:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)strtoll(c->in_values[i], NULL, 10);
            break;
        case FT_NUMERIC:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = parse_numeric_i64(c->in_values[i], cc->tf->numeric_scale);
            break;
        case FT_DOUBLE:
            cc->in_f64 = malloc(sizeof(double) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_f64[i] = atof(c->in_values[i]);
            break;
        case FT_DATE:
            cc->in_i64 = malloc(sizeof(int64_t) * c->in_count);
            for (int i = 0; i < c->in_count; i++)
                cc->in_i64[i] = (int64_t)parse_date_i32(c->in_values[i]);
            break;
        default:
            /* VARCHAR, DATETIME, BOOL, BYTE: use raw strings via c->in_values */
            break;
        }
    }
}

CompiledCriterion *compile_criteria(const SearchCriterion *in, int n,
                                    const TypedSchema *ts) {
    if (n <= 0) return NULL;
    CompiledCriterion *arr = calloc(n, sizeof(CompiledCriterion));
    for (int i = 0; i < n; i++) compile_one(&arr[i], &in[i], ts);
    return arr;
}

void free_compiled_criteria(CompiledCriterion *arr, int n) {
    if (!arr) return;
    for (int i = 0; i < n; i++) {
        free(arr[i].s1);
        free(arr[i].s2);
        free(arr[i].needle_lc);
        free(arr[i].in_i64);
        free(arr[i].in_f64);
    }
    free(arr);
}

/* Compare varchar field at p (fixed size) against compiled criterion. */
static int match_typed_varchar(const uint8_t *p, int size,
                               const CompiledCriterion *cc) {
    int elen = varchar_eff_len(p, size);
    const char *hay = (const char *)(p + 2);  /* content starts after uint16 prefix */
    const SearchCriterion *c = cc->raw;

    switch (cc->op) {
    case OP_EXISTS: return elen > 0;
    case OP_NOT_EXISTS: return elen == 0;
    case OP_EQUAL:
        return elen == (int)cc->s1_len && memcmp(hay, cc->s1, elen) == 0;
    case OP_NOT_EQUAL:
        return !(elen == (int)cc->s1_len && memcmp(hay, cc->s1, elen) == 0);
    case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ: {
        size_t n = elen < (int)cc->s1_len ? (size_t)elen : cc->s1_len;
        int r = memcmp(hay, cc->s1, n);
        if (r == 0) r = (elen < (int)cc->s1_len) ? -1 : (elen > (int)cc->s1_len ? 1 : 0);
        switch (cc->op) {
            case OP_LESS: return r < 0;
            case OP_GREATER: return r > 0;
            case OP_LESS_EQ: return r <= 0;
            case OP_GREATER_EQ: return r >= 0;
            default: return 0;
        }
    }
    case OP_BETWEEN: {
        size_t n1 = elen < (int)cc->s1_len ? (size_t)elen : cc->s1_len;
        int r1 = memcmp(hay, cc->s1, n1);
        if (r1 == 0) r1 = (elen < (int)cc->s1_len) ? -1 : (elen > (int)cc->s1_len ? 1 : 0);
        if (r1 < 0) return 0;
        size_t n2 = elen < (int)cc->s2_len ? (size_t)elen : cc->s2_len;
        int r2 = memcmp(hay, cc->s2, n2);
        if (r2 == 0) r2 = (elen < (int)cc->s2_len) ? -1 : (elen > (int)cc->s2_len ? 1 : 0);
        return r2 <= 0;
    }
    case OP_LIKE:
        switch (cc->like_kind) {
        case LK_EXACT: {
            if (elen != (int)cc->needle_len) return 0;
            for (int i = 0; i < elen; i++) {
                char a = hay[i], b = cc->needle_lc[i];
                if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
                if (a != b) return 0;
            }
            return 1;
        }
        case LK_PREFIX:
            if (elen < (int)cc->needle_len) return 0;
            for (size_t i = 0; i < cc->needle_len; i++) {
                char a = hay[i], b = cc->needle_lc[i];
                if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
                if (a != b) return 0;
            }
            return 1;
        case LK_CONTAINS:
            return memcasemem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
        }
        return 0;
    case OP_NOT_LIKE: {
        CompiledCriterion tmp = *cc; tmp.op = OP_LIKE;
        return !match_typed_varchar(p, size, &tmp);
    }
    case OP_CONTAINS:
        return memcasemem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
    case OP_NOT_CONTAINS:
        return memcasemem(hay, elen, cc->needle_lc, cc->needle_len) == NULL;
    case OP_STARTS_WITH: {
        if (elen < (int)cc->needle_len) return 0;
        for (size_t i = 0; i < cc->needle_len; i++) {
            char a = hay[i], b = cc->needle_lc[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != b) return 0;
        }
        return 1;
    }
    case OP_ENDS_WITH: {
        if (elen < (int)cc->needle_len) return 0;
        const char *tail = hay + elen - cc->needle_len;
        for (size_t i = 0; i < cc->needle_len; i++) {
            char a = tail[i], b = cc->needle_lc[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != b) return 0;
        }
        return 1;
    }
    case OP_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = strlen(c->in_values[i]);
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 1;
        }
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < c->in_count; i++) {
            size_t vl = strlen(c->in_values[i]);
            if (elen == (int)vl && memcmp(hay, c->in_values[i], vl) == 0) return 0;
        }
        return 1;
    }
    return 0;
}

/* Numeric comparison helpers — integer and double flavors, sharing op dispatch. */
static inline int cmp_op_i64(int64_t v, int64_t q1, int64_t q2,
                             enum SearchOp op, const int64_t *in_list,
                             int in_count, const CompiledCriterion *cc) {
    (void)cc;
    switch (op) {
    case OP_EXISTS: return 1; /* numeric fields always "exist" — zero is valid */
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: return v == q1;
    case OP_NOT_EQUAL: return v != q1;
    case OP_LESS: return v < q1;
    case OP_GREATER: return v > q1;
    case OP_LESS_EQ: return v <= q1;
    case OP_GREATER_EQ: return v >= q1;
    case OP_BETWEEN: return v >= q1 && v <= q2;
    case OP_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 0;
        return 1;
    default: return 0;
    }
}

static inline int cmp_op_f64(double v, double q1, double q2,
                             enum SearchOp op, const double *in_list, int in_count) {
    switch (op) {
    case OP_EXISTS: return 1;
    case OP_NOT_EXISTS: return 0;
    case OP_EQUAL: return v == q1;
    case OP_NOT_EQUAL: return v != q1;
    case OP_LESS: return v < q1;
    case OP_GREATER: return v > q1;
    case OP_LESS_EQ: return v <= q1;
    case OP_GREATER_EQ: return v >= q1;
    case OP_BETWEEN: return v >= q1 && v <= q2;
    case OP_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 1;
        return 0;
    case OP_NOT_IN:
        for (int i = 0; i < in_count; i++) if (v == in_list[i]) return 0;
        return 1;
    default: return 0;
    }
}

/* Hot-path match: typed binary compare against pre-compiled criterion.
   `rec` points at the raw value region of the record (hdr->key_len offset
   into block). Returns 1 on match, 0 otherwise.
   For composite/unknown fields, falls back to decode_field + match_criterion. */
int match_typed(const uint8_t *rec, const CompiledCriterion *cc, FieldSchema *fs) {
    if (cc->composite || !cc->tf) {
        char *attr = decode_field((const char *)rec, 0, cc->raw->field, fs);
        int r = match_criterion(attr, cc->raw);
        free(attr);
        return r;
    }

    const TypedField *f = cc->tf;
    const uint8_t *p = rec + f->offset;

    switch (f->type) {
    case FT_NONE:
        return 0;
    case FT_VARCHAR:
        return match_typed_varchar(p, f->size, cc);
    case FT_LONG: {
        int64_t v = ld_be_i64(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_INT: {
        int64_t v = (int64_t)ld_be_i32(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_SHORT: {
        int64_t v = (int64_t)ld_be_i16(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        return cmp_op_i64(v, cc->i1, cc->i2, cc->op, cc->in_i64, cc->in_count, cc);
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        return cmp_op_f64(v, cc->d1, cc->d2, cc->op, cc->in_f64, cc->in_count);
    }
    case FT_BOOL: case FT_BYTE: {
        int64_t v = (int64_t)p[0];
        int64_t q = (int64_t)cc->b1;
        switch (cc->op) {
        case OP_EXISTS: return 1;
        case OP_NOT_EXISTS: return 0;
        case OP_EQUAL: return v == q;
        case OP_NOT_EQUAL: return v != q;
        default: return 0;
        }
    }
    case FT_DATE: {
        int64_t v = (int64_t)ld_be_i32(p);
        switch (cc->op) {
        case OP_EXISTS: return v != 0;
        case OP_NOT_EXISTS: return v == 0;
        default:
            if (v == 0) return 0; /* empty date matches nothing except NOT_EXISTS */
            if (cc->op == OP_IN || cc->op == OP_NOT_IN)
                return cmp_op_i64(v, 0, 0, cc->op, cc->in_i64, cc->in_count, cc);
            return cmp_op_i64(v, cc->i1, cc->i2, cc->op, NULL, 0, cc);
        }
    }
    case FT_DATETIME: {
        int64_t d = (int64_t)ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        /* Compose into single int64 for ordered compare: date*100000 + seconds */
        int64_t v = d * 100000LL + (int64_t)t;
        int64_t q1 = cc->i1 * 100000LL + (int64_t)cc->t1;
        int64_t q2 = cc->i2 * 100000LL + (int64_t)cc->t2;
        switch (cc->op) {
        case OP_EXISTS: return !(d == 0 && t == 0);
        case OP_NOT_EXISTS: return d == 0 && t == 0;
        default:
            if (d == 0 && t == 0) return 0;
            return cmp_op_i64(v, q1, q2, cc->op, NULL, 0, cc);
        }
    }
    }
    return 0;
}

/* ========== Joins (find-side) ==========
 * Parse, resolve, lookup and emit join results. Joined queries always return
 * tabular {"columns":[...],"rows":[[...]]} with fully-namespaced column names.
 */

/* Forward decl — defined later in the aggregate section for value extraction. */
static int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,
                                  char *buf, size_t bufsz);

static int parse_one_join(const char *obj_buf, JoinSpec *j) {
    memset(j, 0, sizeof(*j));
    j->type = JOIN_INNER;

    JsonObj jobj;
    json_parse_object(obj_buf, strlen(obj_buf), &jobj);

    char *o   = json_obj_strdup(&jobj, "object");
    char *l   = json_obj_strdup(&jobj, "local");
    char *r   = json_obj_strdup(&jobj, "remote");
    char *as  = json_obj_strdup(&jobj, "as");
    char *t   = json_obj_strdup(&jobj, "type");
    char *f   = json_obj_strdup_raw(&jobj, "fields");

    if (o)  { strncpy(j->object, o, 255); free(o); }
    if (l)  { strncpy(j->local_field, l, 255); free(l); }
    if (r)  { strncpy(j->remote_field, r, 255); free(r); }
    if (as) { strncpy(j->as_name, as, 255); free(as); }
    else    { strncpy(j->as_name, j->object, 255); }
    if (t)  { if (strcmp(t, "left") == 0) j->type = JOIN_LEFT; free(t); }

    if (f) {
        const char *p = f;
        while (*p && j->proj_count < MAX_FIELDS) {
            while (*p && *p != '"') p++;
            if (!*p) break;
            p++;
            const char *s = p;
            while (*p && *p != '"') p++;
            int len = (int)(p - s);
            if (len > 0 && len < 255) {
                if (len == 3 && strncmp(s, "key", 3) == 0) {
                    j->include_remote_key = 1;
                } else {
                    memcpy(j->proj_fields[j->proj_count], s, len);
                    j->proj_fields[j->proj_count][len] = '\0';
                    j->proj_count++;
                }
            }
            if (*p == '"') p++;
        }
        free(f);
    }

    return (j->object[0] && j->local_field[0] && j->remote_field[0] && j->as_name[0]) ? 1 : 0;
}

int parse_joins_json(const char *json, JoinSpec **out, int *count) {
    *out = NULL; *count = 0;
    if (!json || !json[0]) return 0;
    const char *p = json_skip(json);
    if (*p != '[') return -1;

    int cap = 8;
    JoinSpec *arr = calloc(cap, sizeof(JoinSpec));
    int n = 0;
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '{') { p++; continue; }

        if (n >= cap) {
            cap *= 2;
            arr = realloc(arr, cap * sizeof(JoinSpec));
            memset(&arr[n], 0, (cap - n) * sizeof(JoinSpec));
        }

        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char obj_buf[MAX_LINE];
        if (obj_len >= sizeof(obj_buf)) { p = obj_end; continue; }
        memcpy(obj_buf, obj_start, obj_len);
        obj_buf[obj_len] = '\0';

        if (parse_one_join(obj_buf, &arr[n])) n++;
        p = obj_end;
    }
    *out = arr; *count = n;
    return 0;
}

void free_joins(JoinSpec *arr, int n) {
    (void)n;
    free(arr);
}

/* Validate join specs against driver schema + load remote schemas + pre-resolve
   field pointers. Writes {"error":...} to OUT on failure. */
static int resolve_joins(JoinSpec *joins, int n, const char *db_root,
                         const char *driver_object, FieldSchema *driver_fs) {
    for (int i = 0; i < n; i++) {
        if (strcmp(joins[i].as_name, driver_object) == 0) {
            OUT("{\"error\":\"join 'as' [%s] collides with driver object name\"}\n",
                joins[i].as_name);
            return -1;
        }
        for (int k = 0; k < i; k++) {
            if (strcmp(joins[i].as_name, joins[k].as_name) == 0) {
                OUT("{\"error\":\"duplicate join 'as' [%s]\"}\n", joins[i].as_name);
                return -1;
            }
        }
    }

    for (int i = 0; i < n; i++) {
        JoinSpec *j = &joins[i];

        j->remote_sch = load_schema(db_root, j->object);
        if (j->remote_sch.splits <= 0) {
            OUT("{\"error\":\"join remote object [%s] not found\"}\n", j->object);
            return -1;
        }
        init_field_schema(&j->remote_fs, db_root, j->object);

        j->local_is_composite = (strchr(j->local_field, '+') != NULL);
        if (!j->local_is_composite && driver_fs && driver_fs->ts) {
            int idx = typed_field_index(driver_fs->ts, j->local_field);
            if (idx >= 0) j->local_tf = &driver_fs->ts->fields[idx];
        }

        if (strcmp(j->remote_field, "key") == 0) {
            j->remote_is_key = 1;
        } else {
            j->remote_is_key = 0;
            snprintf(j->remote_idx_path, sizeof(j->remote_idx_path),
                     "%s/%s/indexes/%s.idx", db_root, j->object, j->remote_field);
            struct stat ist;
            if (stat(j->remote_idx_path, &ist) != 0 || ist.st_size == 0) {
                OUT("{\"error\":\"join remote field [%s.%s] must be 'key' or indexed\"}\n",
                    j->object, j->remote_field);
                return -1;
            }
        }

        if (j->proj_count == 0) {
            /* No explicit fields → all non-tombstoned remote fields */
            if (j->remote_fs.ts) {
                for (int k = 0; k < j->remote_fs.ts->nfields && j->proj_count < MAX_FIELDS; k++) {
                    if (j->remote_fs.ts->fields[k].removed) continue;
                    strncpy(j->proj_fields[j->proj_count],
                            j->remote_fs.ts->fields[k].name, 255);
                    j->proj_tfs[j->proj_count] = &j->remote_fs.ts->fields[k];
                    j->proj_count++;
                }
            }
        } else {
            for (int k = 0; k < j->proj_count; k++) {
                if (strchr(j->proj_fields[k], '+')) {
                    OUT("{\"error\":\"composite projection field [%s] not supported\"}\n",
                        j->proj_fields[k]);
                    return -1;
                }
                if (j->remote_fs.ts) {
                    int idx = typed_field_index(j->remote_fs.ts, j->proj_fields[k]);
                    if (idx >= 0) {
                        j->proj_tfs[k] = &j->remote_fs.ts->fields[idx];
                    } else {
                        OUT("{\"error\":\"join field [%s.%s] not found\"}\n",
                            j->object, j->proj_fields[k]);
                        return -1;
                    }
                }
            }
        }
    }
    return 0;
}

/* Extract the local field value from a driver record into buf.
   Handles composite (a+b → concat values). Returns written length (0 = empty). */
static int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                             const TypedSchema *driver_ts,
                             char *buf, size_t bufsz) {
    if (j->local_is_composite) {
        char fb[256]; strncpy(fb, j->local_field, 255); fb[255] = '\0';
        int pos = 0;
        char *save = NULL;
        char *tok = strtok_r(fb, "+", &save);
        while (tok && pos < (int)bufsz - 1) {
            int idx = driver_ts ? typed_field_index(driver_ts, tok) : -1;
            if (idx >= 0) {
                const TypedField *tf = &driver_ts->fields[idx];
                char tmp[256];
                int tl = typed_field_to_buf_raw(tf, driver_raw + tf->offset,
                                                tmp, sizeof(tmp));
                if (tl > 0 && pos + tl < (int)bufsz) {
                    memcpy(buf + pos, tmp, tl);
                    pos += tl;
                }
            }
            tok = strtok_r(NULL, "+", &save);
        }
        buf[pos] = '\0';
        return pos;
    }
    if (j->local_tf) {
        return typed_field_to_buf_raw(j->local_tf,
                                      driver_raw + j->local_tf->offset,
                                      buf, bufsz);
    }
    return 0;
}

/* Btree search callback — captures the first hash hit. */
typedef struct { uint8_t hash[16]; int found; } JoinBtHit;
static int join_bt_first_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    JoinBtHit *h = (JoinBtHit *)ctx;
    if (h->found) return -1;
    memcpy(h->hash, hash16, 16);
    h->found = 1;
    return -1;  /* stop after first match */
}

/* Look up the remote record for one join. Returns 1 if found, 0 otherwise.
   On success, *out_fc holds the mmap handle (caller must fcache_release) and
   *out_raw points to the record's value bytes in the shard map. */
static int lookup_remote(const JoinSpec *j, const char *db_root,
                         const char *local_key, size_t local_len,
                         FcacheRead *out_fc, const uint8_t **out_raw) {
    out_fc->map = NULL;
    *out_raw = NULL;
    if (local_len == 0) return 0;

    uint8_t hash[16];
    int shard_id, start_slot;

    if (j->remote_is_key) {
        compute_addr(local_key, local_len, j->remote_sch.splits, hash, &shard_id, &start_slot);
    } else {
        JoinBtHit hit = {{0}, 0};
        btree_search(j->remote_idx_path, local_key, local_len, join_bt_first_cb, &hit);
        if (!hit.found) return 0;
        memcpy(hash, hit.hash, 16);
        addr_from_hash(hash, j->remote_sch.splits, &shard_id, &start_slot);
    }

    char path[PATH_MAX];
    build_shard_path(path, sizeof(path), db_root, j->object, shard_id);
    FcacheRead fc = fcache_get_read(path);
    if (!fc.map) return 0;

    uint32_t slots = fc.slots_per_shard;
    uint32_t mask  = slots - 1;
    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)start_slot + p) & mask;
        const SlotHeader *h = (const SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash, 16) != 0) continue;
        /* For indexed lookup, also verify key equality is satisfied by the index
           (btree values equal local_key by construction). For primary-key lookup,
           verify key bytes match local_key. */
        if (j->remote_is_key) {
            if (h->key_len != local_len) continue;
            const uint8_t *kb = fc.map + zoneB_off(s, slots, j->remote_sch.slot_size);
            if (memcmp(kb, local_key, local_len) != 0) continue;
        }
        *out_fc = fc;
        *out_raw = fc.map + zoneB_off(s, slots, j->remote_sch.slot_size) + h->key_len;
        return 1;
    }
    fcache_release(fc);
    return 0;
}

/* Write one field value as a JSON token (number/"string"/null) into buf.
   Returns bytes written (>= 0). Safe for worker threads. */
static int buf_field_value(const TypedField *tf, const uint8_t *field_ptr,
                           char *buf, size_t bufsz) {
    if (!tf) return snprintf(buf, bufsz, "null");
    char tmp[512];
    int n;
    switch (tf->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(field_ptr, tf->size);
        if (len == 0) return snprintf(buf, bufsz, "\"\"");
        return snprintf(buf, bufsz, "\"%.*s\"", len, (const char *)(field_ptr + 2));
    }
    case FT_DATE:
    case FT_DATETIME:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "\"%s\"", tmp);
    default:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf(buf, bufsz, "null");
        return snprintf(buf, bufsz, "%s", tmp);
    }
}

/* Write one join's contribution (,val,val,...) to buf. remote_raw NULL → nulls. */
static int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf(buf + pos, bufsz - pos, "null");
        else
            pos += buf_field_value(j->proj_tfs[k],
                                   remote_raw + j->proj_tfs[k]->offset,
                                   buf + pos, bufsz - pos);
    }
    return pos;
}

/* Write driver-side row values (,val,val,...) after the key. */
static int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    pos += buf_field_value(&driver_fs->ts->fields[idx],
                                           driver_raw + driver_fs->ts->fields[idx].offset,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf(buf + pos, bufsz - pos, ",");
            pos += buf_field_value(&driver_fs->ts->fields[i],
                                   driver_raw + driver_fs->ts->fields[i].offset,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}

/* Emit the columns header for a joined query (main thread only). */
static void emit_joined_columns(const char *driver_object,
                                FieldSchema *driver_fs,
                                const JoinSpec *joins, int njoins,
                                const char **driver_proj, int driver_proj_count) {
    OUT("{\"columns\":[\"%s.key\"", driver_object);
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++)
            OUT(",\"%s.%s\"", driver_object, driver_proj[i]);
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            OUT(",\"%s.%s\"", driver_object, driver_fs->ts->fields[i].name);
        }
    }
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        if (j->include_remote_key) OUT(",\"%s.key\"", j->as_name);
        for (int k = 0; k < j->proj_count; k++)
            OUT(",\"%s.%s\"", j->as_name, j->proj_fields[k]);
    }
    OUT("],\"rows\":[");
}

int adv_search_cb(const SlotHeader *hdr, const uint8_t *block,
                          void *ctx) {
    AdvSearchCtx *sc = (AdvSearchCtx *)ctx;
    if (sc->limit > 0 && sc->printed >= sc->limit) return 1; /* stop */
    if (query_deadline_tick(sc->deadline, &sc->dl_counter)) return 1;

    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    /* Check excluded keys first */
    if (is_excluded(&sc->excluded, key)) { free(key); return 0; }

    const char *raw = (const char *)block + hdr->key_len;

    /* Check full criteria tree (AND/OR short-circuit) */
    int match = criteria_match_tree((const uint8_t *)raw, sc->tree, sc->fs);

    if (match) {
        /* For joined queries: evaluate joins first; inner-drops are skipped so
           offset/limit reflect the actual emitted row count. */
        FcacheRead *join_handles = NULL;
        const uint8_t **join_raws = NULL;
        int dropped = 0;
        if (sc->njoins > 0) {
            join_handles = calloc(sc->njoins, sizeof(FcacheRead));
            join_raws    = calloc(sc->njoins, sizeof(const uint8_t *));
            for (int i = 0; i < sc->njoins; i++) {
                char lk[1024];
                int llen = extract_local_key(&sc->joins[i], (const uint8_t *)raw,
                                             sc->fs ? sc->fs->ts : NULL, lk, sizeof(lk));
                int found = 0;
                if (llen > 0) {
                    found = lookup_remote(&sc->joins[i], sc->db_root, lk, (size_t)llen,
                                          &join_handles[i], &join_raws[i]);
                }
                if (!found && sc->joins[i].type == JOIN_INNER) { dropped = 1; break; }
            }
        }

        if (!dropped) {
            sc->count++;
            if (sc->count > sc->offset) {
                if (sc->njoins > 0) {
                    /* Tabular row: [driver.key, driver fields..., join1 fields..., ...] */
                    char row[16384];
                    int pos = snprintf(row, sizeof(row), "%s[\"%s\"",
                                       sc->printed ? "," : "", key);
                    pos += buf_driver_values((const uint8_t *)raw, sc->fs,
                                             sc->proj_count > 0 ? sc->proj_fields : NULL,
                                             sc->proj_count,
                                             row + pos, sizeof(row) - pos);
                    for (int i = 0; i < sc->njoins && pos < (int)sizeof(row) - 2; i++)
                        pos += buf_join_values(&sc->joins[i], join_raws[i],
                                               row + pos, sizeof(row) - pos);
                    snprintf(row + pos, sizeof(row) - pos, "]");
                    OUT("%s", row);
                } else if (sc->csv_delim) {
                    csv_emit_row(key, (const uint8_t *)raw, hdr->value_len,
                                 sc->proj_count > 0 ? sc->proj_fields : NULL,
                                 sc->proj_count, sc->fs, sc->csv_delim);
                } else if (sc->rows_fmt) {
                    OUT("%s[\"%s\"", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    } else if (sc->fs && sc->fs->ts) {
                        for (int i = 0; i < sc->fs->ts->nfields; i++) {
                            if (sc->fs->ts->fields[i].removed) continue;
                            char *pv = typed_get_field_str(sc->fs->ts, (const uint8_t *)raw, i);
                            OUT(",\"%s\"", pv ? pv : "");
                            free(pv);
                        }
                    }
                    OUT("]");
                } else if (sc->proj_count > 0) {
                    OUT("%s{\"key\":\"%s\",\"value\":{", sc->printed ? "," : "", key);
                    int first = 1;
                    for (int i = 0; i < sc->proj_count; i++) {
                        char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}}");
                } else {
                    char *val = decode_value(raw, hdr->value_len, sc->fs);
                    OUT("%s{\"key\":\"%s\",\"value\":%s}", sc->printed ? "," : "", key, val);
                    free(val);
                }
                sc->printed++;
            }
        }

        if (sc->njoins > 0) {
            for (int i = 0; i < sc->njoins; i++) {
                if (join_handles[i].map) fcache_release(join_handles[i]);
            }
            free(join_handles);
            free(join_raws);
        }
    }
    free(key);
    return 0;
}

/* ========== Indexed Find: Collect ALL → Batch Process in Parallel ========== */

/* Collecting callback for find — gathers hashes, applies primary filter */
typedef struct {
    CollectedHash *entries;
    size_t count;
    size_t cap;
    int splits;
    int collect_cap;     /* max entries to collect (0 = unlimited) */
    SearchCriterion *primary_crit;   /* pointer into tree — for check_primary pre-filter */
    int check_primary;
    QueryDeadline *deadline;
    int dl_counter;
    int budget_exceeded; /* set when collected bytes would exceed QUERY_BUFFER_MB */
} CollectCtx;

static int collect_hash_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    CollectCtx *cc = (CollectCtx *)ctx;
    if (query_deadline_tick(cc->deadline, &cc->dl_counter)) return -1;

    /* For CONTAINS/LIKE/ENDS: filter on B+ tree value before collecting */
    if (cc->check_primary && cc->primary_crit) {
        char tmp[1028];
        size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
        memcpy(tmp, val, cl); tmp[cl] = '\0';
        if (!match_criterion(tmp, cc->primary_crit)) return 0;
    }

    /* Stop at collection cap (set by caller per batch) */
    if (cc->collect_cap > 0 && (int)cc->count >= cc->collect_cap)
        return -1;

    /* Per-query memory cap — 24 bytes per entry (CollectedHash) */
    if ((cc->count + 1) * sizeof(CollectedHash) > g_query_buffer_max_bytes) {
        cc->budget_exceeded = 1;
        return -1;
    }

    if (cc->count >= cc->cap) {
        cc->cap *= 2;
        cc->entries = realloc(cc->entries, cc->cap * sizeof(CollectedHash));
    }
    CollectedHash *e = &cc->entries[cc->count++];
    memcpy(e->hash, hash16, 16);
    addr_from_hash(hash16, cc->splits, &e->shard_id, &e->start_slot);
    return 0;
}

/* Dispatch B+ tree query based on search operator. Used by find, count, aggregate. */
static void btree_dispatch(const char *idx_path, SearchCriterion *pc,
                           bt_result_cb cb, void *ctx) {
    switch (pc->op) {
        case OP_EQUAL:
            btree_search(idx_path, pc->value, strlen(pc->value), cb, ctx);
            break;
        case OP_GREATER_EQ:
            btree_range(idx_path, pc->value, strlen(pc->value),
                       "\xff\xff\xff\xff", 4, cb, ctx);
            break;
        case OP_GREATER:
            btree_range_ex(idx_path, pc->value, strlen(pc->value), 1,
                          "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        case OP_LESS_EQ:
            btree_range(idx_path, "", 0,
                       pc->value, strlen(pc->value), cb, ctx);
            break;
        case OP_LESS:
            btree_range_ex(idx_path, "", 0, 0,
                          pc->value, strlen(pc->value), 1, cb, ctx);
            break;
        case OP_BETWEEN:
            btree_range(idx_path, pc->value, strlen(pc->value),
                       pc->value2, strlen(pc->value2), cb, ctx);
            break;
        case OP_IN:
            for (int iv = 0; iv < pc->in_count; iv++)
                btree_search(idx_path, pc->in_values[iv],
                            strlen(pc->in_values[iv]), cb, ctx);
            break;
        case OP_STARTS_WITH: {
            size_t plen = strlen(pc->value);
            char end_val[1028];
            memcpy(end_val, pc->value, plen);
            memset(end_val + plen, 0xff, 4);
            btree_range(idx_path, pc->value, plen, end_val, plen + 4, cb, ctx);
            break;
        }
        case OP_NOT_EQUAL:
            /* Two exclusive ranges: everything before + everything after */
            btree_range_ex(idx_path, "", 0, 0,
                          pc->value, strlen(pc->value), 1, cb, ctx);
            btree_range_ex(idx_path, pc->value, strlen(pc->value), 1,
                          "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        default:
            /* Full index scan: contains, like, ends_with, not_like, not_contains, not_in, exists */
            btree_range(idx_path, "", 0, "\xff\xff\xff\xff", 4, cb, ctx);
            break;
    }
}

/* Lightweight counting callback — no hash storage, just increments counter.
   Used by count mode. For single criterion: pure index counter.
   For multi-criteria: fetches record inline to verify all criteria. */
typedef struct {
    SearchCriterion *primary_crit;  /* criteria[primary_idx] — used iff check_primary */
    int check_primary;
    size_t count;
    QueryDeadline *deadline;
    int dl_counter;
} IdxCountCtx;

/* Single-criterion inline counter. No record fetch — btree visit = match. */
static int idx_count_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)hash16;
    IdxCountCtx *ic = (IdxCountCtx *)ctx;
    if (query_deadline_tick(ic->deadline, &ic->dl_counter)) return -1;  /* stop btree walk */
    if (ic->check_primary) {
        char tmp[1028];
        size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
        memcpy(tmp, val, cl); tmp[cl] = '\0';
        if (!match_criterion(tmp, ic->primary_crit)) return 0;
    }
    ic->count++;
    return 0;
}

/* Per-shard worker — opens shard once, processes all entries, applies secondary filters */
typedef struct {
    char *key;
    char *json;
} MatchResult;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    CriteriaNode *tree;            /* compiled criteria tree; full re-match per candidate */
    ExcludedKeys *excluded;
    const char **proj_fields;
    int proj_count;
    FieldSchema *fs;
    /* Joins (tabular path when njoins > 0) */
    JoinSpec *joins;
    int njoins;
    QueryDeadline *deadline;
    int dl_counter;
    MatchResult *results;
    int result_count;
    int result_cap;
} ShardWorkCtx;

static void *shard_find_worker(void *arg) {
    ShardWorkCtx *sw = (ShardWorkCtx *)arg;
    if (sw->entry_count == 0) return NULL;

    int sid = sw->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, sid);
    int fd = open(shard, O_RDONLY);
    if (fd < 0) return NULL;
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return NULL; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;
    if ((size_t)st.st_size < SHARD_HDR_SIZE) { munmap(map, st.st_size); return NULL; }
    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) { munmap(map, st.st_size); return NULL; }
    uint32_t slots = sh->slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sw->entry_count; ei++) {
        if (query_deadline_tick(sw->deadline, &sw->dl_counter)) break;
        CollectedHash *e = &sw->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;

            char *key = malloc(h->key_len + 1);
            memcpy(key, map + zoneB_off(s, slots, sw->sch->slot_size), h->key_len);
            key[h->key_len] = '\0';

            if (is_excluded(sw->excluded, key)) { free(key); break; }

            const char *raw = (const char *)(map + zoneB_off(s, slots, sw->sch->slot_size) + h->key_len);
            size_t raw_len = h->value_len;

            int match = criteria_match_tree((const uint8_t *)raw, sw->tree, sw->fs);

            if (match) {
                /* Apply joins if present — inner-drop on no-match. */
                FcacheRead *jhs = NULL;
                const uint8_t **jraws = NULL;
                int dropped = 0;
                if (sw->njoins > 0) {
                    jhs = calloc(sw->njoins, sizeof(FcacheRead));
                    jraws = calloc(sw->njoins, sizeof(const uint8_t *));
                    for (int i = 0; i < sw->njoins; i++) {
                        char lk[1024];
                        int llen = extract_local_key(&sw->joins[i], (const uint8_t *)raw,
                                                     sw->fs ? sw->fs->ts : NULL,
                                                     lk, sizeof(lk));
                        int found = 0;
                        if (llen > 0)
                            found = lookup_remote(&sw->joins[i], sw->db_root, lk, (size_t)llen,
                                                  &jhs[i], &jraws[i]);
                        if (!found && sw->joins[i].type == JOIN_INNER) { dropped = 1; break; }
                    }
                }

                if (!dropped) {
                    if (sw->result_count >= sw->result_cap) {
                        sw->result_cap = sw->result_cap ? sw->result_cap * 2 : 64;
                        sw->results = realloc(sw->results, sw->result_cap * sizeof(MatchResult));
                    }
                    MatchResult *mr = &sw->results[sw->result_count++];
                    mr->key = key;
                    if (sw->njoins > 0) {
                        /* Build tabular row string: [driver.key, driver fields..., join fields...] */
                        size_t bsz = 16384;
                        mr->json = malloc(bsz);
                        int pos = snprintf(mr->json, bsz, "[\"%s\"", key);
                        pos += buf_driver_values((const uint8_t *)raw, sw->fs,
                                                 sw->proj_count > 0 ? sw->proj_fields : NULL,
                                                 sw->proj_count,
                                                 mr->json + pos, bsz - pos);
                        for (int i = 0; i < sw->njoins && pos < (int)bsz - 2; i++)
                            pos += buf_join_values(&sw->joins[i], jraws[i],
                                                   mr->json + pos, bsz - pos);
                        snprintf(mr->json + pos, bsz - pos, "]");
                    } else if (sw->proj_count > 0) {
                        size_t bsz = 256 + sw->proj_count * 256;
                        mr->json = malloc(bsz);
                        int pos = snprintf(mr->json, bsz, "{");
                        int first = 1;
                        for (int fi = 0; fi < sw->proj_count; fi++) {
                            char *pv = decode_field(raw, raw_len, sw->proj_fields[fi], sw->fs);
                            if (!pv) continue;
                            pos += snprintf(mr->json + pos, bsz - pos, "%s\"%s\":\"%s\"",
                                           first ? "" : ",", sw->proj_fields[fi], pv);
                            first = 0; free(pv);
                        }
                        snprintf(mr->json + pos, bsz - pos, "}");
                    } else {
                        mr->json = decode_value(raw, raw_len, sw->fs);
                    }
                    key = NULL;
                }

                if (sw->njoins > 0) {
                    for (int i = 0; i < sw->njoins; i++)
                        if (jhs[i].map) fcache_release(jhs[i]);
                    free(jhs); free(jraws);
                }
            }
            free(key);
            break;
        }
    }
    munmap(map, st.st_size);
    return NULL;
}

/* ========== Indexed count with multi-criteria: parallel per-shard ==========
   Mirrors shard_find_worker's shape but accumulates a counter instead of
   collecting MatchResult — called when cmd_count has secondary criteria. */
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    CriteriaNode *tree;     /* compiled tree; full re-match per candidate */
    FieldSchema *fs;
    QueryDeadline *deadline;
    int dl_counter;
    size_t count;           /* result: matches in this shard */
} ShardCountCtx;

static void *shard_count_worker(void *arg) {
    ShardCountCtx *sc = (ShardCountCtx *)arg;
    if (sc->entry_count == 0) return NULL;

    int sid = sc->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sc->db_root, sc->object, sid);
    int fd = open(shard, O_RDONLY);
    if (fd < 0) return NULL;
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return NULL; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;
    if ((size_t)st.st_size < SHARD_HDR_SIZE) { munmap(map, st.st_size); return NULL; }
    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) { munmap(map, st.st_size); return NULL; }
    uint32_t slots = sh->slots_per_shard;
    uint32_t mask = slots - 1;

    size_t local = 0;
    for (int ei = 0; ei < sc->entry_count; ei++) {
        if (query_deadline_tick(sc->deadline, &sc->dl_counter)) break;
        CollectedHash *e = &sc->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;

            const uint8_t *raw = map + zoneB_off(s, slots, sc->sch->slot_size) + h->key_len;
            if (criteria_match_tree(raw, sc->tree, sc->fs)) local++;
            break;
        }
    }
    munmap(map, st.st_size);
    sc->count = local;
    return NULL;
}

/* Orchestrate parallel indexed count: qsort by shard, fan out per-shard workers. */
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl) {
    if (batch_count == 0) return 0;
    qsort(batch, batch_count, sizeof(CollectedHash), cmp_by_shard);

    int nshard_groups = 0;
    int group_starts[1024], group_sizes[1024];
    int prev_sid = -1;
    for (int i = 0; i < batch_count && nshard_groups < 1024; i++) {
        if (batch[i].shard_id != prev_sid) {
            group_starts[nshard_groups] = i;
            if (nshard_groups > 0)
                group_sizes[nshard_groups - 1] = i - group_starts[nshard_groups - 1];
            prev_sid = batch[i].shard_id;
            nshard_groups++;
        }
    }
    if (nshard_groups > 0)
        group_sizes[nshard_groups - 1] = batch_count - group_starts[nshard_groups - 1];

    ShardCountCtx *workers = calloc(nshard_groups, sizeof(ShardCountCtx));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &batch[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].tree = tree;
        workers[g].fs = fs;
        workers[g].deadline = dl;
    }

    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++) shard_count_worker(&workers[g]);
    } else {
        parallel_for(shard_count_worker, workers, nshard_groups, sizeof(ShardCountCtx));
    }

    size_t total = 0;
    for (int g = 0; g < nshard_groups; g++) total += workers[g].count;
    free(workers);
    return total;
}

/* Process a batch of hashes: group by shard, parallel shard reads + filter.
   Returns total matches found. Appends output to stdout. */
static int process_batch(CollectedHash *batch, int batch_count,
                         const char *db_root, const char *object, const Schema *sch,
                         CriteriaNode *tree,
                         ExcludedKeys *excluded, const char **proj_fields, int proj_count,
                         FieldSchema *fs, int offset, int limit, int *count, int *printed,
                         int rows_fmt, char csv_delim,
                         JoinSpec *joins, int njoins, QueryDeadline *dl) {

    qsort(batch, batch_count, sizeof(CollectedHash), cmp_by_shard);

    /* Group by shard_id */
    int nshard_groups = 0;
    int group_starts[1024], group_sizes[1024]; /* max 1K shards in a batch */
    int prev_sid = -1;
    for (int i = 0; i < batch_count && nshard_groups < 1024; i++) {
        if (batch[i].shard_id != prev_sid) {
            group_starts[nshard_groups] = i;
            if (nshard_groups > 0) group_sizes[nshard_groups - 1] = i - group_starts[nshard_groups - 1];
            prev_sid = batch[i].shard_id;
            nshard_groups++;
        }
    }
    if (nshard_groups > 0) group_sizes[nshard_groups - 1] = batch_count - group_starts[nshard_groups - 1];

    /* Allocate workers */
    ShardWorkCtx *workers = calloc(nshard_groups, sizeof(ShardWorkCtx));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &batch[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].tree = tree;
        workers[g].excluded = excluded;
        workers[g].proj_fields = proj_fields;
        workers[g].proj_count = proj_count;
        workers[g].fs = fs;
        workers[g].joins = joins;
        workers[g].njoins = njoins;
        workers[g].deadline = dl;
    }

    /* < 1K entries: sequential (grouped by shard, each shard opened once — no thread overhead)
       >= 1K entries: parallel across shard groups */
    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++)
            shard_find_worker(&workers[g]);
    } else {
        parallel_for(shard_find_worker, workers, nshard_groups, sizeof(ShardWorkCtx));
    }

    /* Output matches, respecting offset/limit */
    int batch_matches = 0;
    for (int g = 0; g < nshard_groups && (limit <= 0 || *printed < limit); g++) {
        for (int r = 0; r < workers[g].result_count && (limit <= 0 || *printed < limit); r++) {
            (*count)++;
            batch_matches++;
            if (*count > offset) {
                MatchResult *mr = &workers[g].results[r];
                if (njoins > 0) {
                    /* mr->json already holds the full tabular row "[...]"; just emit. */
                    OUT("%s%s", *printed ? "," : "", mr->json);
                } else if (csv_delim || rows_fmt) {
                    /* Projection emit — parse mr->json once per row rather than
                       walking it N times (once per projected field). On a
                       100-match find projecting 3 fields this drops from 300
                       json walks to 100 parses + 300 O(1) struct lookups. */
                    JsonObj mjo;
                    json_parse_object(mr->json, strlen(mr->json), &mjo);
                    if (csv_delim) {
                        csv_emit_cell(mr->key, csv_delim);
                        if (proj_count > 0) {
                            for (int fi = 0; fi < proj_count; fi++) {
                                char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                                char *pv = json_obj_strdup(&mjo, proj_fields[fi]);
                                csv_emit_cell(pv, csv_delim);
                                free(pv);
                            }
                        } else if (fs && fs->ts) {
                            for (int fi = 0; fi < fs->ts->nfields; fi++) {
                                if (fs->ts->fields[fi].removed) continue;
                                char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                                char *pv = json_obj_strdup(&mjo, fs->ts->fields[fi].name);
                                csv_emit_cell(pv, csv_delim);
                                free(pv);
                            }
                        }
                        OUT("\n");
                    } else {
                        /* rows_fmt */
                        OUT("%s[\"%s\"", *printed ? "," : "", mr->key);
                        if (proj_count > 0) {
                            for (int fi = 0; fi < proj_count; fi++) {
                                char *pv = json_obj_strdup(&mjo, proj_fields[fi]);
                                OUT(",\"%s\"", pv ? pv : "");
                                free(pv);
                            }
                        } else if (fs && fs->ts) {
                            for (int fi = 0; fi < fs->ts->nfields; fi++) {
                                if (fs->ts->fields[fi].removed) continue;
                                char *pv = json_obj_strdup(&mjo, fs->ts->fields[fi].name);
                                OUT(",\"%s\"", pv ? pv : "");
                                free(pv);
                            }
                        }
                        OUT("]");
                    }
                } else {
                    OUT("%s{\"key\":\"%s\",\"value\":%s}", *printed ? "," : "", mr->key, mr->json);
                }
                (*printed)++;
            }
        }
    }

    /* Cleanup */
    for (int g = 0; g < nshard_groups; g++) {
        for (int r = 0; r < workers[g].result_count; r++) {
            free(workers[g].results[r].key);
            free(workers[g].results[r].json);
        }
        free(workers[g].results);
    }
    free(workers);
    return batch_matches;
}

/* Orchestrate: collect hashes from B+ tree, group by shard, process */
static int idx_find_parallel(const char *db_root, const char *object, const Schema *sch,
                             const char *primary_idx_path, CriteriaNode *tree,
                             SearchCriterion *primary_crit, int check_primary,
                             ExcludedKeys *excluded,
                             int offset, int limit, const char **proj_fields, int proj_count,
                             FieldSchema *fs, int rows_fmt, char csv_delim,
                             JoinSpec *joins, int njoins, QueryDeadline *dl) {
    /* Collection cap heuristic:
       - No joins, or all-LEFT joins: every collected hash emits at most one row,
         so we can safely cap at offset+limit.
       - Any INNER join: records may drop, so we must collect unbounded and
         apply limit at emission time. */
    int all_left_or_none = 1;
    for (int i = 0; i < njoins; i++)
        if (joins[i].type != JOIN_LEFT) { all_left_or_none = 0; break; }
    int collect_target = (limit > 0 && all_left_or_none) ? offset + limit : 0;
    CollectCtx cc;
    memset(&cc, 0, sizeof(cc));
    cc.cap = (collect_target > 0 && collect_target < 4096) ? collect_target : 4096;
    cc.entries = malloc(cc.cap * sizeof(CollectedHash));
    cc.splits = sch->splits;
    cc.collect_cap = collect_target;
    cc.primary_crit = primary_crit;
    cc.check_primary = check_primary;
    cc.deadline = dl;

    btree_dispatch(primary_idx_path, primary_crit, collect_hash_cb, &cc);

    if (cc.budget_exceeded) { free(cc.entries); return -2; }  /* -2 = budget exceeded */
    if (cc.count == 0) { free(cc.entries); return 0; }

    int count = 0, printed = 0;
    process_batch(cc.entries, cc.count, db_root, object, sch,
                 tree, excluded,
                 proj_fields, proj_count, fs, offset, limit, &count, &printed,
                 rows_fmt, csv_delim, joins, njoins, dl);

    free(cc.entries);
    return printed;
}

enum SearchOp parse_op(const char *s) {
    if (strcmp(s, "eq") == 0 || strcmp(s, "equal") == 0) return OP_EQUAL;
    if (strcmp(s, "neq") == 0 || strcmp(s, "not_equal") == 0) return OP_NOT_EQUAL;
    if (strcmp(s, "lt") == 0 || strcmp(s, "less") == 0) return OP_LESS;
    if (strcmp(s, "gt") == 0 || strcmp(s, "greater") == 0) return OP_GREATER;
    if (strcmp(s, "lte") == 0 || strcmp(s, "less_eq") == 0) return OP_LESS_EQ;
    if (strcmp(s, "gte") == 0 || strcmp(s, "greater_eq") == 0) return OP_GREATER_EQ;
    if (strcmp(s, "like") == 0) return OP_LIKE;
    if (strcmp(s, "nlike") == 0 || strcmp(s, "not_like") == 0) return OP_NOT_LIKE;
    if (strcmp(s, "contains") == 0) return OP_CONTAINS;
    if (strcmp(s, "ncontains") == 0 || strcmp(s, "not_contains") == 0) return OP_NOT_CONTAINS;
    if (strcmp(s, "starts") == 0 || strcmp(s, "starts_with") == 0) return OP_STARTS_WITH;
    if (strcmp(s, "ends") == 0 || strcmp(s, "ends_with") == 0) return OP_ENDS_WITH;
    if (strcmp(s, "in") == 0) return OP_IN;
    if (strcmp(s, "nin") == 0 || strcmp(s, "not_in") == 0) return OP_NOT_IN;
    if (strcmp(s, "between") == 0) return OP_BETWEEN;
    if (strcmp(s, "exists") == 0) return OP_EXISTS;
    if (strcmp(s, "nexists") == 0 || strcmp(s, "not_exists") == 0) return OP_NOT_EXISTS;
    return OP_EQUAL;
}

/* ========== Reusable criteria parser ========== */

/* Parse a single criterion object {"field":"x","op":"eq","value":"y"} into *c.
   Helper for parse_criteria_json. Called in a loop over every element of
   the criteria array; parsing the sub-object once and indexing into the
   JsonObj saves 5 walks per criterion over the previous per-field parse. */
static void parse_one_criterion(const char *obj_buf, SearchCriterion *c) {
    memset(c, 0, sizeof(*c));

    JsonObj cobj;
    json_parse_object(obj_buf, strlen(obj_buf), &cobj);

    char *f     = json_obj_strdup(&cobj, "field");
    char *o     = json_obj_strdup(&cobj, "op");
    char *v     = json_obj_strdup(&cobj, "value");
    char *v_raw = json_obj_strdup_raw(&cobj, "value");
    char *v2    = json_obj_strdup(&cobj, "value2");

    if (f) { strncpy(c->field, f, 255); free(f); }
    if (o) { c->op = parse_op(o); free(o); }
    if (v) {
        strncpy(c->value, v, sizeof(c->value) - 1);
        /* LIKE/NOT_LIKE accept '*' as an alias for '%'. Normalize once
           so both the typed and legacy match paths see a single form. */
        if (c->op == OP_LIKE || c->op == OP_NOT_LIKE) {
            for (char *q = c->value; *q; q++) if (*q == '*') *q = '%';
        }
        if (c->op == OP_IN || c->op == OP_NOT_IN) {
            c->in_cap = 64;
            c->in_values = malloc(c->in_cap * sizeof(char *));
            const char *ap = v_raw ? v_raw : v;
            if (*ap == '[') {
                ap++;
                while (*ap) {
                    while (*ap == ' ' || *ap == ',') ap++;
                    if (*ap == ']') break;
                    if (*ap == '"') {
                        ap++;
                        const char *start = ap;
                        while (*ap && *ap != '"') ap++;
                        size_t len = ap - start;
                        if (c->in_count >= c->in_cap) {
                            c->in_cap *= 2;
                            c->in_values = realloc(c->in_values, c->in_cap * sizeof(char *));
                        }
                        char *val = malloc(len + 1);
                        memcpy(val, start, len); val[len] = '\0';
                        c->in_values[c->in_count++] = val;
                        if (*ap == '"') ap++;
                    } else ap++;
                }
            } else {
                char *iv = strdup(v);
                char *_tok_save = NULL; char *tok = strtok_r(iv, ",", &_tok_save);
                while (tok) {
                    if (c->in_count >= c->in_cap) {
                        c->in_cap *= 2;
                        c->in_values = realloc(c->in_values, c->in_cap * sizeof(char *));
                    }
                    c->in_values[c->in_count++] = strdup(tok);
                    tok = strtok_r(NULL, ",", &_tok_save);
                }
                free(iv);
            }
        }
        free(v);
    }
    free(v_raw);
    if (v2) { strncpy(c->value2, v2, sizeof(c->value2) - 1); free(v2); }
}

/* Parse criteria from JSON — supports two forms:
   Array form:  [{"field":"x","op":"eq","value":"y"}, ...]
   Simple form: {"status":"pending","city":"London"}  (all equality)
   Returns heap-allocated array in *out, count in *count. Caller must free_criteria(). */
int parse_criteria_json(const char *json, SearchCriterion **out, int *count) {
    const char *p = json_skip(json);

    if (*p == '[') {
        /* Array form — same as old cmd_find inline parser */
        SearchCriterion *criteria = calloc(64, sizeof(SearchCriterion));
        int n = 0;
        p++;
        while (*p && n < 64) {
            p = json_skip(p);
            if (*p == ']') break;
            if (*p == ',') { p++; continue; }
            if (*p != '{') { p++; continue; }

            const char *obj_start = p;
            const char *obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;
            char obj_buf[MAX_LINE];
            if (obj_len >= sizeof(obj_buf)) { p = obj_end; continue; }
            memcpy(obj_buf, obj_start, obj_len);
            obj_buf[obj_len] = '\0';

            parse_one_criterion(obj_buf, &criteria[n]);
            n++;
            p = obj_end;
        }
        *out = criteria;
        *count = n;
        return 0;
    } else if (*p == '{') {
        /* Simple equality form: {"field1":"val1","field2":"val2"} */
        SearchCriterion *criteria = calloc(64, sizeof(SearchCriterion));
        int n = 0;
        p++;
        while (*p && n < 64) {
            p = json_skip(p);
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') { p++; continue; }

            /* Parse key */
            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;

            p = json_skip(p);
            if (*p == ':') p++;
            p = json_skip(p);

            /* Parse value */
            const char *vstart = p;
            const char *vend = json_skip_value(p);
            size_t vlen = vend - vstart;

            SearchCriterion *c = &criteria[n];
            memset(c, 0, sizeof(*c));
            if (flen > 255) flen = 255;
            memcpy(c->field, fname, flen);
            c->field[flen] = '\0';
            c->op = OP_EQUAL;

            /* Strip quotes from value */
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') {
                vlen -= 2; vstart++;
            }
            if (vlen > sizeof(c->value) - 1) vlen = sizeof(c->value) - 1;
            memcpy(c->value, vstart, vlen);
            c->value[vlen] = '\0';

            n++;
            p = vend;
        }
        *out = criteria;
        *count = n;
        return 0;
    }

    *out = NULL;
    *count = 0;
    return -1;
}

void free_criteria(SearchCriterion *c, int count) {
    if (!c) return;
    for (int i = 0; i < count; i++) {
        if (c[i].in_values) {
            for (int j = 0; j < c[i].in_count; j++) free(c[i].in_values[j]);
            free(c[i].in_values);
        }
    }
    free(c);
}

/* ========== CriteriaNode tree parser (AND/OR composition) ========== */

static CriteriaNode *cnode_new(CriteriaNodeKind kind) {
    CriteriaNode *n = calloc(1, sizeof(CriteriaNode));
    if (n) n->kind = kind;
    return n;
}

static int cnode_append(CriteriaNode *parent, CriteriaNode *child) {
    CriteriaNode **nc = realloc(parent->children,
                                (parent->n_children + 1) * sizeof(CriteriaNode *));
    if (!nc) return -1;
    parent->children = nc;
    parent->children[parent->n_children++] = child;
    return 0;
}

void free_criteria_tree(CriteriaNode *n) {
    if (!n) return;
    if (n->kind == CNODE_LEAF) {
        if (n->leaf.in_values) {
            for (int i = 0; i < n->leaf.in_count; i++) free(n->leaf.in_values[i]);
            free(n->leaf.in_values);
        }
        if (n->compiled) {
            free_compiled_criteria(n->compiled, 1);
        }
    } else {
        for (int i = 0; i < n->n_children; i++) free_criteria_tree(n->children[i]);
        free(n->children);
    }
    free(n);
}

void compile_criteria_tree(CriteriaNode *n, const TypedSchema *ts) {
    if (!n) return;
    if (n->kind == CNODE_LEAF) {
        if (!n->compiled) {
            n->compiled = calloc(1, sizeof(CompiledCriterion));
            if (n->compiled) compile_one(n->compiled, &n->leaf, ts);
        }
        return;
    }
    for (int i = 0; i < n->n_children; i++) compile_criteria_tree(n->children[i], ts);
}

int criteria_match_tree(const uint8_t *rec, const CriteriaNode *n, FieldSchema *fs) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF:
        if (!n->compiled) return 0;
        return match_typed(rec, n->compiled, fs);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++)
            if (!criteria_match_tree(rec, n->children[i], fs)) return 0;
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (criteria_match_tree(rec, n->children[i], fs)) return 1;
        return 0;
    }
    return 0;
}

/* Forward decl — parse_tree_element and parse_tree_array recurse through each other. */
static CriteriaNode *parse_tree_element(const char *obj_buf, int depth, const char **err);

/* Parse the children of an array `[elem, elem, ...]` into parent. `arr_p` must
   point at the opening '['. Each element is a leaf or a branch object. */
static int parse_tree_array(const char *arr_p, CriteriaNode *parent,
                            int depth, const char **err) {
    if (depth > MAX_CRITERIA_DEPTH) {
        if (err) *err = "nesting too deep (max 16)";
        return -1;
    }
    const char *p = json_skip(arr_p);
    if (*p != '[') { if (err) *err = "expected array"; return -1; }
    p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '{') { if (err) *err = "expected object in criteria array"; return -1; }

        const char *obj_start = p;
        const char *obj_end = json_skip_value(p);
        size_t obj_len = obj_end - obj_start;
        char *obj_buf = malloc(obj_len + 1);
        if (!obj_buf) { if (err) *err = "out of memory"; return -1; }
        memcpy(obj_buf, obj_start, obj_len);
        obj_buf[obj_len] = '\0';

        CriteriaNode *child = parse_tree_element(obj_buf, depth + 1, err);
        free(obj_buf);
        if (!child) return -1;

        if (cnode_append(parent, child) != 0) {
            free_criteria_tree(child);
            if (err) *err = "out of memory";
            return -1;
        }
        p = obj_end;
    }
    return 0;
}

/* Parse a single element: either a branch `{or:[...]}` / `{and:[...]}` or a leaf
   `{field,op,value,...}`. Returns newly-allocated node or NULL on error. */
static CriteriaNode *parse_tree_element(const char *obj_buf, int depth,
                                        const char **err) {
    if (depth > MAX_CRITERIA_DEPTH) {
        if (err) *err = "nesting too deep (max 16)";
        return NULL;
    }

    JsonObj tobj;
    json_parse_object(obj_buf, strlen(obj_buf), &tobj);
    char *or_arr  = json_obj_strdup_raw(&tobj, "or");
    char *and_arr = or_arr ? NULL : json_obj_strdup_raw(&tobj, "and");

    if (or_arr || and_arr) {
        CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
        if (!n) { free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL; }
        const char *arr = or_arr ? or_arr : and_arr;
        if (parse_tree_array(arr, n, depth, err) != 0) {
            free_criteria_tree(n); free(or_arr); free(and_arr);
            return NULL;
        }
        if (n->n_children == 0) {
            free_criteria_tree(n); free(or_arr); free(and_arr);
            if (err) *err = "empty or/and";
            return NULL;
        }
        free(or_arr); free(and_arr);
        return n;
    }

    CriteriaNode *n = cnode_new(CNODE_LEAF);
    if (!n) { if (err) *err = "out of memory"; return NULL; }
    parse_one_criterion(obj_buf, &n->leaf);
    if (n->leaf.field[0] == '\0') {
        free_criteria_tree(n);
        if (err) *err = "leaf missing 'field'";
        return NULL;
    }
    return n;
}

CriteriaNode *parse_criteria_tree(const char *json, const char **err) {
    if (err) *err = NULL;
    if (!json || !json[0]) return NULL;

    const char *p = json_skip(json);
    if (!*p) return NULL;

    if (*p == '[') {
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        if (parse_tree_array(p, root, 0, err) != 0) {
            free_criteria_tree(root);
            return NULL;
        }
        if (root->n_children == 0) {
            free_criteria_tree(root);
            return NULL;
        }
        return root;
    }

    if (*p == '{') {
        JsonObj pobj;
        json_parse_object(p, strlen(p), &pobj);
        char *or_arr  = json_obj_strdup_raw(&pobj, "or");
        char *and_arr = or_arr ? NULL : json_obj_strdup_raw(&pobj, "and");
        if (or_arr || and_arr) {
            CriteriaNode *n = cnode_new(or_arr ? CNODE_OR : CNODE_AND);
            if (!n) { free(or_arr); free(and_arr); if (err) *err = "out of memory"; return NULL; }
            if (parse_tree_array(or_arr ? or_arr : and_arr, n, 0, err) != 0) {
                free_criteria_tree(n); free(or_arr); free(and_arr);
                return NULL;
            }
            if (n->n_children == 0) {
                free_criteria_tree(n); free(or_arr); free(and_arr);
                if (err) *err = "empty or/and";
                return NULL;
            }
            free(or_arr); free(and_arr);
            return n;
        }

        const char *field_v; size_t field_vl;
        if (json_obj_get(&pobj, "field", &field_v, &field_vl)) {
            CriteriaNode *n = cnode_new(CNODE_LEAF);
            if (!n) { if (err) *err = "out of memory"; return NULL; }
            parse_one_criterion(p, &n->leaf);
            return n;
        }

        /* Simple-equality form `{"k1":"v1","k2":"v2"}` — backward compat.
           Parse k:v pairs as EQ leaves under an implicit AND root. */
        CriteriaNode *root = cnode_new(CNODE_AND);
        if (!root) { if (err) *err = "out of memory"; return NULL; }
        p++;
        while (*p) {
            p = json_skip(p);
            if (*p == '}') break;
            if (*p == ',') { p++; continue; }
            if (*p != '"') { p++; continue; }
            p++;
            const char *fname = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t flen = p - fname;
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p++;
            p = json_skip(p);
            const char *vstart = p;
            const char *vend = json_skip_value(p);
            size_t vlen = vend - vstart;

            CriteriaNode *leaf = cnode_new(CNODE_LEAF);
            if (!leaf) { free_criteria_tree(root); if (err) *err = "out of memory"; return NULL; }
            if (flen > 255) flen = 255;
            memcpy(leaf->leaf.field, fname, flen);
            leaf->leaf.field[flen] = '\0';
            leaf->leaf.op = OP_EQUAL;
            if (vlen >= 2 && *vstart == '"' && *(vend - 1) == '"') { vlen -= 2; vstart++; }
            if (vlen > sizeof(leaf->leaf.value) - 1) vlen = sizeof(leaf->leaf.value) - 1;
            memcpy(leaf->leaf.value, vstart, vlen);
            leaf->leaf.value[vlen] = '\0';

            if (cnode_append(root, leaf) != 0) {
                free_criteria_tree(leaf); free_criteria_tree(root);
                if (err) *err = "out of memory";
                return NULL;
            }
            p = vend;
        }
        if (root->n_children == 0) { free_criteria_tree(root); return NULL; }
        return root;
    }

    if (err) *err = "criteria must be array or object";
    return NULL;
}

/* ========== Criteria tree planner ========== */

/* Is the leaf's field indexable AND does the operator make a useful btree range?
   Returns 1 and fills out_idx_path on success, 0 otherwise. */
static int leaf_is_indexed(const SearchCriterion *c, const char *db_root,
                           const char *object, char *out_idx_path, size_t out_sz) {
    if (!c || !c->field[0]) return 0;
    if (c->op == OP_NOT_EXISTS) return 0;  /* missing field → not in index */
    char p[PATH_MAX];
    snprintf(p, sizeof(p), "%s/%s/indexes/%s.idx", db_root, object, c->field);
    struct stat st;
    if (stat(p, &st) != 0 || st.st_size == 0) return 0;
    if (out_idx_path) strncpy(out_idx_path, p, out_sz - 1);
    return 1;
}

/* Walk immediate children of an AND root looking for the first indexable leaf.
   Single-LEAF roots are treated as an AND of one. Returns the leaf's
   SearchCriterion* (pointer into the tree — do not free). */
static SearchCriterion *find_primary_leaf(CriteriaNode *root,
                                          const char *db_root, const char *object,
                                          char *out_idx_path, size_t out_sz) {
    if (!root) return NULL;
    if (root->kind == CNODE_LEAF) {
        if (leaf_is_indexed(&root->leaf, db_root, object, out_idx_path, out_sz))
            return &root->leaf;
        return NULL;
    }
    if (root->kind == CNODE_AND) {
        for (int i = 0; i < root->n_children; i++) {
            CriteriaNode *c = root->children[i];
            if (c->kind == CNODE_LEAF &&
                leaf_is_indexed(&c->leaf, db_root, object, out_idx_path, out_sz))
                return &c->leaf;
        }
    }
    return NULL;
}

/* True if every child of `or_node` is a LEAF with an index. Nested AND/OR inside
   OR disqualifies (keeps the planner simple — those fall to full scan). */
static int or_all_children_indexed(const CriteriaNode *or_node,
                                   const char *db_root, const char *object) {
    if (!or_node || or_node->kind != CNODE_OR) return 0;
    for (int i = 0; i < or_node->n_children; i++) {
        CriteriaNode *c = or_node->children[i];
        if (c->kind != CNODE_LEAF) return 0;
        if (!leaf_is_indexed(&c->leaf, db_root, object, NULL, 0)) return 0;
    }
    return or_node->n_children > 0;
}

/* Look for an OR child of the root AND (or root itself if root is OR) whose
   children are all indexed. Used when no AND leaf is indexable — we can still
   narrow candidates via an OR-union fast path. Returns the OR node, or NULL. */
static CriteriaNode *find_fully_indexed_or(CriteriaNode *root,
                                           const char *db_root, const char *object) {
    if (!root) return NULL;
    if (root->kind == CNODE_OR) {
        return or_all_children_indexed(root, db_root, object) ? root : NULL;
    }
    if (root->kind == CNODE_AND) {
        for (int i = 0; i < root->n_children; i++) {
            CriteriaNode *c = root->children[i];
            if (c->kind == CNODE_OR && or_all_children_indexed(c, db_root, object))
                return c;
        }
    }
    return NULL;
}

typedef enum {
    PRIMARY_NONE,     /* full scan */
    PRIMARY_LEAF,     /* single indexed leaf drives btree_range */
    PRIMARY_KEYSET    /* OR sub-tree drives index-union into a KeySet */
} PrimaryKind;

typedef struct {
    PrimaryKind kind;
    SearchCriterion *primary_leaf;   /* PRIMARY_LEAF */
    char primary_idx_path[PATH_MAX];
    CriteriaNode *or_node;           /* PRIMARY_KEYSET */
} QueryPlan;

static QueryPlan choose_primary_source(CriteriaNode *tree,
                                       const char *db_root, const char *object) {
    QueryPlan p = {0};
    if (!tree) { p.kind = PRIMARY_NONE; return p; }

    char idx_path[PATH_MAX] = "";
    SearchCriterion *leaf = find_primary_leaf(tree, db_root, object,
                                              idx_path, sizeof(idx_path));
    if (leaf) {
        p.kind = PRIMARY_LEAF;
        p.primary_leaf = leaf;
        strncpy(p.primary_idx_path, idx_path, sizeof(p.primary_idx_path) - 1);
        return p;
    }

    CriteriaNode *or_node = find_fully_indexed_or(tree, db_root, object);
    if (or_node) {
        p.kind = PRIMARY_KEYSET;
        p.or_node = or_node;
        return p;
    }

    p.kind = PRIMARY_NONE;
    return p;
}

/* ========== OR index-union (KeySet fast path) ========== */

typedef struct {
    const char *db_root;
    const char *object;
    const SearchCriterion *leaf;
    const char *idx_path;
    KeySet *ks;
    QueryDeadline *deadline;
    int dl_counter;
} OrChildWorkerCtx;

/* btree callback — drops every hit into the shared KeySet. */
static int or_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)ctx;
    if (query_deadline_tick(w->deadline, &w->dl_counter)) return -1;
    keyset_insert(w->ks, hash16);
    return 0;
}

static void *or_child_worker(void *arg) {
    OrChildWorkerCtx *w = (OrChildWorkerCtx *)arg;
    btree_dispatch(w->idx_path, (SearchCriterion *)w->leaf, or_collect_cb, w);
    return NULL;
}

/* Build a KeySet by unioning the index lookups of every child of `or_node`.
   Every child must be a LEAF with an index (verified by the planner).
   Caller owns the returned KeySet and must keyset_free() it.
   If the OR's estimated capacity would exceed the per-query buffer cap, returns
   NULL and sets *out_budget_exceeded = 1 when the pointer is non-NULL. */
static KeySet *build_or_keyset(const char *db_root, const char *object,
                               const CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    int n = or_node->n_children;
    if (n <= 0) return NULL;

    /* Estimate total candidate size from index file sizes (very rough: 1 entry
       per 64 bytes of leaf storage). Floor at 1024, cap at 1M to keep memory
       bounded for a single query. */
    size_t est_total = 0;
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        char p[PATH_MAX];
        snprintf(p, sizeof(p), "%s/%s/indexes/%s.idx", db_root, object, c->leaf.field);
        struct stat st;
        if (stat(p, &st) == 0) est_total += (size_t)st.st_size / 64;
    }
    if (est_total < 1024) est_total = 1024;
    if (est_total > 1000000) est_total = 1000000;

    /* Budget check: KeySet alloc is O(cap_pow2 * 24 bytes) — keys[] + state[].
       Cap est_total to fit the per-query buffer cap. */
    size_t ks_cap_guess = est_total * 2; /* keyset_new doubles + rounds up */
    size_t ks_bytes = ks_cap_guess * (sizeof(uint8_t[16]) + sizeof(uint32_t));
    if (ks_bytes > g_query_buffer_max_bytes) {
        if (out_budget_exceeded) *out_budget_exceeded = 1;
        return NULL;
    }

    KeySet *ks = keyset_new(est_total);
    if (!ks) return NULL;

    OrChildWorkerCtx *ctxs = calloc(n, sizeof(OrChildWorkerCtx));
    char (*idx_paths)[PATH_MAX] = calloc(n, sizeof(*idx_paths));
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        snprintf(idx_paths[i], PATH_MAX, "%s/%s/indexes/%s.idx",
                 db_root, object, c->leaf.field);
        ctxs[i].db_root = db_root;
        ctxs[i].object = object;
        ctxs[i].leaf = &c->leaf;
        ctxs[i].idx_path = idx_paths[i];
        ctxs[i].ks = ks;
        ctxs[i].deadline = dl;
    }

    parallel_for(or_child_worker, ctxs, n, sizeof(OrChildWorkerCtx));

    free(ctxs);
    free(idx_paths);
    return ks;
}

/* Read the record at `hash` from the object's shards. Writes the value payload
   start + length into *out_val, *out_len. Returns 0 on success, -1 not found.
   Caller holds the returned pointer only for the duration of the mmap lease. */
typedef struct {
    uint8_t *map;
    size_t map_len;
    const uint8_t *val;
    size_t val_len;
} KeysetRecordRead;

static int read_record_by_hash(const char *db_root, const char *object,
                               const Schema *sch, const uint8_t hash[16],
                               KeysetRecordRead *out) {
    memset(out, 0, sizeof(*out));
    int shard_id, slot;
    addr_from_hash(hash, sch->splits, &shard_id, &slot);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), db_root, object, shard_id);
    int fd = open(shard, O_RDONLY);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < (off_t)SHARD_HDR_SIZE) {
        close(fd); return -1;
    }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return -1;

    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) {
        munmap(map, st.st_size); return -1;
    }
    uint32_t slots = sh->slots_per_shard;
    uint32_t mask  = slots - 1;

    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)slot + p) & mask;
        SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash, 16) != 0) continue;

        out->map = map;
        out->map_len = st.st_size;
        out->val = map + zoneB_off(s, slots, sch->slot_size) + h->key_len;
        out->val_len = h->value_len;
        return 0;
    }
    munmap(map, st.st_size);
    return -1;
}

static void release_record_read(KeysetRecordRead *r) {
    if (r && r->map) { munmap(r->map, r->map_len); r->map = NULL; }
}

/* Emit records matching `tree` by iterating a KeySet built from the OR branch.
   Honours offset/limit at emit time. Excluded keys, joins, projections, and
   rows_fmt all mirror the behaviour of idx_find_parallel / adv_search_cb.
   Returns the number of rows printed. */
static int keyset_find_from_or(const char *db_root, const char *object,
                               const Schema *sch, CriteriaNode *tree,
                               CriteriaNode *or_node,
                               ExcludedKeys *excluded, int offset, int limit,
                               const char **proj_fields, int proj_count,
                               FieldSchema *fs, int rows_fmt, char csv_delim,
                               JoinSpec *joins, int njoins,
                               QueryDeadline *dl, int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, or_node, dl, out_budget_exceeded);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    /* Pure-OR (root itself is the or_node) — every KeySet member matches,
       no need to re-evaluate the tree against each record. */
    int need_rematch = (tree != or_node && tree->kind != CNODE_OR);

    int count = 0, printed = 0;
    int dl_counter = 0;
    for (size_t b = 0; b < ks->cap && (limit <= 0 || printed < limit); b++) {
        if (query_deadline_tick(dl, &dl_counter)) break;
        if (ks->state[b] != 2) continue;

        KeysetRecordRead r;
        if (read_record_by_hash(db_root, object, sch, ks->keys[b], &r) != 0) continue;

        /* Extract key for exclusion + output. */
        uint32_t shard_id, slot;
        (void)shard_id; (void)slot;
        char keybuf[1024];
        /* To get the key, re-scan the shard. But we already matched the slot in
           read_record_by_hash. We can compute the slot from the hash — but we
           lost the key pointer. Simpler: re-open the shard here for the one
           record. For now, extract the key via a follow-up scan. */
        release_record_read(&r);

        /* Re-walk for key + value together. Do it in-line (cheap — one mmap open). */
        int sid, slt;
        addr_from_hash(ks->keys[b], sch->splits, &sid, &slt);
        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, sid);
        int fd = open(shard, O_RDONLY);
        if (fd < 0) continue;
        struct stat st;
        if (fstat(fd, &st) != 0 || st.st_size < (off_t)SHARD_HDR_SIZE) { close(fd); continue; }
        uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (map == MAP_FAILED) continue;
        const ShardHeader *sh = (const ShardHeader *)map;
        if (sh->magic != SHARD_MAGIC) { munmap(map, st.st_size); continue; }
        uint32_t slots = sh->slots_per_shard;
        uint32_t mask = slots - 1;

        int found = 0;
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)slt + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, ks->keys[b], 16) != 0) continue;

            size_t kl = h->key_len < sizeof(keybuf) - 1 ? h->key_len : sizeof(keybuf) - 1;
            memcpy(keybuf, map + zoneB_off(s, slots, sch->slot_size), kl);
            keybuf[kl] = '\0';

            if (is_excluded(excluded, keybuf)) { found = 1; break; }

            const uint8_t *raw = map + zoneB_off(s, slots, sch->slot_size) + h->key_len;
            if (need_rematch && !criteria_match_tree(raw, tree, fs)) { found = 1; break; }

            /* Match. Apply joins (inner-drop), offset, emit. */
            FcacheRead *jhs = NULL;
            const uint8_t **jraws = NULL;
            int dropped = 0;
            if (njoins > 0) {
                jhs = calloc(njoins, sizeof(FcacheRead));
                jraws = calloc(njoins, sizeof(const uint8_t *));
                for (int i = 0; i < njoins; i++) {
                    char lk[1024];
                    int llen = extract_local_key(&joins[i], raw,
                                                 fs ? fs->ts : NULL, lk, sizeof(lk));
                    int jfound = 0;
                    if (llen > 0)
                        jfound = lookup_remote(&joins[i], db_root, lk, (size_t)llen,
                                               &jhs[i], &jraws[i]);
                    if (!jfound && joins[i].type == JOIN_INNER) { dropped = 1; break; }
                }
            }

            if (!dropped) {
                count++;
                if (count > offset && (limit <= 0 || printed < limit)) {
                    if (njoins > 0) {
                        /* Tabular row for joins */
                        char buf[16384];
                        int pos = snprintf(buf, sizeof(buf), "[\"%s\"", keybuf);
                        pos += buf_driver_values(raw, fs,
                                                 proj_count > 0 ? proj_fields : NULL,
                                                 proj_count,
                                                 buf + pos, sizeof(buf) - pos);
                        for (int i = 0; i < njoins; i++) {
                            if (!jraws[i]) {
                                int ncols = joins[i].proj_count > 0
                                            ? joins[i].proj_count
                                            : (joins[i].remote_fs.ts
                                               ? joins[i].remote_fs.ts->nfields : 0);
                                if (joins[i].include_remote_key) ncols++;
                                for (int k = 0; k < ncols; k++)
                                    pos += snprintf(buf + pos, sizeof(buf) - pos, ",null");
                            } else {
                                pos += buf_join_values(&joins[i], jraws[i],
                                                       buf + pos, sizeof(buf) - pos);
                            }
                        }
                        snprintf(buf + pos, sizeof(buf) - pos, "]");
                        OUT("%s%s", printed ? "," : "", buf);
                    } else if (csv_delim) {
                        csv_emit_row(keybuf, raw, h->value_len,
                                     proj_count > 0 ? proj_fields : NULL,
                                     proj_count, fs, csv_delim);
                    } else if (rows_fmt) {
                        OUT("%s[\"%s\"", printed ? "," : "", keybuf);
                        if (proj_count > 0) {
                            for (int j = 0; j < proj_count; j++) {
                                char *pv = decode_field((const char *)raw, h->value_len,
                                                        proj_fields[j], fs);
                                OUT(",\"%s\"", pv ? pv : "");
                                free(pv);
                            }
                        } else if (fs && fs->ts) {
                            for (int j = 0; j < fs->ts->nfields; j++) {
                                if (fs->ts->fields[j].removed) continue;
                                char *pv = typed_get_field_str(fs->ts, raw, j);
                                OUT(",\"%s\"", pv ? pv : "");
                                free(pv);
                            }
                        }
                        OUT("]");
                    } else if (proj_count > 0) {
                        OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", keybuf);
                        int first = 1;
                        for (int j = 0; j < proj_count; j++) {
                            char *pv = decode_field((const char *)raw, h->value_len,
                                                    proj_fields[j], fs);
                            if (!pv) continue;
                            OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                            first = 0;
                            free(pv);
                        }
                        OUT("}}");
                    } else {
                        char *v = decode_value((const char *)raw, h->value_len, fs);
                        OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", keybuf, v);
                        free(v);
                    }
                    printed++;
                }
            }

            if (jhs) {
                for (int i = 0; i < njoins; i++)
                    if (jhs[i].map) fcache_release(jhs[i]);
                free(jhs); free(jraws);
            }
            found = 1;
            break;
        }
        munmap(map, st.st_size);
        if (!found) continue;
    }

    keyset_free(ks);
    return printed;
}

/* Forward decl — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);

/* Run the aggregate pipeline over records keyed by an OR index-union KeySet.
   Tree-match is performed by agg_scan_cb itself (it checks ctx->tree), so
   both Shape C (pure OR) and hybrid (AND + OR) reach the correct records. */
static void keyset_agg_from_or(const char *db_root, const char *object,
                               const Schema *sch, void *agg_ctx,
                               CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, or_node, dl, out_budget_exceeded);
    if (!ks) return;
    if (dl->timed_out) { keyset_free(ks); return; }

    int dl_counter = 0;
    for (size_t b = 0; b < ks->cap; b++) {
        if (query_deadline_tick(dl, &dl_counter)) break;
        if (ks->state[b] != 2) continue;

        int sid, slt;
        addr_from_hash(ks->keys[b], sch->splits, &sid, &slt);
        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, sid);
        int fd = open(shard, O_RDONLY);
        if (fd < 0) continue;
        struct stat st;
        if (fstat(fd, &st) != 0 || st.st_size < (off_t)SHARD_HDR_SIZE) { close(fd); continue; }
        uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (map == MAP_FAILED) continue;
        const ShardHeader *sh = (const ShardHeader *)map;
        if (sh->magic != SHARD_MAGIC) { munmap(map, st.st_size); continue; }
        uint32_t slots = sh->slots_per_shard;
        uint32_t mask = slots - 1;
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)slt + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, ks->keys[b], 16) != 0) continue;
            const uint8_t *block = map + zoneB_off(s, slots, sch->slot_size);
            agg_scan_cb(h, block, agg_ctx);
            break;
        }
        munmap(map, st.st_size);
    }
    keyset_free(ks);
}

/* Count records matching `tree` by iterating a KeySet built from the OR branch.
   For pure-OR trees (root IS the or_node), returns |KeySet| directly.
   For hybrid (AND + OR), re-matches each keyed record against the full tree. */
static size_t keyset_count_from_or(const char *db_root, const char *object,
                                   const Schema *sch, CriteriaNode *tree,
                                   CriteriaNode *or_node, FieldSchema *fs,
                                   QueryDeadline *dl, int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, or_node, dl, out_budget_exceeded);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    /* Pure-OR (root is the OR itself) — no AND siblings, no re-match needed. */
    if (tree == or_node || tree->kind == CNODE_OR) {
        size_t n = keyset_size(ks);
        keyset_free(ks);
        return n;
    }

    /* Hybrid: fetch each keyed record, apply full tree match. */
    size_t n = 0;
    int dl_counter = 0;
    for (size_t b = 0; b < ks->cap; b++) {
        if (query_deadline_tick(dl, &dl_counter)) break;
        if (ks->state[b] != 2) continue;
        KeysetRecordRead r;
        if (read_record_by_hash(db_root, object, sch, ks->keys[b], &r) != 0) continue;
        if (criteria_match_tree(r.val, tree, fs)) n++;
        release_record_read(&r);
    }
    keyset_free(ks);
    return n;
}

/* ========== COUNT with criteria ========== */

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    int count;
    QueryDeadline *deadline;
    int dl_counter;
} CountCtx;

static int count_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    CountCtx *cc = (CountCtx *)ctx;
    if (query_deadline_tick(cc->deadline, &cc->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;
    if (!criteria_match_tree(raw, cc->tree, cc->fs)) return 0;
    cc->count++;
    return 0;
}

int cmd_count(const char *db_root, const char *object, const char *criteria_json) {
    /* No criteria = O(1) from metadata */
    if (!criteria_json || criteria_json[0] == '\0') {
        int n = get_live_count(db_root, object);
        OUT("{\"count\":%d}\n", n);
        return 0;
    }

    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        return 1;
    }
    if (!tree) {
        int n = get_live_count(db_root, object);
        OUT("{\"count\":%d}\n", n);
        return 0;
    }

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    compile_criteria_tree(tree, fs.ts);

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    QueryPlan plan = choose_primary_source(tree, db_root, object);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

    if (plan.kind == PRIMARY_LEAF) {
        SearchCriterion *pc = plan.primary_leaf;
        enum SearchOp op = pc->op;
        int check_primary = (op == OP_CONTAINS || op == OP_LIKE || op == OP_ENDS_WITH ||
                             op == OP_NOT_LIKE || op == OP_NOT_CONTAINS || op == OP_NOT_IN);

        /* Single-leaf tree → inline btree count, no record fetch. */
        int is_single_leaf =
            (tree->kind == CNODE_LEAF) ||
            (tree->kind == CNODE_AND && tree->n_children == 1 &&
             tree->children[0]->kind == CNODE_LEAF);

        if (is_single_leaf) {
            IdxCountCtx ic = { pc, check_primary, 0, &dl, 0 };
            btree_dispatch(plan.primary_idx_path, pc, idx_count_cb, &ic);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("{\"count\":%zu}\n", ic.count);
        } else {
            CollectCtx cc;
            memset(&cc, 0, sizeof(cc));
            cc.cap = 4096;
            cc.entries = malloc(cc.cap * sizeof(CollectedHash));
            cc.splits = sch.splits;
            cc.primary_crit = pc;
            cc.check_primary = check_primary;
            cc.deadline = &dl;
            btree_dispatch(plan.primary_idx_path, pc, collect_hash_cb, &cc);

            if (cc.budget_exceeded) {
                OUT(QUERY_BUFFER_ERR);
                free(cc.entries); free_criteria_tree(tree);
                return -1;
            }
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("{\"count\":%zu}\n", count);
            free(cc.entries);
        }
    } else if (plan.kind == PRIMARY_KEYSET) {
        /* Shape C / hybrid: build KeySet from OR index-union. */
        int budget_exceeded = 0;
        size_t count = keyset_count_from_or(db_root, object, &sch, tree, plan.or_node,
                                            &fs, &dl, &budget_exceeded);
        if (budget_exceeded) OUT(QUERY_BUFFER_ERR);
        else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("{\"count\":%zu}\n", count);
    } else {
        CountCtx ctx = { tree, &fs, 0, &dl, 0 };
        scan_shards(data_dir, sch.slot_size, count_scan_cb, &ctx);
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("{\"count\":%d}\n", ctx.count);
    }

    free_criteria_tree(tree);
    return 0;
}

/* find <object> <criteria_json> [offset] [limit] [fields]
   criteria_json: [{"field":"name","op":"contains","value":"ali"},{"field":"age","op":"gte","value":"18"}] */
/* ===== Ordered find: buffer matches, sort, emit slice =====
   Used when caller sets order_by on find mode. Joins are not supported in
   this path (caller rejects the combination). Full-scan based; an indexed
   ordered scan is a v2 item. */
typedef struct {
    char *key;
    size_t key_len;
    uint8_t *record;        /* malloc'd copy of [key bytes | value bytes] */
    size_t value_len;       /* length of the value portion */
    char *sort_str;         /* extracted sort field as string (may be NULL) */
    double sort_num;        /* numeric form, valid iff sort_is_num */
    int sort_is_num;
} OrderedRow;

typedef struct {
    OrderedRow *rows;
    size_t count;
    size_t cap;
    CriteriaNode *tree;
    FieldSchema *fs;
    int order_field_idx;    /* index in typed schema, -1 if field unknown/untyped */
    const char *order_field_name;
    ExcludedKeys *excluded;
    QueryDeadline *deadline;
    int dl_counter;
    int order_is_numeric;
    size_t buffer_bytes;    /* running total for QUERY_BUFFER_MB cap */
    int budget_exceeded;    /* set once we cross the cap — callback stops collecting */
    pthread_mutex_t lock;
} OrderedCollectCtx;

static int ordered_collect_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    OrderedCollectCtx *oc = (OrderedCollectCtx *)ctx;
    if (oc->budget_exceeded) return 1;  /* stop scanning once cap hit */
    if (query_deadline_tick(oc->deadline, &oc->dl_counter)) return 1;

    /* Exclusion check */
    char keybuf[1024];
    size_t klen = hdr->key_len < sizeof(keybuf) - 1 ? hdr->key_len : sizeof(keybuf) - 1;
    memcpy(keybuf, block, klen);
    keybuf[klen] = '\0';
    if (is_excluded(oc->excluded, keybuf)) return 0;

    const uint8_t *raw = block + hdr->key_len;
    if (!criteria_match_tree(raw, oc->tree, oc->fs)) return 0;

    /* Extract sort key */
    char *sv = NULL;
    if (oc->fs && oc->fs->ts && oc->order_field_idx >= 0) {
        sv = typed_get_field_str(oc->fs->ts, raw, oc->order_field_idx);
    } else {
        sv = decode_field((const char *)raw, hdr->value_len, oc->order_field_name, oc->fs);
    }

    size_t rec_len = (size_t)hdr->key_len + (size_t)hdr->value_len;
    size_t row_bytes = sizeof(OrderedRow) + hdr->key_len + rec_len + (sv ? strlen(sv) + 1 : 0);

    pthread_mutex_lock(&oc->lock);
    if (oc->buffer_bytes + row_bytes > g_query_buffer_max_bytes) {
        oc->budget_exceeded = 1;
        pthread_mutex_unlock(&oc->lock);
        free(sv);
        return 1;  /* stop scan */
    }
    oc->buffer_bytes += row_bytes;
    if (oc->count >= oc->cap) {
        oc->cap = oc->cap ? oc->cap * 2 : 256;
        oc->rows = realloc(oc->rows, oc->cap * sizeof(OrderedRow));
    }
    OrderedRow *r = &oc->rows[oc->count++];
    r->key_len = hdr->key_len;
    r->key = malloc(hdr->key_len + 1);
    memcpy(r->key, block, hdr->key_len);
    r->key[hdr->key_len] = '\0';
    r->value_len = hdr->value_len;
    r->record = malloc(rec_len);
    memcpy(r->record, block, rec_len);
    r->sort_str = sv;
    r->sort_is_num = oc->order_is_numeric;
    r->sort_num = (oc->order_is_numeric && sv) ? atof(sv) : 0.0;
    pthread_mutex_unlock(&oc->lock);
    return 0;
}

static int cmp_row_asc(const void *a, const void *b) {
    const OrderedRow *ra = (const OrderedRow *)a;
    const OrderedRow *rb = (const OrderedRow *)b;
    if (ra->sort_is_num) {
        if (ra->sort_num < rb->sort_num) return -1;
        if (ra->sort_num > rb->sort_num) return  1;
        return 0;
    }
    const char *sa = ra->sort_str ? ra->sort_str : "";
    const char *sb = rb->sort_str ? rb->sort_str : "";
    return strcmp(sa, sb);
}
static int cmp_row_desc(const void *a, const void *b) { return -cmp_row_asc(a, b); }

static int typed_field_is_numeric(uint8_t ft) {
    return ft == FT_INT || ft == FT_LONG || ft == FT_SHORT || ft == FT_DOUBLE ||
           ft == FT_NUMERIC || ft == FT_DATE || ft == FT_DATETIME ||
           ft == FT_BOOL || ft == FT_BYTE;
}

int cmd_find(const char *db_root, const char *object,
                    const char *criteria_json, int offset, int limit,
                    const char *proj_str, const char *excluded_csv,
                    const char *format, const char *delimiter,
                    const char *join_json,
                    const char *order_by, const char *order_dir) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;

    /* Parse joins (if any) — forces tabular output irrespective of `format`. */
    JoinSpec *joins = NULL;
    int njoins = 0;
    if (join_json && join_json[0]) {
        if (parse_joins_json(join_json, &joins, &njoins) < 0) {
            OUT("{\"error\":\"invalid 'join' array\"}\n");
            return -1;
        }
    }
    int has_joins = (njoins > 0);

    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Parse projection */
    const char *proj_fields[MAX_FIELDS];
    char proj_buf[MAX_LINE];
    int proj_count = 0;
    if (proj_str && proj_str[0]) {
        snprintf(proj_buf, sizeof(proj_buf), "%s", proj_str);
        char *_tok_save = NULL; char *tok = strtok_r(proj_buf, ",", &_tok_save);
        while (tok && proj_count < MAX_FIELDS) {
            proj_fields[proj_count++] = tok;
            tok = strtok_r(NULL, ",", &_tok_save);
        }
    }

    /* Parse criteria into a tree (AND/OR supported). Empty/absent → no criteria. */
    const char *perr = NULL;
    CriteriaNode *tree = parse_criteria_tree(criteria_json, &perr);
    if (perr) {
        OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
        free_criteria_tree(tree);
        free_joins(joins, njoins);
        return -1;
    }

    /* Apply hard limit: 0 or -1 means use server default, else cap at hard limit */
    if (limit <= 0) limit = g_global_limit;

    ExcludedKeys excluded = parse_excluded_keys(excluded_csv);

    /* Resolve joins (needs driver FieldSchema to pre-resolve local fields) */
    FieldSchema driver_fs;
    init_field_schema(&driver_fs, db_root, object);
    if (has_joins && resolve_joins(joins, njoins, db_root, object, &driver_fs) < 0) {
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return -1;
    }

    compile_criteria_tree(tree, driver_fs.ts);

    QueryPlan plan = choose_primary_source(tree, db_root, object);

    /* Statement-timeout deadline, shared across all worker threads of this query */
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };

    if (csv_delim && has_joins) {
        OUT("{\"error\":\"format=csv is not supported with join\"}\n");
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return -1;
    }

    if (has_joins) {
        /* Joined queries always emit tabular, ignoring `format` and `rows_fmt`. */
        emit_joined_columns(object,
                            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                            joins, njoins,
                            proj_count > 0 ? proj_fields : NULL, proj_count);
    } else if (csv_delim) {
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count,
                        (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                        csv_delim);
    } else if (rows_fmt) {
        emit_rows_columns(proj_fields, proj_count,
                          (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL);
    } else {
        OUT("[");
    }

    int has_order = (order_by && order_by[0] && !has_joins);

    if (has_order) {
        /* ===== ORDERED FIND: full-scan collect → sort → emit slice =====
           v1.x: buffer all matches, qsort by order_by, then emit offset..offset+limit.
           Indexed ordered scan is a v2 item. Joins + order_by is not supported. */

        int order_idx = -1;
        int order_is_num = 0;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_idx = i;
                    order_is_num = typed_field_is_numeric(driver_fs.ts->fields[i].type);
                    break;
                }
            }
        }

        OrderedCollectCtx oc;
        memset(&oc, 0, sizeof(oc));
        oc.tree = tree;
        oc.fs = (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL;
        oc.order_field_idx = order_idx;
        oc.order_field_name = order_by;
        oc.excluded = &excluded;
        oc.deadline = &dl;
        oc.order_is_numeric = order_is_num;
        pthread_mutex_init(&oc.lock, NULL);

        scan_shards(data_dir, sch.slot_size, ordered_collect_cb, &oc);

        if (oc.budget_exceeded) {
            for (size_t i = 0; i < oc.count; i++) {
                free(oc.rows[i].key); free(oc.rows[i].record); free(oc.rows[i].sort_str);
            }
            free(oc.rows);
            pthread_mutex_destroy(&oc.lock);
            OUT(QUERY_BUFFER_ERR);
            free_excluded(&excluded);
            free_criteria_tree(tree);
            free_joins(joins, njoins);
            return -1;
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 || strcmp(order_dir, "DESC") == 0));
        if (oc.count > 1)
            qsort(oc.rows, oc.count, sizeof(OrderedRow), desc ? cmp_row_desc : cmp_row_asc);

        size_t start = offset > 0 ? (size_t)offset : 0;
        size_t end = (limit > 0) ? start + (size_t)limit : oc.count;
        if (end > oc.count) end = oc.count;
        int printed = 0;
        for (size_t i = start; i < end; i++) {
            OrderedRow *r = &oc.rows[i];
            const uint8_t *val = r->record + r->key_len;
            if (csv_delim) {
                csv_emit_row(r->key, val, r->value_len,
                             proj_count > 0 ? proj_fields : NULL,
                             proj_count, &driver_fs, csv_delim);
                printed++;
                continue;
            }
            if (rows_fmt) {
                OUT("%s[\"%s\"", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                } else if (driver_fs.ts) {
                    for (int j = 0; j < driver_fs.ts->nfields; j++) {
                        if (driver_fs.ts->fields[j].removed) continue;
                        char *pv = typed_get_field_str(driver_fs.ts, val, j);
                        OUT(",\"%s\"", pv ? pv : "");
                        free(pv);
                    }
                }
                OUT("]");
            } else if (proj_count > 0) {
                OUT("%s{\"key\":\"%s\",\"value\":{", printed ? "," : "", r->key);
                int first = 1;
                for (int j = 0; j < proj_count; j++) {
                    char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                    if (!pv) continue;
                    OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                    first = 0;
                    free(pv);
                }
                OUT("}}");
            } else {
                char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                OUT("%s{\"key\":\"%s\",\"value\":%s}", printed ? "," : "", r->key, v);
                free(v);
            }
            printed++;
        }

        for (size_t i = 0; i < oc.count; i++) {
            free(oc.rows[i].key);
            free(oc.rows[i].record);
            free(oc.rows[i].sort_str);
        }
        free(oc.rows);
        pthread_mutex_destroy(&oc.lock);
    } else if (plan.kind == PRIMARY_LEAF) {
        /* ===== INDEXED FIND: collect → group by shard → parallel process ===== */
        SearchCriterion *pc = plan.primary_leaf;
        enum SearchOp op = pc->op;
        int check_primary = (op == OP_CONTAINS || op == OP_LIKE || op == OP_ENDS_WITH ||
                             op == OP_NOT_LIKE || op == OP_NOT_CONTAINS || op == OP_NOT_IN);
        int rc = idx_find_parallel(db_root, object, &sch, plan.primary_idx_path, tree,
                         pc, check_primary, &excluded, offset, limit,
                         proj_fields, proj_count,
                         (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                         rows_fmt, csv_delim, joins, njoins, &dl);
        if (rc == -2) {
            if (csv_delim) { /* nothing to close */ }
            else if (has_joins || rows_fmt) OUT("]}\n");
            else OUT("]\n");
            free_excluded(&excluded); free_criteria_tree(tree); free_joins(joins, njoins);
            OUT(QUERY_BUFFER_ERR);
            return -1;
        }
    } else if (plan.kind == PRIMARY_KEYSET) {
        /* ===== OR INDEX-UNION FIND ===== */
        int budget_exceeded = 0;
        keyset_find_from_or(db_root, object, &sch, tree, plan.or_node,
                            &excluded, offset, limit, proj_fields, proj_count,
                            (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                            rows_fmt, csv_delim, joins, njoins, &dl, &budget_exceeded);
        if (budget_exceeded) {
            /* Already wrote no rows. Close the open envelope cleanly, then emit error. */
            if (csv_delim) { /* no envelope to close */ }
            else if (has_joins || rows_fmt) OUT("]}\n");
            else OUT("]\n");
            free_excluded(&excluded);
            free_criteria_tree(tree);
            free_joins(joins, njoins);
            OUT(QUERY_BUFFER_ERR);
            return -1;
        }
    } else {
        /* ===== FULL SCAN FALLBACK ===== */
        AdvSearchCtx ctx = { tree, offset, limit, 0, 0,
                             proj_fields, proj_count, excluded, &driver_fs,
                             rows_fmt, csv_delim,
                             object, joins, njoins, db_root, &dl, 0 };
        scan_shards(data_dir, sch.slot_size, adv_search_cb, &ctx);
    }

    if (has_joins)
        OUT("]}\n");
    else if (csv_delim)
        { /* CSV body already ends with its own \n per row — nothing to close */ }
    else if (rows_fmt)
        OUT("]}\n");
    else
        OUT("]\n");
    free_excluded(&excluded);

    free_criteria_tree(tree);
    free_joins(joins, njoins);

    return 0;
}

/* ========== SEQUENCES ========== */

/* Atomic counter per object, stored in $DB_ROOT/<object>/metadata/sequences/<name> */
int cmd_sequence(const char *db_root, const char *object,
                        const char *seq_name, const char *action, int batch_size) {
    char seq_dir[PATH_MAX], seq_path[PATH_MAX], lock_path[PATH_MAX];
    snprintf(seq_dir, sizeof(seq_dir), "%s/%s/metadata/sequences", db_root, object);
    mkdirp(seq_dir);
    snprintf(seq_path, sizeof(seq_path), "%s/%s", seq_dir, seq_name);
    snprintf(lock_path, sizeof(lock_path), "%s/%s.lock", seq_dir, seq_name);

    if (strcmp(action, "current") == 0) {
        long long val = 0;
        FILE *f = fopen(seq_path, "r");
        if (f) { fscanf(f, "%lld", &val); fclose(f); }
        OUT("{\"sequence\":\"%s\",\"value\":%lld}\n", seq_name, val);
        return 0;
    }

    if (strcmp(action, "reset") == 0) {
        FILE *f = fopen(seq_path, "w");
        if (f) { fprintf(f, "0\n"); fclose(f); }
        OUT("{\"sequence\":\"%s\",\"value\":0}\n", seq_name);
        return 0;
    }

    if (strcmp(action, "next") == 0 || strcmp(action, "next-batch") == 0) {
        int lockfd = open(lock_path, O_RDWR | O_CREAT, 0644);
        flock(lockfd, LOCK_EX);

        long long val = 0;
        FILE *f = fopen(seq_path, "r");
        if (f) { fscanf(f, "%lld", &val); fclose(f); }

        if (batch_size <= 1) {
            val++;
            f = fopen(seq_path, "w");
            fprintf(f, "%lld\n", val);
            fclose(f);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            OUT("{\"sequence\":\"%s\",\"value\":%lld}\n", seq_name, val);
        } else {
            long long start = val + 1;
            val += batch_size;
            f = fopen(seq_path, "w");
            fprintf(f, "%lld\n", val);
            fclose(f);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            OUT("{\"sequence\":\"%s\",\"start\":%lld,\"end\":%lld,\"count\":%d}\n",
                   seq_name, start, val, batch_size);
        }
        return 0;
    }

    OUT("{\"error\":\"Unknown sequence action: %s\"}\n", action);
    return 1;
}

/* ========== BACKUP ========== */

int cmd_backup(const char *db_root, const char *object) {
    char src_dir[PATH_MAX], bak_dir[PATH_MAX];
    snprintf(src_dir, sizeof(src_dir), "%s/%s", db_root, object);
    struct stat st;
    if (stat(src_dir, &st) != 0) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return 1;
    }

    /* Create timestamped backup directory */
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y%m%d_%H%M%S", t);
    snprintf(bak_dir, sizeof(bak_dir), "%s/%s/backup/%s", db_root, object, ts);
    mkdirp(bak_dir);

    /* Copy data directory */
    char cmd[PATH_MAX * 2];
    snprintf(cmd, sizeof(cmd), "cp -r '%s/data' '%s/data' 2>/dev/null", src_dir, bak_dir);
    system(cmd);
    snprintf(cmd, sizeof(cmd), "cp -r '%s/indexes' '%s/indexes' 2>/dev/null", src_dir, bak_dir);
    system(cmd);
    snprintf(cmd, sizeof(cmd), "cp -r '%s/metadata' '%s/metadata' 2>/dev/null", src_dir, bak_dir);
    system(cmd);

    OUT("{\"status\":\"backed_up\",\"path\":\"%s\"}\n", bak_dir);
    return 0;
}

/* ========== RECOUNT ========== */

/* Recount uses its own parallel scan — no shared out_lock overhead.
   The generic scan_shards wraps each callback invocation in a mutex to
   serialize JSON output for find/keys. Recount doesn't
   emit per-record output, just counts, so that mutex is pure waste at
   1M+ slots. Use atomic increment and skip the mutex. */

typedef struct {
    const char *path;
    int slot_size;
    int *counter; /* shared atomic counter */
} RecountWorkerArg;

static void *recount_worker(void *arg) {
    RecountWorkerArg *w = (RecountWorkerArg *)arg;
    int local = 0;
    /* Bypass ucache — MADV_RANDOM would kill readahead for a sequential
       scan. Open direct + MADV_SEQUENTIAL + MADV_DONTNEED after so recount
       doesn't evict hot random-lookup pages. */
    int fd = open(w->path, O_RDONLY);
    if (fd < 0) return NULL;
    struct stat st;
    if (fstat(fd, &st) < 0 || st.st_size == 0) { close(fd); return NULL; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;
    madvise(map, st.st_size, MADV_SEQUENTIAL);

    size_t file_size = st.st_size;
    if (file_size < SHARD_HDR_SIZE) { munmap(map, file_size); return NULL; }
    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) { munmap(map, file_size); return NULL; }
    uint32_t shard_slots = sh->slots_per_shard;
    if (file_size < shard_zoneA_end(shard_slots)) { munmap(map, file_size); return NULL; }
    size_t scan_end = shard_slots;
    while (scan_end > 0) {
        const SlotHeader *h = (const SlotHeader *)(map + zoneA_off(scan_end - 1));
        if (h->flag != 0 || h->key_len != 0) break;
        scan_end--;
    }
    for (size_t s = 0; s < scan_end; s++) {
        const SlotHeader *hdr = (const SlotHeader *)(map + zoneA_off(s));
        if (hdr->flag == 1) local++;
    }
    madvise(map, file_size, MADV_DONTNEED);
    munmap(map, file_size);
    __sync_fetch_and_add(w->counter, local);
    return NULL;
}

/* shard-stats: walk every shard file under data/, read each ShardHeader, report slots/records/load
   plus a hint when splits may be too low. Cheap — reads only 32B per shard.
   as_table=1 emits ASCII table; as_table=0 emits JSON. */
int cmd_shard_stats(const char *db_root, const char *object, int as_table) {
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    typedef struct { int shard_id; uint32_t slots; uint32_t records; uint64_t file_bytes; } Row;
    int cap = sch.splits > 0 ? sch.splits : 16;
    Row *rows = calloc(cap, sizeof(Row));
    int nrows = 0;

    DIR *d1 = opendir(data_dir);
    if (!d1) { OUT("{\"error\":\"Object [%s] has no data\"}\n", object); free(rows); return 1; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nl = strlen(e1->d_name);
        if (nl < 5 || strcmp(e1->d_name + nl - 4, ".bin") != 0) continue;
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", data_dir, e1->d_name);
        int fd = open(path, O_RDONLY);
        if (fd < 0) continue;
        struct stat st; if (fstat(fd, &st) < 0) { close(fd); continue; }
        ShardHeader hdr;
        if (pread(fd, &hdr, sizeof(hdr), 0) != (ssize_t)sizeof(hdr)) { close(fd); continue; }
        close(fd);
        if (hdr.magic != SHARD_MAGIC) continue;
        int sid = (int)strtol(e1->d_name, NULL, 16);
        if (nrows >= cap) { cap *= 2; rows = realloc(rows, cap * sizeof(Row)); }
        rows[nrows++] = (Row){ sid, hdr.slots_per_shard, hdr.record_count, (uint64_t)st.st_size };
    }
    closedir(d1);

    /* Sort by shard_id */
    for (int i = 1; i < nrows; i++) {
        Row k = rows[i]; int j = i - 1;
        while (j >= 0 && rows[j].shard_id > k.shard_id) { rows[j+1] = rows[j]; j--; }
        rows[j+1] = k;
    }

    uint64_t total_records = 0, total_bytes = 0;
    uint32_t max_slots = 0, max_records = 0, min_records = UINT32_MAX;
    int grows = 0;  /* max number of doublings observed = log2(slots/INITIAL) */
    for (int i = 0; i < nrows; i++) {
        total_records += rows[i].records;
        total_bytes += rows[i].file_bytes;
        if (rows[i].slots > max_slots) max_slots = rows[i].slots;
        if (rows[i].records > max_records) max_records = rows[i].records;
        if (rows[i].records < min_records) min_records = rows[i].records;
    }
    for (uint32_t s = max_slots; s > INITIAL_SLOTS; s >>= 1) grows++;

    /* Hint: suggest raising splits if shards are both large and highly loaded */
    const char *hint = NULL;
    double avg_load = 0.0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        if (grows >= 4 && avg_load > 0.35 && sch.splits < MAX_SPLITS) {
            hint = "shards have grown heavily — consider raising splits for more parallelism (vacuum --splits=N)";
        } else if (min_records > 0 && max_records > min_records * 4) {
            hint = "shard load is skewed — check key distribution";
        } else if (grows == 0 && total_records > 0) {
            hint = "no growth yet — INITIAL_SLOTS is sufficient for this load";
        }
    }

    if (as_table) {
        OUT("splits=%d shards=%d total_records=%lu total_bytes=%lu max_grows=%d avg_load=%.3f\n",
            sch.splits, nrows, (unsigned long)total_records, (unsigned long)total_bytes,
            grows, avg_load);
        OUT("  %-8s %-10s %-10s %-8s %-14s\n", "shard", "slots", "records", "load", "bytes");
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("  %-8d %-10u %-10u %-8.3f %-14lu\n",
                rows[i].shard_id, rows[i].slots, rows[i].records, load,
                (unsigned long)rows[i].file_bytes);
        }
        if (hint) OUT("hint: %s\n", hint);
    } else {
        OUT("{\"splits\":%d,\"shards\":%d,\"total_records\":%lu,\"total_bytes\":%lu,\"shard_list\":[",
            sch.splits, nrows, (unsigned long)total_records, (unsigned long)total_bytes);
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("%s{\"shard\":%d,\"slots\":%u,\"records\":%u,\"load\":%.3f,\"bytes\":%lu}",
                i ? "," : "", rows[i].shard_id, rows[i].slots, rows[i].records,
                load, (unsigned long)rows[i].file_bytes);
        }
        OUT("],\"max_grows\":%d", grows);
        if (hint) OUT(",\"hint\":\"%s\"", hint);
        OUT("}\n");
    }
    free(rows);
    return 0;
}

int cmd_recount(const char *db_root, const char *object) {
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    /* Collect shard paths */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(paths); OUT("{\"count\":0}\n"); set_count(db_root, object, 0); return 0; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (path_count >= path_cap) {
            path_cap *= 2;
            paths = realloc(paths, path_cap * sizeof(char *));
        }
        char bp[PATH_MAX];
        snprintf(bp, sizeof(bp), "%s/%s", data_dir, e1->d_name);
        paths[path_count++] = strdup(bp);
    }
    closedir(d1);

    int total = 0;
    if (path_count > 0) {
        RecountWorkerArg *args = malloc(path_count * sizeof(RecountWorkerArg));
        for (int i = 0; i < path_count; i++)
            args[i] = (RecountWorkerArg){ paths[i], sch.slot_size, &total };
        parallel_for(recount_worker, args, path_count, sizeof(RecountWorkerArg));
        free(args);
    }

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);

    set_count(db_root, object, total);
    OUT("{\"count\":%d}\n", total);
    return 0;
}

/* ========== TRUNCATE ========== */

int cmd_truncate(const char *db_root, const char *object) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    struct stat st;
    if (stat(obj_dir, &st) != 0) {
        fprintf(stderr, "Error: Object [%s] not found\n", object);
        return 1;
    }
    fcache_invalidate(obj_dir);
    invalidate_idx_cache(object);

    /* Only delete data, metadata, and index data — preserve config files */
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/data", obj_dir);
    rmrf(path);
    snprintf(path, sizeof(path), "%s/metadata", obj_dir);
    rmrf(path);
    /* Delete index .idx files but keep index.conf */
    snprintf(path, sizeof(path), "%s/indexes", obj_dir);
    DIR *d = opendir(path);
    if (d) {
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            size_t nlen = strlen(e->d_name);
            if (nlen > 4 && strcmp(e->d_name + nlen - 4, ".idx") == 0) {
                char fp[PATH_MAX];
                snprintf(fp, sizeof(fp), "%s/%s", path, e->d_name);
                unlink(fp);
            }
        }
        closedir(d);
    }
    /* Recreate data and metadata dirs */
    snprintf(path, sizeof(path), "%s/data", obj_dir);
    mkdirp(path);
    snprintf(path, sizeof(path), "%s/metadata", obj_dir);
    mkdirp(path);

    set_count(db_root, object, 0);
    OUT("{\"status\":\"truncated\",\"object\":\"%s\"}\n", object);
    return 0;
}

/* ========== PUT-FILE / GET-FILE-PATH ========== */

int cmd_put_file(const char *db_root, const char *object, const char *src) {
    struct stat st;
    if (stat(src, &st) != 0) {
        fprintf(stderr, "Error: Source file %s not found\n", src);
        return 1;
    }
    const char *filename = strrchr(src, '/');
    filename = filename ? filename + 1 : src;
    char key[PATH_MAX];
    snprintf(key, sizeof(key), "%s", filename);
    char *dot = strrchr(key, '.');
    if (dot) *dot = '\0';

    uint8_t hash[16];
    compute_hash_raw(key, strlen(key), hash);

    char dest_dir[PATH_MAX], dest[PATH_MAX];
    snprintf(dest_dir, sizeof(dest_dir), "%s/%s/files/%02x/%02x",
             db_root, object, hash[0], hash[1]);
    snprintf(dest, sizeof(dest), "%s/%s", dest_dir, filename);
    mkdirp(dest_dir);

    int sfd = open(src, O_RDONLY);
    if (sfd < 0) { fprintf(stderr, "Error: Cannot read %s\n", src); return 1; }
    int dfd = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) { close(sfd); fprintf(stderr, "Error: Cannot create %s\n", dest); return 1; }
    char buf[65536]; ssize_t n;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) write(dfd, buf, n);
    close(sfd); close(dfd);
    OUT("{\"status\":\"stored\",\"path\":\"%s\"}\n", dest);
    return 0;
}

int cmd_get_file_path(const char *db_root, const char *object, const char *filename) {
    char key[PATH_MAX];
    snprintf(key, sizeof(key), "%s", filename);
    char *dot = strrchr(key, '.');
    if (dot) *dot = '\0';
    uint8_t hash[16];
    compute_hash_raw(key, strlen(key), hash);
    OUT("{\"path\":\"%s/%s/files/%02x/%02x/%s\"}\n",
           db_root, object, hash[0], hash[1], filename);
    return 0;
}

/* Compute the on-disk destination path for a filename. Caller supplies buffers. */
static void file_dest_path(const char *db_root, const char *object, const char *filename,
                           char *dest_dir, size_t dd_sz, char *dest, size_t d_sz) {
    char key[PATH_MAX];
    snprintf(key, sizeof(key), "%s", filename);
    char *dot = strrchr(key, '.');
    if (dot) *dot = '\0';
    uint8_t hash[16];
    compute_hash_raw(key, strlen(key), hash);
    snprintf(dest_dir, dd_sz, "%s/%s/files/%02x/%02x", db_root, object, hash[0], hash[1]);
    snprintf(dest, d_sz, "%s/%s", dest_dir, filename);
}

/* Streaming upload (base64-in-JSON). Atomic: writes to .tmp.<pid>, fsyncs, renames.
   if_not_exists=1 → refuse overwrite. */
int cmd_put_file_b64(const char *db_root, const char *object,
                     const char *filename, const char *b64_data, size_t b64_len,
                     int if_not_exists) {
    if (!valid_filename(filename)) {
        OUT("{\"error\":\"invalid filename\"}\n");
        return 1;
    }
    if (!b64_data) {
        OUT("{\"error\":\"missing data\"}\n");
        return 1;
    }

    char dest_dir[PATH_MAX], dest[PATH_MAX];
    file_dest_path(db_root, object, filename, dest_dir, sizeof(dest_dir), dest, sizeof(dest));

    if (if_not_exists) {
        struct stat st;
        if (stat(dest, &st) == 0) {
            OUT("{\"error\":\"file exists\",\"filename\":\"%s\"}\n", filename);
            return 1;
        }
    }

    size_t cap = b64_decoded_maxsize(b64_len);
    uint8_t *raw = malloc(cap);
    if (!raw) { OUT("{\"error\":\"out of memory\"}\n"); return 1; }
    size_t raw_len = 0;
    if (b64_decode(b64_data, b64_len, raw, &raw_len) != 0) {
        free(raw);
        OUT("{\"error\":\"invalid base64\"}\n");
        return 1;
    }

    mkdirp(dest_dir);

    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s.tmp.%d", dest, (int)getpid());
    int fd = open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { free(raw); OUT("{\"error\":\"cannot create %s\"}\n", tmp); return 1; }

    size_t w = 0;
    while (w < raw_len) {
        ssize_t n = write(fd, raw + w, raw_len - w);
        if (n <= 0) { close(fd); unlink(tmp); free(raw); OUT("{\"error\":\"write failed\"}\n"); return 1; }
        w += (size_t)n;
    }
    fsync(fd);
    close(fd);
    free(raw);

    if (rename(tmp, dest) != 0) {
        unlink(tmp);
        OUT("{\"error\":\"rename failed\"}\n");
        return 1;
    }

    OUT("{\"status\":\"stored\",\"filename\":\"%s\",\"bytes\":%zu}\n", filename, raw_len);
    return 0;
}

/* Streaming download: read file, base64-encode, emit with bytes + data. */
int cmd_get_file_b64(const char *db_root, const char *object, const char *filename) {
    if (!valid_filename(filename)) {
        OUT("{\"error\":\"invalid filename\"}\n");
        return 1;
    }

    char dest_dir[PATH_MAX], dest[PATH_MAX];
    file_dest_path(db_root, object, filename, dest_dir, sizeof(dest_dir), dest, sizeof(dest));

    int fd = open(dest, O_RDONLY);
    if (fd < 0) {
        OUT("{\"error\":\"file not found\",\"filename\":\"%s\"}\n", filename);
        return 1;
    }
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); OUT("{\"error\":\"stat failed\"}\n"); return 1; }
    size_t sz = (size_t)st.st_size;

    uint8_t *raw = malloc(sz ? sz : 1);
    if (!raw) { close(fd); OUT("{\"error\":\"out of memory\"}\n"); return 1; }
    size_t r = 0;
    while (r < sz) {
        ssize_t n = read(fd, raw + r, sz - r);
        if (n <= 0) break;
        r += (size_t)n;
    }
    close(fd);
    if (r != sz) { free(raw); OUT("{\"error\":\"read failed\"}\n"); return 1; }

    size_t enc_sz = b64_encoded_size(sz);
    char *enc = malloc(enc_sz + 1);
    if (!enc) { free(raw); OUT("{\"error\":\"out of memory\"}\n"); return 1; }
    b64_encode(raw, sz, enc);
    free(raw);

    OUT("{\"status\":\"ok\",\"filename\":\"%s\",\"bytes\":%zu,\"data\":\"%s\"}\n",
        filename, sz, enc);
    free(enc);
    return 0;
}

/* Remove a file previously stored via put-file. Hash-bucket resolution matches
   put-file / get-file. Returns {"status":"deleted",...} or {"error":"file not found",...}. */
int cmd_delete_file(const char *db_root, const char *object, const char *filename) {
    if (!valid_filename(filename)) {
        OUT("{\"error\":\"invalid filename\"}\n");
        return 1;
    }

    char dest_dir[PATH_MAX], dest[PATH_MAX];
    file_dest_path(db_root, object, filename, dest_dir, sizeof(dest_dir), dest, sizeof(dest));

    if (unlink(dest) != 0) {
        if (errno == ENOENT)
            OUT("{\"error\":\"file not found\",\"filename\":\"%s\"}\n", filename);
        else
            OUT("{\"error\":\"unlink failed: %s\",\"filename\":\"%s\"}\n",
                strerror(errno), filename);
        return 1;
    }

    OUT("{\"status\":\"deleted\",\"filename\":\"%s\"}\n", filename);
    return 0;
}

/* ========== CREATE OBJECT ========== */

/* Validate a field type spec like "name:varchar:30" or "age:int".
   Returns the storage size (>0) on success, 0 on invalid. */
static int validate_field_type(const char *field_spec) {
    const char *colon = strchr(field_spec, ':');
    if (!colon || colon == field_spec) return 0; /* no type separator or empty name */

    /* Work on a copy so we can strip modifiers */
    char buf[512];
    strncpy(buf, colon + 1, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';

    /* Strip :removed */
    size_t blen = strlen(buf);
    if (blen >= 8 && strcmp(buf + blen - 8, ":removed") == 0) {
        buf[blen - 8] = '\0';
    }
    /* Strip default modifiers: :auto_create, :auto_update, :default=... */
    char *dm;
    if ((dm = strstr(buf, ":auto_update")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":auto_create")) != NULL && dm[12] == '\0') *dm = '\0';
    else if ((dm = strstr(buf, ":default=")) != NULL) *dm = '\0';

    const char *type = buf;
    if (strncmp(type, "varchar:", 8) == 0) {
        int sz = atoi(type + 8);
        if (sz <= 0 || sz > 65535) return 0;   /* bounded: 1..65535 content bytes */
        return sz + 2;                          /* 2-byte length prefix */
    }
    if (strcmp(type, "varchar") == 0) return 0; /* bare "varchar" — require :N */
    if (strcmp(type, "long") == 0)   return 8;
    if (strcmp(type, "int") == 0)    return 4;
    if (strcmp(type, "short") == 0)  return 2;
    if (strcmp(type, "double") == 0) return 8;
    if (strcmp(type, "bool") == 0)   return 1;
    if (strcmp(type, "byte") == 0)   return 1;
    if (strcmp(type, "date") == 0)   return 4;
    if (strcmp(type, "datetime") == 0) return 6;
    if (strcmp(type, "currency") == 0) return 8;
    if (strncmp(type, "numeric:", 8) == 0) return 8;
    return 0;
}

int cmd_create_object(const char *db_root, const char *dir, const char *object,
                      const char *fields_json, const char *indexes_json,
                      int splits, int max_key) {
    if (!dir || !dir[0]) {
        OUT("{\"error\":\"dir is required\"}\n");
        return 1;
    }
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        return 1;
    }
    if (!fields_json || !fields_json[0]) {
        OUT("{\"error\":\"fields is required — e.g. [\\\"name:varchar:30\\\",\\\"age:int\\\"]\"}\n");
        return 1;
    }

    /* Validate fields array — must be non-empty, every field must have a valid type */
    const char *p = json_skip(fields_json);
    if (*p != '[') {
        OUT("{\"error\":\"fields must be a JSON array\"}\n");
        return 1;
    }
    p++;

    /* First pass: validate all fields before creating anything */
    char field_specs[MAX_FIELDS][512];
    int nfields = 0;
    int total_value_size = 0;

    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p != '"') {
            OUT("{\"error\":\"fields array must contain strings\"}\n");
            return 1;
        }
        p++;
        const char *start = p;
        while (*p && *p != '"') p++;
        int flen = (int)(p - start);
        if (*p == '"') p++;

        if (nfields >= MAX_FIELDS) {
            OUT("{\"error\":\"too many fields (max %d)\"}\n", MAX_FIELDS);
            return 1;
        }
        if (flen <= 0 || flen >= 511) {
            OUT("{\"error\":\"invalid field definition (empty or too long)\"}\n");
            return 1;
        }
        memcpy(field_specs[nfields], start, flen);
        field_specs[nfields][flen] = '\0';

        int field_size = validate_field_type(field_specs[nfields]);
        if (field_size <= 0) {
            OUT("{\"error\":\"invalid field type: \\\"%s\\\" — valid types: varchar:N, int, long, short, double, bool, byte, date, datetime, currency, numeric:P,S\"}\n",
                   field_specs[nfields]);
            return 1;
        }
        total_value_size += field_size;
        nfields++;
    }

    if (nfields == 0) {
        OUT("{\"error\":\"fields array is empty — at least one typed field required\"}\n");
        return 1;
    }

    /* Defaults */
    if (splits <= 0) splits = MIN_SPLITS;
    if (splits < MIN_SPLITS) splits = MIN_SPLITS;
    if (splits > MAX_SPLITS) splits = MAX_SPLITS;
    if (max_key <= 0) max_key = 64;
    if (max_key > MAX_KEY_CEILING) {
        OUT("{\"error\":\"max_key %d exceeds ceiling %d — keys larger than this bloat slot_size; use a shorter key (UUIDs are 36B)\"}\n",
            max_key, MAX_KEY_CEILING);
        return 1;
    }

    /* Validate indexes reference defined field names */
    if (indexes_json && indexes_json[0]) {
        p = json_skip(indexes_json);
        if (*p == '[') {
            p++;
            while (*p) {
                p = json_skip(p);
                if (*p == ']') break;
                if (*p == ',') { p++; continue; }
                if (*p == '"') {
                    p++;
                    const char *istart = p;
                    while (*p && *p != '"') p++;
                    int ilen = (int)(p - istart);
                    if (*p == '"') p++;

                    char idx_name[512];
                    memcpy(idx_name, istart, ilen < 511 ? ilen : 511);
                    idx_name[ilen < 511 ? ilen : 511] = '\0';

                    /* For composite indexes like "field1+field2", validate each part */
                    char check[512];
                    strncpy(check, idx_name, 511); check[511] = '\0';
                    char *_tok_save = NULL; char *tok = strtok_r(check, "+", &_tok_save);
                    while (tok) {
                        int found = 0;
                        for (int i = 0; i < nfields; i++) {
                            /* Extract field name (before first colon) */
                            const char *c = strchr(field_specs[i], ':');
                            int nlen = c ? (int)(c - field_specs[i]) : (int)strlen(field_specs[i]);
                            if ((int)strlen(tok) == nlen && memcmp(tok, field_specs[i], nlen) == 0) {
                                found = 1; break;
                            }
                        }
                        if (!found) {
                            OUT("{\"error\":\"index field \\\"%s\\\" not found in fields\"}\n", tok);
                            return 1;
                        }
                        tok = strtok_r(NULL, "+", &_tok_save);
                    }
                } else p++;
            }
        }
    }

    /* All validation passed — invalidate caches (in case object is being recreated) */
    invalidate_idx_cache(object);
    char inv_path[PATH_MAX];
    snprintf(inv_path, sizeof(inv_path), "%s/%s/%s", db_root, dir, object);
    fcache_invalidate(inv_path);

    /* Now create on disk */
    char eff_root[PATH_MAX];
    snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);

    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/%s/data", eff_root, object);
    mkdirp(path);
    snprintf(path, sizeof(path), "%s/%s/metadata", eff_root, object);
    mkdirp(path);
    snprintf(path, sizeof(path), "%s/%s/indexes", eff_root, object);
    mkdirp(path);
    snprintf(path, sizeof(path), "%s/%s/files", eff_root, object);
    mkdirp(path);

    /* Write fields.conf */
    snprintf(path, sizeof(path), "%s/%s/fields.conf", eff_root, object);
    FILE *f = fopen(path, "w");
    if (!f) {
        OUT("{\"error\":\"cannot write fields.conf\"}\n");
        return 1;
    }
    for (int i = 0; i < nfields; i++)
        fprintf(f, "%s\n", field_specs[i]);
    fclose(f);

    /* Add to schema.conf if not already there */
    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    char prefix[512];
    snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
    int exists = 0;
    f = fopen(schema_path, "r");
    if (f) {
        char line[512];
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, prefix, strlen(prefix)) == 0) { exists = 1; break; }
        }
        fclose(f);
    }
    if (!exists) {
        f = fopen(schema_path, "a");
        if (f) {
            fprintf(f, "%s:%s:%d:%d\n", dir, object, splits, max_key);
            fclose(f);
        }
    }

    /* Register the tenant dir in dirs.conf if missing, then reload the in-memory set */
    char dirs_path[PATH_MAX];
    snprintf(dirs_path, sizeof(dirs_path), "%s/dirs.conf", db_root);
    int dir_listed = 0;
    f = fopen(dirs_path, "r");
    if (f) {
        char line[256];
        size_t dlen = strlen(dir);
        while (fgets(line, sizeof(line), f)) {
            line[strcspn(line, "\r\n")] = '\0';
            if (strlen(line) == dlen && memcmp(line, dir, dlen) == 0) { dir_listed = 1; break; }
        }
        fclose(f);
    }
    if (!dir_listed) {
        f = fopen(dirs_path, "a");
        if (f) { fprintf(f, "%s\n", dir); fclose(f); }
    }
    load_dirs();

    /* Write index.conf if indexes provided */
    if (indexes_json && indexes_json[0]) {
        snprintf(path, sizeof(path), "%s/%s/indexes/index.conf", eff_root, object);
        f = fopen(path, "w");
        if (f) {
            p = json_skip(indexes_json);
            if (*p == '[') p++;
            while (*p) {
                p = json_skip(p);
                if (*p == ']') break;
                if (*p == ',') { p++; continue; }
                if (*p == '"') {
                    p++;
                    const char *start = p;
                    while (*p && *p != '"') p++;
                    fprintf(f, "%.*s\n", (int)(p - start), start);
                    if (*p == '"') p++;
                } else p++;
            }
            fclose(f);
        }
    }

    OUT("{\"status\":\"created\",\"object\":\"%s\",\"dir\":\"%s\",\"splits\":%d,\"max_key\":%d,\"value_size\":%d,\"fields\":%d}\n",
           object, dir, splits, max_key, total_value_size, nfields);
    return 0;
}

/* ========== AGGREGATE ========== */

enum AggFn { AGG_COUNT, AGG_SUM, AGG_AVG, AGG_MIN, AGG_MAX };

typedef struct {
    enum AggFn fn;
    char field[256];
    char alias[256];
} AggSpec;

typedef struct {
    double sum;
    double min;
    double max;
    int64_t count;
} AggAccum;

typedef struct AggBucket {
    char *group_key;            /* concatenated group values separated by \x1F */
    char **group_vals;          /* individual group values for output */
    AggAccum *accums;           /* one per aggregate */
    struct AggBucket *next;     /* hash chain */
} AggBucket;

#define AGG_HT_SIZE 16384
#define MAX_AGG_SPECS 32

typedef struct {
    CriteriaNode *tree;
    FieldSchema *fs;
    /* group_by */
    char group_fields[MAX_FIELDS][256];
    int ngroups;
    const TypedField *group_tfs[MAX_FIELDS];  /* resolved typed field per group — NULL = composite/unknown */
    /* aggregates */
    AggSpec *specs;
    int nspecs;
    const TypedField *spec_tfs[MAX_AGG_SPECS]; /* resolved typed field per agg spec — NULL = count or composite */
    /* hash table */
    AggBucket *ht[AGG_HT_SIZE];
    int total_buckets;
    QueryDeadline *deadline;
    int dl_counter;
    /* QUERY_BUFFER_MB accounting. Parallel workers share a single atomic counter
       (pointed to by shared_buffer_bytes); serial path reads/writes it like a
       plain size_t. budget_exceeded flips per-ctx once the cap is hit. */
    _Atomic size_t *shared_buffer_bytes;
    int budget_exceeded;
} AggCtx;

/* Write a typed field's string form into a caller-provided buffer (no malloc).
   Returns the length written. Returns 0 for "empty" fields (0-value LONG/INT/SHORT/
   DOUBLE/DATE/DATETIME/zero-length VARCHAR) to preserve legacy skip-empty behavior.
   FT_NUMERIC and FT_BOOL always return non-zero. */
static int typed_field_to_buf_raw(const TypedField *f, const uint8_t *p,
                                  char *buf, size_t bufsz) {
    switch (f->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(p, f->size);
        if (len == 0) return 0;
        if ((size_t)len >= bufsz) len = bufsz - 1;
        memcpy(buf, p + 2, len);
        buf[len] = '\0';
        return len;
    }
    case FT_LONG: {
        int64_t v = ld_be_i64(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%lld", (long long)v);
    }
    case FT_INT: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_SHORT: {
        int16_t v = ld_be_i16(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%d", v);
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        if (v == 0.0) return 0;
        return snprintf(buf, bufsz, "%g", v);
    }
    case FT_BOOL:
        return snprintf(buf, bufsz, "%s", p[0] ? "true" : "false");
    case FT_BYTE:
        return snprintf(buf, bufsz, "%u", p[0]);
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return 1; }
        int64_t scale = 1;
        for (int s = 0; s < f->numeric_scale; s++) scale *= 10;
        int64_t whole = v / scale;
        int64_t frac = v % scale;
        int neg = (v < 0);
        if (frac < 0) frac = -frac;
        if (whole < 0) whole = -whole;
        if (frac == 0)
            return snprintf(buf, bufsz, "%s%lld", neg ? "-" : "", (long long)whole);
        return snprintf(buf, bufsz, "%s%lld.%0*lld", neg ? "-" : "",
                        (long long)whole, f->numeric_scale, (long long)frac);
    }
    case FT_DATE: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        return snprintf(buf, bufsz, "%08d", v);
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        if (d == 0 && t == 0) return 0;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        return snprintf(buf, bufsz, "%08d%02d%02d%02d", d, hh, mm, ss);
    }
    default:
        return 0;
    }
}

/* Extract a typed field as a double for SUM/AVG/MIN/MAX accumulation.
   Returns 1 if the field is "present" (non-zero/non-empty), 0 if missing
   (so the record is excluded from the aggregate — matches legacy behavior). */
static int typed_field_to_double(const TypedField *f, const uint8_t *p, double *out) {
    switch (f->type) {
    case FT_LONG: {
        int64_t v = ld_be_i64(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_INT: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_SHORT: {
        int16_t v = ld_be_i16(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DOUBLE: {
        double v; memcpy(&v, p, 8);
        if (v == 0.0) return 0;
        *out = v; return 1;
    }
    case FT_NUMERIC: {
        int64_t v = ld_be_i64(p);
        int64_t scale = 1;
        for (int s = 0; s < f->numeric_scale; s++) scale *= 10;
        *out = (double)v / (double)scale; return 1;  /* 0 is a valid numeric value */
    }
    case FT_BOOL: *out = (double)(p[0] ? 1 : 0); return 1;
    case FT_BYTE: *out = (double)p[0]; return 1;
    case FT_DATE: {
        int32_t v = ld_be_i32(p);
        if (v == 0) return 0;
        *out = (double)v; return 1;
    }
    case FT_DATETIME: {
        int32_t d = ld_be_i32(p);
        uint16_t t = ld_be_u16(p + 4);
        if (d == 0 && t == 0) return 0;
        *out = (double)d * 1000000.0 + (double)t; return 1;
    }
    case FT_VARCHAR: {
        int len = varchar_eff_len(p, f->size);
        if (len == 0) return 0;
        char tmp[64]; int n = len < 63 ? len : 63;
        memcpy(tmp, p + 2, n); tmp[n] = '\0';
        *out = atof(tmp); return 1;
    }
    default: return 0;
    }
}

static uint32_t agg_hash(const char *s) {
    uint32_t h = 5381;
    while (*s) h = h * 33 + (uint8_t)*s++;
    return h;
}

static AggBucket *agg_find_or_create(AggCtx *ctx, char **vals, int nvals) {
    /* Build composite key */
    char key[4096];
    int kp = 0;
    for (int i = 0; i < nvals; i++) {
        if (i > 0) key[kp++] = '\x1F';
        int sl = strlen(vals[i]);
        if (kp + sl >= (int)sizeof(key) - 1) break;
        memcpy(key + kp, vals[i], sl);
        kp += sl;
    }
    key[kp] = '\0';

    uint32_t h = agg_hash(key) % AGG_HT_SIZE;
    for (AggBucket *b = ctx->ht[h]; b; b = b->next) {
        if (strcmp(b->group_key, key) == 0) return b;
    }

    /* New bucket: charge against the shared budget before allocating. */
    if (ctx->shared_buffer_bytes) {
        size_t bucket_bytes = sizeof(AggBucket) + (size_t)(kp + 1) +
                              (size_t)nvals * sizeof(char *) +
                              (size_t)ctx->nspecs * sizeof(AggAccum);
        for (int i = 0; i < nvals; i++) bucket_bytes += strlen(vals[i]) + 1;
        size_t prev = atomic_fetch_add_explicit(ctx->shared_buffer_bytes,
                                                bucket_bytes, memory_order_relaxed);
        if (prev + bucket_bytes > g_query_buffer_max_bytes) {
            atomic_fetch_sub_explicit(ctx->shared_buffer_bytes,
                                      bucket_bytes, memory_order_relaxed);
            ctx->budget_exceeded = 1;
            return NULL;
        }
    }

    /* Create new bucket */
    AggBucket *b = calloc(1, sizeof(AggBucket));
    b->group_key = strdup(key);
    b->group_vals = malloc(nvals * sizeof(char *));
    for (int i = 0; i < nvals; i++) b->group_vals[i] = strdup(vals[i]);
    b->accums = calloc(ctx->nspecs, sizeof(AggAccum));
    for (int i = 0; i < ctx->nspecs; i++) {
        b->accums[i].min = 1e308;
        b->accums[i].max = -1e308;
    }
    b->next = ctx->ht[h];
    ctx->ht[h] = b;
    ctx->total_buckets++;
    return b;
}

static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx) {
    AggCtx *ctx = (AggCtx *)raw_ctx;
    if (ctx->budget_exceeded) return 1;
    if (query_deadline_tick(ctx->deadline, &ctx->dl_counter)) return 1;
    const uint8_t *raw = block + hdr->key_len;

    /* Check full criteria tree (AND/OR) */
    if (!criteria_match_tree(raw, ctx->tree, ctx->fs)) return 0;

    /* Extract group_by values into stack buffers (no malloc). */
    char gbuf[MAX_FIELDS][512];
    char *gvals[MAX_FIELDS];
    for (int i = 0; i < ctx->ngroups; i++) {
        if (ctx->group_tfs[i]) {
            typed_field_to_buf_raw(ctx->group_tfs[i],
                                   raw + ctx->group_tfs[i]->offset,
                                   gbuf[i], sizeof(gbuf[i]));
        } else {
            /* Composite/unknown — fallback to decode_field */
            char *s = decode_field((const char *)raw, hdr->value_len,
                                   ctx->group_fields[i], ctx->fs);
            if (s) {
                size_t sl = strlen(s);
                if (sl >= sizeof(gbuf[i])) sl = sizeof(gbuf[i]) - 1;
                memcpy(gbuf[i], s, sl); gbuf[i][sl] = '\0';
                free(s);
            } else {
                gbuf[i][0] = '\0';
            }
        }
        gvals[i] = gbuf[i];
    }

    /* Find or create bucket */
    AggBucket *bkt;
    if (ctx->ngroups > 0) {
        bkt = agg_find_or_create(ctx, gvals, ctx->ngroups);
    } else {
        char *empty = "";
        bkt = agg_find_or_create(ctx, &empty, 1);
    }
    if (!bkt) return 1;  /* budget exceeded — stop scan */

    /* Accumulate — typed direct-to-double for numeric specs, fallback for composite. */
    for (int i = 0; i < ctx->nspecs; i++) {
        AggAccum *a = &bkt->accums[i];
        if (ctx->specs[i].fn == AGG_COUNT) { a->count++; continue; }

        double v;
        int present = 0;
        if (ctx->spec_tfs[i]) {
            present = typed_field_to_double(ctx->spec_tfs[i],
                                            raw + ctx->spec_tfs[i]->offset, &v);
        } else {
            char *fv = decode_field((const char *)raw, hdr->value_len,
                                    ctx->specs[i].field, ctx->fs);
            if (fv && fv[0]) { v = atof(fv); present = 1; }
            free(fv);
        }
        if (present) {
            a->count++;
            a->sum += v;
            if (v < a->min) a->min = v;
            if (v > a->max) a->max = v;
        }
    }
    return 0;
}

static int parse_agg_specs(const char *json, AggSpec **out) {
    /* Parse: [{"fn":"sum","field":"total","alias":"revenue"}, ...] */
    int cap = 16, n = 0;
    AggSpec *specs = calloc(cap, sizeof(AggSpec));
    const char *p = json;
    while (*p && *p != ']') {
        const char *obj = strchr(p, '{');
        if (!obj) break;
        const char *end = strchr(obj, '}');
        if (!end) break;
        char buf[1024];
        int len = end - obj + 1;
        if (len >= (int)sizeof(buf)) { p = end + 1; continue; }
        memcpy(buf, obj, len); buf[len] = '\0';

        if (n >= cap) { cap *= 2; specs = realloc(specs, cap * sizeof(AggSpec)); }
        AggSpec *s = &specs[n];
        memset(s, 0, sizeof(*s));

        JsonObj sobj;
        json_parse_object(buf, len, &sobj);
        char *fn    = json_obj_strdup(&sobj, "fn");
        char *field = json_obj_strdup(&sobj, "field");
        char *alias = json_obj_strdup(&sobj, "alias");

        if (fn) {
            if (strcmp(fn, "count") == 0) s->fn = AGG_COUNT;
            else if (strcmp(fn, "sum") == 0) s->fn = AGG_SUM;
            else if (strcmp(fn, "avg") == 0) s->fn = AGG_AVG;
            else if (strcmp(fn, "min") == 0) s->fn = AGG_MIN;
            else if (strcmp(fn, "max") == 0) s->fn = AGG_MAX;
        }
        if (field) strncpy(s->field, field, 255);
        if (alias) strncpy(s->alias, alias, 255);
        else if (fn && field) snprintf(s->alias, 255, "%s_%s", fn, field);
        else if (fn) strncpy(s->alias, fn, 255);

        free(fn); free(field); free(alias);
        n++;
        p = end + 1;
    }
    *out = specs;
    return n;
}

static int parse_group_by(const char *json, char out[][256]) {
    /* Parse: ["status","currency"] */
    int n = 0;
    const char *p = json;
    while (*p && n < MAX_FIELDS) {
        while (*p && *p != '"') p++;
        if (!*p) break;
        p++; /* skip opening quote */
        const char *start = p;
        while (*p && *p != '"') p++;
        int len = p - start;
        if (len > 0 && len < 256) {
            memcpy(out[n], start, len);
            out[n][len] = '\0';
            n++;
        }
        if (*p == '"') p++;
    }
    return n;
}

/* Get aggregate value by alias from a bucket, for having filter */
static double agg_bucket_value(AggBucket *bkt, AggSpec *specs, int nspecs, const char *alias) {
    for (int i = 0; i < nspecs; i++) {
        if (strcmp(specs[i].alias, alias) == 0) {
            AggAccum *a = &bkt->accums[i];
            switch (specs[i].fn) {
                case AGG_COUNT: return (double)a->count;
                case AGG_SUM:   return a->sum;
                case AGG_AVG:   return a->count > 0 ? a->sum / a->count : 0.0;
                case AGG_MIN:   return a->count > 0 ? a->min : 0.0;
                case AGG_MAX:   return a->count > 0 ? a->max : 0.0;
            }
        }
    }
    return 0.0;
}

/* Check having criteria against a bucket */
static int agg_having_match(AggBucket *bkt, AggSpec *specs, int nspecs,
                            SearchCriterion *having, int nhaving) {
    for (int i = 0; i < nhaving; i++) {
        double val = agg_bucket_value(bkt, specs, nspecs, having[i].field);
        char val_str[64];
        snprintf(val_str, sizeof(val_str), "%.6f", val);
        if (!match_criterion(val_str, &having[i])) return 0;
    }
    return 1;
}

/* Collect all buckets into a flat array */
static AggBucket **agg_collect(AggCtx *ctx, int *out_count) {
    AggBucket **arr = malloc(ctx->total_buckets * sizeof(AggBucket *));
    int n = 0;
    for (int i = 0; i < AGG_HT_SIZE; i++) {
        for (AggBucket *b = ctx->ht[i]; b; b = b->next) {
            arr[n++] = b;
        }
    }
    *out_count = n;
    return arr;
}

/* Clone shared read-only context into a per-worker AggCtx (own hash table). */
static void agg_ctx_clone_shared(AggCtx *dst, const AggCtx *src) {
    memset(dst, 0, sizeof(*dst));
    dst->tree = src->tree;
    dst->fs = src->fs;
    dst->shared_buffer_bytes = src->shared_buffer_bytes;
    memcpy(dst->group_fields, src->group_fields, sizeof(src->group_fields));
    dst->ngroups = src->ngroups;
    memcpy(dst->group_tfs, src->group_tfs, sizeof(src->group_tfs));
    dst->specs = src->specs;
    dst->nspecs = src->nspecs;
    memcpy(dst->spec_tfs, src->spec_tfs, sizeof(src->spec_tfs));
    dst->deadline = src->deadline;
    /* dl_counter stays 0 (per-worker local) */
}

/* Merge src's hash table into dst, freeing src's buckets in place. */
static void agg_ctx_merge(AggCtx *dst, AggCtx *src) {
    int nvals = src->ngroups > 0 ? src->ngroups : 1;
    for (int i = 0; i < AGG_HT_SIZE; i++) {
        AggBucket *b = src->ht[i];
        while (b) {
            AggBucket *next = b->next;
            AggBucket *dbkt = agg_find_or_create(dst, b->group_vals, nvals);
            for (int k = 0; k < src->nspecs; k++) {
                AggAccum *sa = &b->accums[k], *da = &dbkt->accums[k];
                da->sum += sa->sum;
                da->count += sa->count;
                if (sa->count > 0) {
                    if (sa->min < da->min) da->min = sa->min;
                    if (sa->max > da->max) da->max = sa->max;
                }
            }
            free(b->group_key);
            for (int j = 0; j < src->ngroups; j++) free(b->group_vals[j]);
            if (src->ngroups == 0) free(b->group_vals[0]);
            free(b->group_vals);
            free(b->accums);
            free(b);
            b = next;
        }
        src->ht[i] = NULL;
    }
    src->total_buckets = 0;
}

/* Per-shard aggregate worker: own AggCtx, processes this shard's hashes. */
typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    CollectedHash *entries;
    int entry_count;
    AggCtx local;
    QueryDeadline *deadline;
    int dl_counter;
} ShardAggCtx;

static void *shard_agg_worker(void *arg) {
    ShardAggCtx *sa = (ShardAggCtx *)arg;
    if (sa->entry_count == 0) return NULL;

    int sid = sa->entries[0].shard_id;
    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sa->db_root, sa->object, sid);
    int fd = open(shard, O_RDONLY);
    if (fd < 0) return NULL;
    struct stat st; fstat(fd, &st);
    if (st.st_size == 0) { close(fd); return NULL; }
    uint8_t *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return NULL;
    if ((size_t)st.st_size < SHARD_HDR_SIZE) { munmap(map, st.st_size); return NULL; }
    const ShardHeader *sh = (const ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC || sh->slots_per_shard == 0) { munmap(map, st.st_size); return NULL; }
    uint32_t slots = sh->slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sa->entry_count; ei++) {
        if (query_deadline_tick(sa->deadline, &sa->dl_counter)) break;
        CollectedHash *e = &sa->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;
            const uint8_t *block = map + zoneB_off(s, slots, sa->sch->slot_size);
            agg_scan_cb(h, block, &sa->local);
            break;
        }
    }
    munmap(map, st.st_size);
    return NULL;
}

/* Orchestrate parallel indexed aggregate: fan out per-shard workers with
   local AggCtx each, then merge into main_ctx. */
static void parallel_indexed_agg(AggCtx *main_ctx, const char *db_root,
                                 const char *object, const Schema *sch,
                                 CollectedHash *batch, int batch_count) {
    if (batch_count == 0) return;
    qsort(batch, batch_count, sizeof(CollectedHash), cmp_by_shard);

    int nshard_groups = 0;
    int group_starts[1024], group_sizes[1024];
    int prev_sid = -1;
    for (int i = 0; i < batch_count && nshard_groups < 1024; i++) {
        if (batch[i].shard_id != prev_sid) {
            group_starts[nshard_groups] = i;
            if (nshard_groups > 0)
                group_sizes[nshard_groups - 1] = i - group_starts[nshard_groups - 1];
            prev_sid = batch[i].shard_id;
            nshard_groups++;
        }
    }
    if (nshard_groups > 0)
        group_sizes[nshard_groups - 1] = batch_count - group_starts[nshard_groups - 1];

    ShardAggCtx *workers = calloc(nshard_groups, sizeof(ShardAggCtx));
    for (int g = 0; g < nshard_groups; g++) {
        workers[g].db_root = db_root;
        workers[g].object = object;
        workers[g].sch = sch;
        workers[g].entries = &batch[group_starts[g]];
        workers[g].entry_count = group_sizes[g];
        workers[g].deadline = main_ctx->deadline;
        agg_ctx_clone_shared(&workers[g].local, main_ctx);
    }

    if (batch_count < 1024 || nshard_groups <= 2) {
        for (int g = 0; g < nshard_groups; g++) shard_agg_worker(&workers[g]);
    } else {
        parallel_for(shard_agg_worker, workers, nshard_groups, sizeof(ShardAggCtx));
    }

    for (int g = 0; g < nshard_groups; g++) {
        if (workers[g].local.budget_exceeded) main_ctx->budget_exceeded = 1;
        agg_ctx_merge(main_ctx, &workers[g].local);
    }
    free(workers);
}

/* Sort context for qsort */
static AggSpec *g_sort_specs;
static int g_sort_nspecs;
static char g_sort_field[256];
static int g_sort_desc;
static int g_sort_ngroups;
static char (*g_sort_group_fields)[256];

static int agg_sort_cmp(const void *a, const void *b) {
    AggBucket *ba = *(AggBucket **)a;
    AggBucket *bb = *(AggBucket **)b;

    /* Check if order_by is a group_by field */
    int ga = -1, gb = -1;
    for (int i = 0; i < g_sort_ngroups; i++) {
        if (strcmp(g_sort_group_fields[i], g_sort_field) == 0) {
            ga = gb = i; break;
        }
    }
    if (ga >= 0) {
        int cmp = strcmp(ba->group_vals[ga], bb->group_vals[gb]);
        return g_sort_desc ? -cmp : cmp;
    }

    /* Otherwise it's an aggregate alias */
    double va = agg_bucket_value(ba, g_sort_specs, g_sort_nspecs, g_sort_field);
    double vb = agg_bucket_value(bb, g_sort_specs, g_sort_nspecs, g_sort_field);
    if (va < vb) return g_sort_desc ? 1 : -1;
    if (va > vb) return g_sort_desc ? -1 : 1;
    return 0;
}

static void agg_free(AggCtx *ctx) {
    for (int i = 0; i < AGG_HT_SIZE; i++) {
        AggBucket *b = ctx->ht[i];
        while (b) {
            AggBucket *next = b->next;
            free(b->group_key);
            for (int j = 0; j < ctx->ngroups; j++) free(b->group_vals[j]);
            free(b->group_vals);
            free(b->accums);
            free(b);
            b = next;
        }
    }
    free(ctx->specs);
}

/* Format a double, removing trailing zeros */
static void fmt_double(char *buf, size_t sz, double v) {
    snprintf(buf, sz, "%.6f", v);
    /* Trim trailing zeros after decimal point */
    char *dot = strchr(buf, '.');
    if (dot) {
        char *end = buf + strlen(buf) - 1;
        while (end > dot && *end == '0') *end-- = '\0';
        if (end == dot) *end = '\0';
    }
}

/* Indexed aggregate callback — fetches record inline + accumulates. No hash storage. */
typedef struct {
    AggCtx *agg;
    const Schema *sch;
    const char *db_root;
    const char *object;
    SearchCriterion *primary_crit;   /* pointer into tree (for check_primary pre-filter) */
    int check_primary;
} IdxAggCtx;

static int idx_agg_cb(const char *val, size_t vlen, const uint8_t *hash16, void *raw) {
    IdxAggCtx *ia = (IdxAggCtx *)raw;
    if (ia->check_primary && ia->primary_crit) {
        char tmp[1028];
        size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
        memcpy(tmp, val, cl); tmp[cl] = '\0';
        if (!match_criterion(tmp, ia->primary_crit)) return 0;
    }
    int r_shard_id, r_slot;
    addr_from_hash(hash16, ia->sch->splits, &r_shard_id, &r_slot);
    char r_shard[PATH_MAX];
    build_shard_path(r_shard, sizeof(r_shard), ia->db_root, ia->object, r_shard_id);
    FcacheRead fc = fcache_get_read(r_shard);
    if (!fc.map) return 0;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;
    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)r_slot + p) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash16, 16) != 0) continue;
        const uint8_t *block = fc.map + zoneB_off(s, slots, ia->sch->slot_size);
        agg_scan_cb(h, block, ia->agg);
        break;
    }
    fcache_release(fc);
    return 0;
}

int cmd_aggregate(const char *db_root, const char *object,
                  const char *criteria_json, const char *group_by_json,
                  const char *aggregates_json, const char *having_json,
                  const char *order_by, int order_desc, int limit,
                  const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    if (!aggregates_json || aggregates_json[0] == '\0') {
        OUT("{\"error\":\"Missing aggregates\"}\n");
        return -1;
    }

    Schema sch = load_schema(db_root, object);
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);

    /* Parse aggregates */
    AggSpec *specs = NULL;
    int nspecs = parse_agg_specs(aggregates_json, &specs);
    if (nspecs == 0) {
        OUT("{\"error\":\"No valid aggregates\"}\n");
        free(specs);
        return -1;
    }

    /* Parse criteria into tree (AND/OR supported). */
    CriteriaNode *tree = NULL;
    if (criteria_json && criteria_json[0]) {
        const char *perr = NULL;
        tree = parse_criteria_tree(criteria_json, &perr);
        if (perr) {
            OUT("{\"error\":\"bad criteria: %s\"}\n", perr);
            free_criteria_tree(tree);
            free(specs);
            return -1;
        }
    }

    /* Fast path: count-only with no criteria and no group_by → metadata */
    int no_group = (!group_by_json || group_by_json[0] == '\0' || strcmp(group_by_json, "[]") == 0);
    if (!tree && no_group && nspecs == 1 && specs[0].fn == AGG_COUNT) {
        int n = get_live_count(db_root, object);
        OUT("{\"%s\":%d}\n", specs[0].alias, n);
        free(specs);
        return 0;
    }

    compile_criteria_tree(tree, fs.ts);

    /* Build context */
    AggCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.tree = tree;
    ctx.fs = &fs;
    ctx.specs = specs;
    ctx.nspecs = nspecs;
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    ctx.deadline = &dl;
    _Atomic size_t agg_budget_bytes = 0;
    atomic_init(&agg_budget_bytes, 0);
    ctx.shared_buffer_bytes = &agg_budget_bytes;

    /* Parse group_by */
    if (group_by_json && group_by_json[0])
        ctx.ngroups = parse_group_by(group_by_json, ctx.group_fields);

    /* Pre-resolve TypedField pointers for group_by and agg specs — NULL means
       composite ("a+b") or unknown field, which falls back to decode_field. */
    for (int i = 0; i < ctx.ngroups; i++) {
        ctx.group_tfs[i] = NULL;
        if (fs.ts && !strchr(ctx.group_fields[i], '+')) {
            int idx = typed_field_index(fs.ts, ctx.group_fields[i]);
            if (idx >= 0) ctx.group_tfs[i] = &fs.ts->fields[idx];
        }
    }
    for (int i = 0; i < ctx.nspecs && i < MAX_AGG_SPECS; i++) {
        ctx.spec_tfs[i] = NULL;
        if (ctx.specs[i].fn != AGG_COUNT && fs.ts && !strchr(ctx.specs[i].field, '+')) {
            int idx = typed_field_index(fs.ts, ctx.specs[i].field);
            if (idx >= 0) ctx.spec_tfs[i] = &fs.ts->fields[idx];
        }
    }

    QueryPlan plan = choose_primary_source(tree, db_root, object);

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    if (plan.kind == PRIMARY_LEAF) {
        /* Indexed aggregate: collect hashes → group by shard → batch process */
        SearchCriterion *pc = plan.primary_leaf;
        enum SearchOp op = pc->op;
        int check_primary = (op == OP_CONTAINS || op == OP_LIKE || op == OP_ENDS_WITH ||
                             op == OP_NOT_LIKE || op == OP_NOT_CONTAINS || op == OP_NOT_IN);

        CollectCtx cc;
        memset(&cc, 0, sizeof(cc));
        cc.cap = 4096;
        cc.entries = malloc(cc.cap * sizeof(CollectedHash));
        cc.splits = sch.splits;
        cc.primary_crit = pc;
        cc.check_primary = check_primary;
        cc.deadline = &dl;

        btree_dispatch(plan.primary_idx_path, pc, collect_hash_cb, &cc);

        if (cc.budget_exceeded) ctx.budget_exceeded = 1;
        else parallel_indexed_agg(&ctx, db_root, object, &sch, cc.entries, (int)cc.count);

        free(cc.entries);
    } else if (plan.kind == PRIMARY_KEYSET) {
        /* Shape C / hybrid: build KeySet from OR index-union, feed each keyed
           record through agg_scan_cb (which re-checks the full tree). */
        int budget_exceeded = 0;
        keyset_agg_from_or(db_root, object, &sch, &ctx, plan.or_node, &dl, &budget_exceeded);
        if (budget_exceeded) ctx.budget_exceeded = 1;
    } else {
        /* Full scan fallback */
        scan_shards(data_dir, sch.slot_size, agg_scan_cb, &ctx);
    }

    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
        free_criteria_tree(tree);
        agg_free(&ctx);
        return -1;
    }
    if (ctx.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        free_criteria_tree(tree);
        agg_free(&ctx);
        return -1;
    }

    /* Collect buckets */
    int nbuckets = 0;
    AggBucket **buckets = agg_collect(&ctx, &nbuckets);

    /* Apply having filter */
    SearchCriterion *having = NULL;
    int nhaving = 0;
    if (having_json && having_json[0])
        parse_criteria_json(having_json, &having, &nhaving);

    if (nhaving > 0) {
        int dst = 0;
        for (int i = 0; i < nbuckets; i++) {
            if (agg_having_match(buckets[i], specs, nspecs, having, nhaving))
                buckets[dst++] = buckets[i];
        }
        nbuckets = dst;
    }

    /* Sort */
    if (order_by && order_by[0]) {
        g_sort_specs = specs;
        g_sort_nspecs = nspecs;
        strncpy(g_sort_field, order_by, 255);
        g_sort_desc = order_desc;
        g_sort_ngroups = ctx.ngroups;
        g_sort_group_fields = ctx.group_fields;
        qsort(buckets, nbuckets, sizeof(AggBucket *), agg_sort_cmp);
    }

    /* Apply limit */
    if (limit <= 0) limit = g_global_limit;
    if (nbuckets > limit) nbuckets = limit;

    /* Output — CSV path short-circuits before JSON emit. */
    if (csv_delim) {
        /* Header: group fields then aggregate aliases. */
        for (int g = 0; g < ctx.ngroups; g++) {
            if (g > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
            csv_emit_cell(ctx.group_fields[g], csv_delim);
        }
        for (int i = 0; i < nspecs; i++) {
            if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
            csv_emit_cell(specs[i].alias, csv_delim);
        }
        OUT("\n");

        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            for (int g = 0; g < ctx.ngroups; g++) {
                if (g > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                csv_emit_cell(b->group_vals[g], csv_delim);
            }
            for (int i = 0; i < nspecs; i++) {
                if (ctx.ngroups > 0 || i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                AggAccum *a = &b->accums[i];
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", a->count); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), a->sum); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0); break;
                    case AGG_MIN:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0); break;
                    case AGG_MAX:   fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0); break;
                    default:        vbuf[0] = '\0'; break;
                }
                csv_emit_cell(vbuf, csv_delim);
            }
            OUT("\n");
        }

        free(buckets);
        free_criteria_tree(tree);
        free_criteria(having, nhaving);
        agg_free(&ctx);
        return 0;
    }

    if (ctx.ngroups == 0 && nbuckets == 1) {
        /* No group_by: single object */
        AggBucket *b = buckets[0];
        OUT("{");
        for (int i = 0; i < nspecs; i++) {
            AggAccum *a = &b->accums[i];
            if (i > 0) OUT(",");
            char vbuf[64];
            switch (specs[i].fn) {
                case AGG_COUNT:
                    OUT("\"%s\":%ld", specs[i].alias, a->count);
                    break;
                case AGG_SUM:
                    fmt_double(vbuf, sizeof(vbuf), a->sum);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_AVG:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_MIN:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
                case AGG_MAX:
                    fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                    OUT("\"%s\":%s", specs[i].alias, vbuf);
                    break;
            }
        }
        OUT("}\n");
    } else {
        /* Group_by: array of objects */
        OUT("[");
        for (int bi = 0; bi < nbuckets; bi++) {
            AggBucket *b = buckets[bi];
            if (bi > 0) OUT(",");
            OUT("{");
            int first = 1;
            /* Group fields */
            for (int g = 0; g < ctx.ngroups; g++) {
                if (!first) OUT(",");
                OUT("\"%s\":\"%s\"", ctx.group_fields[g], b->group_vals[g]);
                first = 0;
            }
            /* Aggregate values */
            for (int i = 0; i < nspecs; i++) {
                AggAccum *a = &b->accums[i];
                if (!first) OUT(",");
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT:
                        OUT("\"%s\":%ld", specs[i].alias, a->count);
                        break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), a->sum);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_AVG:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->sum / a->count : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_MIN:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->min : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                    case AGG_MAX:
                        fmt_double(vbuf, sizeof(vbuf), a->count > 0 ? a->max : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf);
                        break;
                }
                first = 0;
            }
            OUT("}");
        }
        OUT("]\n");
    }

    free(buckets);
    free_criteria_tree(tree);
    free_criteria(having, nhaving);
    agg_free(&ctx);
    return 0;
}

