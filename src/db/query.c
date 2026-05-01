#include "types.h"
#include <fnmatch.h>

/* ========== Probing helpers ========== */

/* Check if slot at given offset matches our key. Returns:
   1 = exact match (same key, active)
   0 = empty slot (can write here)
  -1 = tombstone (deleted, can write here on insert, skip on get)
  -2 = occupied by different key (continue probing) */
volatile int g_scan_stop = 0; /* shared stop flag for parallel scan */

void scan_one_shard(const char *binpath, int slot_size,
                           scan_callback cb, void *ctx) {
    /* Use the persistent shard mmap cache. Earlier versions opened the file
       fresh with MADV_SEQUENTIAL on the theory that ucache's MADV_RANDOM
       hint would hurt linear scans, but in practice repeated bench runs
       (and the bench harness in particular) hit the same shards back-to-
       back — ucache keeps the pages hot across queries, which dwarfs the
       readahead benefit on a one-shot scan. */
    FcacheRead fc = fcache_get_read(binpath);
    if (!fc.map) return;
    uint8_t *map = fc.map;
    size_t file_size = fc.size;
    uint32_t shard_slots = fc.slots_per_shard;
    if (shard_slots == 0 || file_size < shard_zoneA_end(shard_slots)) {
        fcache_release(fc);
        return;
    }

    /* Find last used Zone A slot (metadata-only tail trim — tiny region). */
    size_t scan_end = shard_slots;
    while (scan_end > 0) {
        const SlotHeader *h = (const SlotHeader *)(map + zoneA_off(scan_end - 1));
        if (h->flag != 0 || h->key_len != 0) break;
        scan_end--;
    }

    /* Lock-free read loop: the callback is responsible for whatever
       synchronization it needs (most read-only counters use atomics; the
       few that emit output or mutate shared arrays take their own internal
       mutex in their ctx struct). Scanning itself is pure read of mmap'd
       data and must never serialize on a shared mutex — that eats all the
       per-shard parallelism. */
    for (size_t i = 0; i < scan_end; i++) {
        if (g_scan_stop) break;
        const SlotHeader *hdr = (const SlotHeader *)(map + zoneA_off(i));
        if (hdr->flag == 1) {
            const uint8_t *block = map + zoneB_off(i, shard_slots, slot_size);
            int stop = cb(hdr, block, ctx);
            if (stop) { g_scan_stop = 1; break; }
        }
    }
    fcache_release(fc);
}

typedef struct {
    const char *path;
    int slot_size;
    scan_callback cb;
    void *ctx;
    FILE *parent_out;  /* inherit g_out from parent thread */
} ScanWorkerArg;

void *scan_worker(void *arg) {
    ScanWorkerArg *w = (ScanWorkerArg *)arg;
    if (g_scan_stop) return NULL;
    g_out = w->parent_out ? w->parent_out : stdout;
    scan_one_shard(w->path, w->slot_size, w->cb, w->ctx);
    return NULL;
}

void scan_shards(const char *data_dir, int slot_size, scan_callback cb, void *ctx) {
    g_scan_stop = 0; /* reset stop flag */
    /* Collect all shard file paths */
    char **paths = NULL;
    int path_count = 0, path_cap = 256;
    paths = malloc(path_cap * sizeof(char *));
    if (!paths) return;

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(paths); return; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (path_count >= path_cap) {
            path_cap *= 2;
            char **t = realloc(paths, path_cap * sizeof(char *));
            if (!t) {
                for (int k = 0; k < path_count; k++) free(paths[k]);
                free(paths);
                paths = NULL;
                path_count = 0;
                break;
            }
            paths = t;
        }
        char binpath[PATH_MAX];
        snprintf(binpath, sizeof(binpath), "%s/%s", data_dir, e1->d_name);
        paths[path_count++] = strdup(binpath);
    }
    closedir(d1);

    if (!paths || path_count == 0) { free(paths); return; }

    ScanWorkerArg *args = malloc(path_count * sizeof(ScanWorkerArg));
    if (!args) {
        for (int i = 0; i < path_count; i++) free(paths[i]);
        free(paths);
        return;
    }
    for (int i = 0; i < path_count; i++) {
        args[i] = (ScanWorkerArg){ paths[i], slot_size, cb, ctx, g_out };
    }
    parallel_for(scan_worker, args, path_count, sizeof(ScanWorkerArg));
    free(args);

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
}

/* ========== SIZE ========== */

int cmd_size(const char *db_root, const char *object) {
    OUT("%d\n", get_live_count(db_root, object));
    return 0;
}

int cmd_orphaned(const char *db_root, const char *object) {
    OUT("%d\n", get_deleted_count(db_root, object));
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
    const TypedField *rhs_tf;   /* RHS for field-vs-field ops; NULL otherwise */
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

    /* OP_REGEX / OP_NOT_REGEX: pre-compiled POSIX extended regex.
       Heap-allocated so free_compiled_criteria can regfree+free safely
       even when the criterion is reused across many records. */
    regex_t  *re;
    int       re_compiled;        /* 1 iff regcomp succeeded; 0 → no match */
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

/* Sort `batch` in place by shard_id and split into per-shard groups. The
   group_starts[i]/group_sizes[i] pair describes a contiguous run of entries
   with the same shard_id, suitable for fan-out to per-shard workers.
   Returns the number of groups emitted (≤ max_groups). Excess shards beyond
   max_groups fold into the final group — same behavior the inline copies
   shipped before this was extracted. */
static int shard_group_batch(CollectedHash *batch, int batch_count,
                             int *group_starts, int *group_sizes,
                             int max_groups) {
    if (batch_count == 0) return 0;
    qsort(batch, batch_count, sizeof(CollectedHash), cmp_by_shard);
    int n = 0;
    int prev_sid = -1;
    for (int i = 0; i < batch_count && n < max_groups; i++) {
        if (batch[i].shard_id != prev_sid) {
            group_starts[n] = i;
            if (n > 0) group_sizes[n - 1] = i - group_starts[n - 1];
            prev_sid = batch[i].shard_id;
            n++;
        }
    }
    if (n > 0) group_sizes[n - 1] = batch_count - group_starts[n - 1];
    return n;
}

/* Collecting callback for B+ tree traversal */
typedef struct {
    CollectedHash *entries;
    size_t count;
    size_t cap;
    int splits;
    size_t limit;   /* max results to collect, 0 = unlimited */
} SearchCollectCtx;

static int search_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) __attribute__((unused));
static int search_collect_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    SearchCollectCtx *sc = (SearchCollectCtx *)ctx;
    if (sc->limit > 0 && sc->count >= sc->limit) return 1; /* stop */
    if (sc->count >= sc->cap) {
        size_t new_cap = sc->cap * 2;
        CollectedHash *t = xrealloc_or_free(sc->entries, new_cap * sizeof(CollectedHash));
        if (!t) { sc->entries = NULL; sc->count = 0; return 1; /* abort scan on OOM */ }
        sc->entries = t;
        sc->cap = new_cap;
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
                                    FieldSchema *fs, int mode) __attribute__((unused));
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

    /* Sort by shard_id and split into per-shard groups (heap arrays — count
       can exceed the stack-friendly cap used by other call sites). */
    int *group_starts = malloc((count + 1) * sizeof(int));
    int *group_sizes  = malloc((count + 1) * sizeof(int));
    int nshard_groups = shard_group_batch(entries, (int)count, group_starts, group_sizes, (int)count + 1);

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
static void *idx_build_worker(void *arg) __attribute__((unused));
static void *idx_build_worker(void *arg) {
    IdxBuildArg *ib = (IdxBuildArg *)arg;
    /* Merge-rebuild: sort new entries, merge with existing tree, rebuild from scratch.
       Much faster than btree_insert_batch for large datasets because it uses sequential
       I/O (leaf scan + bulk_build) instead of random B+ tree insertions. */
    btree_bulk_merge(ib->ipath, ib->pairs, ib->pair_count);
    return NULL;
}

/* Per-field bulk-merge that runs all idx_n shards' merges serially inside one
   worker. Trades shard-level parallelism for thread-pool dispatch efficiency:
   the prior layout dispatched nfields × idx_n tiny tasks (e.g. 14 × 16 = 224)
   into the 16-thread pool, paying full task-dispatch + parallel_for queue
   overhead per shard. The bench showed insert-with-pre-existing-indexes
   ~30 % slower than the pre-2026.05.1 single-file layout for that reason —
   bulk_merge's actual work-per-call is tiny (a few ms once warm), so the
   per-task overhead dominates. With nfields workers (≤16 in practice) the
   pool dispatches in one wave; each worker streams the 16 per-shard merges
   sequentially using its own thread, which the kernel page-cache handles
   well because consecutive shards of the same field share access patterns. */
typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    int splits;
    BtEntry *new_entries;     /* not owned; values freed by caller */
    size_t   new_count;
} IdxFieldArg;

static void *idx_build_field_worker(void *arg) {
    IdxFieldArg *fa = (IdxFieldArg *)arg;
    if (fa->new_count == 0) return NULL;
    int idx_n = index_splits_for(fa->splits);

    /* Bucket-sort new_entries by idx_shard. */
    size_t *counts  = calloc((size_t)idx_n, sizeof(size_t));
    size_t *offsets = calloc((size_t)idx_n, sizeof(size_t));
    BtEntry *parted = malloc(fa->new_count * sizeof(BtEntry));
    if (!counts || !offsets || !parted) {
        free(counts); free(offsets); free(parted);
        return NULL;
    }
    for (size_t i = 0; i < fa->new_count; i++)
        counts[idx_shard_for_hash(fa->new_entries[i].hash, fa->splits)]++;
    size_t acc = 0;
    for (int s = 0; s < idx_n; s++) { offsets[s] = acc; acc += counts[s]; }
    size_t *cursor = calloc((size_t)idx_n, sizeof(size_t));
    if (!cursor) { free(counts); free(offsets); free(parted); return NULL; }
    for (size_t i = 0; i < fa->new_count; i++) {
        int s = idx_shard_for_hash(fa->new_entries[i].hash, fa->splits);
        parted[offsets[s] + cursor[s]++] = fa->new_entries[i];
    }
    free(cursor);

    /* Serial per-shard bulk_merge — same ops as before, just in one thread. */
    for (int s = 0; s < idx_n; s++) {
        if (counts[s] == 0) continue;
        char path[PATH_MAX];
        build_idx_path(path, sizeof(path), fa->db_root, fa->object, fa->field, s);
        btree_bulk_merge(path, parted + offsets[s], counts[s]);
    }

    free(parted);
    free(counts);
    free(offsets);
    return NULL;
}

/* Partition `pairs` (of total `count`) by idx_shard and append per-shard
   IdxBuildArg slices to `out_args` (which must have room for at least
   index_splits_for(splits) entries). Returns the number of non-empty
   shard buckets appended. The pairs array is reordered in place; on
   return pairs[offset .. offset+counts[s]] holds shard s's entries. The
   caller still owns the BtEntry value strings (one allocation per pair,
   freed exactly once after the build). */
static int partition_pairs_by_idx_shard(BtEntry *pairs, size_t count,
                                        const char *db_root, const char *object,
                                        const char *field, int splits,
                                        IdxBuildArg *out_args) __attribute__((unused));
static int partition_pairs_by_idx_shard(BtEntry *pairs, size_t count,
                                        const char *db_root, const char *object,
                                        const char *field, int splits,
                                        IdxBuildArg *out_args) {
    int n = index_splits_for(splits);
    /* First pass: tally per-shard sizes. */
    size_t *counts = calloc((size_t)n, sizeof(size_t));
    if (!counts) return 0;
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        counts[s]++;
    }
    /* Compute prefix-sum offsets. */
    size_t *offsets = calloc((size_t)n, sizeof(size_t));
    if (!offsets) { free(counts); return 0; }
    size_t acc = 0;
    for (int s = 0; s < n; s++) { offsets[s] = acc; acc += counts[s]; }
    /* Second pass: scatter pairs into a temporary array. */
    BtEntry *tmp = malloc(count * sizeof(BtEntry));
    if (!tmp) { free(counts); free(offsets); return 0; }
    size_t *cursor = calloc((size_t)n, sizeof(size_t));
    if (!cursor) { free(counts); free(offsets); free(tmp); return 0; }
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        tmp[offsets[s] + cursor[s]++] = pairs[i];
    }
    memcpy(pairs, tmp, count * sizeof(BtEntry));
    free(tmp);
    free(cursor);

    int out_count = 0;
    for (int s = 0; s < n; s++) {
        if (counts[s] == 0) continue;
        build_idx_path(out_args[out_count].ipath, sizeof(out_args[out_count].ipath),
                       db_root, object, field, s);
        out_args[out_count].pairs = pairs + offsets[s];
        out_args[out_count].pair_count = counts[s];
        out_count++;
    }
    free(counts);
    free(offsets);
    return out_count;
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
    /* CAS: when if_not_exists is set, an existing-key probe match is treated
       as a no-op and counted in `skipped` instead of overwriting. */
    int             if_not_exists;
    int             skipped;
    /* Phase-2 profiling: total worker wall time and time spent inside
       ucache_grow_shard. Aggregated post-join to show "of this much
       Phase 2 time, X ms was grow." Helps isolate rehash cost. */
    uint64_t        wall_ms;
    uint64_t        grow_ms;
    int             grow_count;
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
    uint64_t t_worker_start = now_ms_coarse();

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
            uint64_t t_grow_start = now_ms_coarse();
            int grow_result = ucache_grow_shard(shard_path, sw->sch->slot_size, sw->sch->prealloc_mb);
            sw->grow_ms += now_ms_coarse() - t_grow_start;
            sw->grow_count++;
            if (grow_result < 0) {
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
        if (is_update && sw->if_not_exists) {
            /* CAS: caller asked us not to overwrite — count as skipped and
               leave the existing record alone. */
            sw->skipped++;
            i++;
            continue;
        }
        /* Capture old index keys BEFORE the slot rewrite. Without this,
           overwriting an existing key leaves stale (old-value, key-hash)
           pairs in every index — symptoms: idx_count_cb over-counts (no
           record fetch to filter the stale hit), find pays an extra
           record fetch per stale entry, index files grow per overwrite.
           Mirrors what cmd_update does for single-record overwrites. */
        uint8_t *old_idx_bufs[MAX_FIELDS];
        size_t   old_idx_lens[MAX_FIELDS];
        int      old_idx_have[MAX_FIELDS];
        memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
        memset(old_idx_lens, 0, sizeof(old_idx_lens));
        memset(old_idx_have, 0, sizeof(old_idx_have));
        if (is_update && sw->nidx > 0) {
            uint8_t *old_value_ptr = map + zoneB_off(slot, slots, sw->sch->slot_size) +
                                     existing->key_len;
            for (int fi = 0; fi < sw->nidx; fi++) {
                old_idx_have[fi] = build_index_key_from_record(
                    sw->ts, old_value_ptr, sw->idx_fields[fi],
                    &old_idx_bufs[fi], &old_idx_lens[fi]);
            }
        }
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

        /* Collect index entries from the typed payload.
           Composite indexes use the legacy ASCII-concat path.
           Single-field indexes use typed_field_to_index_key so the binary
           representation matches what the read side encodes via
           encode_field_for_index. Without this, every signed-numeric
           indexed query misses (write ASCII, read sign-flipped binary). */
        for (int fi = 0; fi < sw->nidx; fi++) {
            uint8_t *key_buf = NULL;
            size_t key_len = 0;

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
                if (ok && cp > 0) {
                    key_buf = malloc((size_t)cp);
                    memcpy(key_buf, cat, (size_t)cp);
                    key_len = (size_t)cp;
                }
            } else {
                int tidx = sw->idx_field_indices[fi][0];
                if (tidx >= 0) {
                    const TypedField *f = &sw->ts->fields[tidx];
                    size_t cap = (size_t)(f->size > 8 ? f->size : 8);
                    key_buf = malloc(cap);
                    typed_field_to_index_key(sw->ts, r->payload, tidx, key_buf, &key_len);
                    if (key_len == 0) { free(key_buf); key_buf = NULL; }
                }
            }

            int have_new = (key_buf != NULL && key_len > 0);
            int unchanged = old_idx_have[fi] && have_new &&
                            key_len == old_idx_lens[fi] &&
                            memcmp(key_buf, old_idx_bufs[fi], key_len) == 0;

            /* Drop stale btree entry on overwrite. Skip the drop iff old==new
               because re-adding via btree_bulk_merge would duplicate (the
               bulk path does not dedup on (value, hash) pairs the way
               btree_insert does). */
            if (old_idx_have[fi] && !unchanged) {
                delete_index_entry(sw->db_root, sw->object, sw->idx_fields[fi],
                                   sw->sch->splits,
                                   old_idx_bufs[fi], old_idx_lens[fi], r->hash);
            }
            if (old_idx_have[fi]) free(old_idx_bufs[fi]);

            if (unchanged) {
                /* No-op: old btree entry still points at this hash with the
                   same value bytes. Don't queue a new entry — would duplicate. */
                free(key_buf);
                continue;
            }

            if (have_new) {
                if (sw->idx_pair_counts[fi] >= sw->idx_pair_caps[fi]) {
                    size_t new_cap = sw->idx_pair_caps[fi] ? sw->idx_pair_caps[fi] * 2 : 64;
                    BtEntry *t = xrealloc_or_free(sw->idx_pairs[fi], new_cap * sizeof(BtEntry));
                    if (!t) {
                        log_msg(1, "INDEX_OOM shard=%d field=%s (dropped index pair on realloc; rerun reindex)",
                                sw->shard_id, sw->idx_fields[fi]);
                        sw->idx_pairs[fi] = NULL;
                        sw->idx_pair_counts[fi] = 0;
                        sw->idx_pair_caps[fi] = 0;
                        free(key_buf);
                        continue;
                    }
                    sw->idx_pairs[fi] = t;
                    sw->idx_pair_caps[fi] = new_cap;
                }
                BtEntry *bp = &sw->idx_pairs[fi][sw->idx_pair_counts[fi]++];
                bp->value = (const char *)key_buf;
                bp->vlen  = key_len;
                memcpy(bp->hash, r->hash, 16);
            } else {
                free(key_buf);
            }
        }

        i++;
    }

    if (wh.map) {
        /* Note: we previously called ucache_nudge_writeback(wh.slot) here to
           kick the kernel's writeback early, but sync_file_range(nbytes=0)
           ended up blocking in balance_dirty_pages under concurrent
           writeback from all 16 workers — turning "non-blocking nudge"
           into "synchronous flush storm" at 5 M+ scale. Left the helper
           in storage.c for future targeted use (e.g. ranged flush of just
           what we touched) but no longer fire it on the shard-worker end
           path. The regular kernel writeback daemons handle durability. */
        ucache_write_release(wh);
    }
    sw->wall_ms = now_ms_coarse() - t_worker_start;
    return NULL;
}

/* Internal: bulk insert from a json string already in memory (no file I/O) */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str, int if_not_exists);

int cmd_bulk_insert(const char *db_root, const char *object, const char *input,
                    int if_not_exists) {
    uint64_t t0 = now_ms_coarse();
    size_t len;
    char *json;
    int json_mmaped = 0;
    if (input) {
        /* mmap the file instead of malloc — OS pages in/out as we scan */
        int ifd = open(input, O_RDONLY);
        if (ifd < 0) { fprintf(stderr, "Error: Cannot open %s\n", input); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); fprintf(stderr, "Error: Cannot stat %s\n", input); return 1; }
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
        if (!json) { fprintf(stderr, "Error: out of memory reading stdin\n"); return 1; }
        int c;
        while ((c = fgetc(stdin)) != EOF) {
            if (pos >= cap - 1) {
                cap *= 2;
                char *t = xrealloc_or_free(json, cap);
                if (!t) { json = NULL; break; }
                json = t;
            }
            json[pos++] = c;
        }
        if (!json) { fprintf(stderr, "Error: out of memory reading stdin\n"); return 1; }
        json[pos] = '\0'; len = pos;
    }
    /* json is non-NULL on all paths reaching here: the file branch (988-1015)
       returns early on every alloc failure; the stdin branch (1016-1032)
       returns at the initial-malloc and post-realloc NULL guards. */

    /* Load config ONCE */
    Schema sc = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
    (void)nfields; /* indexes are walked per-shard later via load_index_fields */

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
    if (ts && ts->total_size > sc.max_value) {
        fprintf(stderr, "Error: typed record size %d exceeds max_value %d\n",
                ts->total_size, sc.max_value);
        if (json_mmaped) munmap(json, len + 1); else free(json);
        return 1;
    }

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

    const char *p = json_skip(json);
    int is_object_format = (*p == '{'); /* {"k1":{...},"k2":{...}} */
    int is_array_format = (*p == '[');  /* [{"id":"k1","data":{...}},...] */

    if (!is_object_format && !is_array_format) {
        fprintf(stderr, "Error: Expected JSON object or array\n");
        if (json_mmaped) munmap(json, len + 1); else free(json);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        return 1;
    }
    p++;

    int count = 0, errors = 0;

    /* ===== Phase 1: parse every record into a buffered records[] array. Per-record
       `id` + `payload` (ts->total_size bytes) are bump-allocated from a single
       BulkArena so the 2-mallocs-per-record cost is replaced by O(1) pointer
       advance. Arena freed after Phase-2 workers join — ordering holds because
       the post-phase merge loop reads records[i].id/.payload BEFORE arena_free. */
    BulkArena *arena = arena_new(256 * 1024);
    size_t rec_cap = 1024, rec_count = 0;
    BulkInsRecord *records = malloc(rec_cap * sizeof(BulkInsRecord));

    while (*p) {
        p = json_skip(p);
        if (*p == ']' || *p == '}') break;
        if (*p == ',') { p++; continue; }

        char *id = NULL;
        size_t klen = 0;
        const char *data_ptr = NULL;  /* span into json mmap (object fmt) or obj_str (array fmt) */
        /* obj_str owns the array-format record's NUL-terminated copy so
           json_parse_object can walk it; must stay alive through the
           typed_encode_defaults call below. Freed at end of iteration. */
        char obj_buf[8192];
        char *obj_str = NULL;
        int obj_heap = 0;

        if (is_object_format) {
            /* Object format: "key": {...} */
            if (*p != '"') { p++; continue; }
            p++;
            const char *key_start = p;
            while (*p && *p != '"') p++;
            klen = p - key_start;
            id = arena_strndup(&arena, key_start, klen);
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p = json_skip(p + 1);

            /* Data span points into the original mmap — NUL-terminated at
               buffer end; typed_encode_defaults stops at the matching brace. */
            data_ptr = p;
            p = json_skip_value(p);
        } else {
            /* Array format: {"id":"k1","data":{...}} */
            if (*p != '{') { p++; continue; }
            const char *obj_start = p;
            const char *obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;

            if (obj_len < sizeof(obj_buf)) {
                memcpy(obj_buf, obj_start, obj_len);
                obj_buf[obj_len] = '\0';
                obj_str = obj_buf;
            } else {
                obj_str = malloc(obj_len + 1);
                memcpy(obj_str, obj_start, obj_len);
                obj_str[obj_len] = '\0';
                obj_heap = 1;
            }

            JsonObj rec;
            json_parse_object(obj_str, obj_len, &rec);
            const char *iv; size_t ivl;
            if (json_obj_unquoted(&rec, "id", &iv, &ivl)) {
                id = arena_strndup(&arena, iv, ivl);
                klen = ivl;
            }
            const char *dv; size_t dl;
            if (json_obj_get(&rec, "data", &dv, &dl)) {
                data_ptr = dv;  /* span into obj_str */
                (void)dl;       /* encoder finds matching brace itself */
            }
            p = obj_end;
        }

        if (id && data_ptr) {
            if ((int)klen > sc.max_key) {
                errors++;
                /* id lives in arena — dropped bytes are trivial, no free here */
            } else {
                /* Allocate payload in arena up front and encode the record
                   directly into it — skips the typed_tmp bounce + memcpy. */
                uint8_t *payload = arena_alloc(&arena, ts->total_size);
                typed_encode_defaults(ts, data_ptr, payload, ts->total_size,
                                      db_root, object);

                if (rec_count >= rec_cap) {
                    rec_cap *= 2;
                    BulkInsRecord *t = xrealloc_or_free(records, rec_cap * sizeof(*t));
                    if (!t) {
                        /* Reset rec_count so phase 1.5 below sees an empty
                           set instead of dereferencing NULL records. */
                        records = NULL;
                        rec_count = 0;
                        break;
                    }
                    records = t;
                }
                BulkInsRecord *r = &records[rec_count++];
                r->id = id;
                r->payload = payload;
                r->klen = klen;
                compute_addr(id, klen, sc.splits, r->hash, &r->shard_id, &r->start_slot);
            }
        }
        if (obj_heap) free(obj_str);
    }

    /* ===== Phase 1.5: bucket records by shard_id so each worker owns one shard's
       writes and can hold the ucache wrlock once for the entire bucket.
       OOM at any of the allocs below frees every prior allocation
       (records, arena, idx_pairs[], idx_pair_*, json buffer) in reverse
       order before bailing — same cleanup the success path runs at the
       function tail, just earlier. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    if (!shard_counts) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        if (json_mmaped) munmap(json, len + 1); else free(json);
        OUT("{\"error\":\"oom: bulk_insert shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    if ((nshard_groups > 0 && !workers) || !shard_to_worker) {
        free(workers); free(shard_to_worker);
        free(shard_counts); free(records);
        arena_free(arena);
        for (int i = 0; i < nfields; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        if (json_mmaped) munmap(json, len + 1); else free(json);
        OUT("{\"error\":\"oom: bulk_insert workers\"}\n");
        return 1;
    }
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
    /* Invariant: rec_count > 0 ⇒ at least one shard has count > 0 ⇒
       nshard_groups > 0 ⇒ workers != NULL (post-OOM-guard above).
       Coverity can't trace the chain, so the loop body's workers[w]
       deref is flagged FORWARD_NULL — annotate. */
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        /* coverity[forward_null] workers is non-NULL when rec_count > 0 */
        workers[w].records[workers[w].count++] = records[i];  /* shallow copy, ownership transferred */
    }
    free(records);
    free(shard_counts);
    free(shard_to_worker);

    /* Wire read-only worker context + allocate per-worker idx-entry collectors. */
    for (int wi = 0; wi < nshard_groups; wi++) {
        /* coverity[forward_null] same invariant — nshard_groups > 0 ⇒ workers non-NULL */
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
        ws->skipped = 0;
        ws->if_not_exists = if_not_exists;
        ws->idx_pairs = nfields > 0 ? calloc(nfields, sizeof(BtEntry *)) : NULL;
        ws->idx_pair_counts = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
        ws->idx_pair_caps = nfields > 0 ? calloc(nfields, sizeof(size_t)) : NULL;
    }

    uint64_t t1 = now_ms_coarse();  /* end of Phase 1 (parse + bucket) */

    /* ===== Phase 2: run shard workers in parallel. Each worker owns one shard's
       writes so ucache wrlocks are disjoint across workers — no cross-worker
       coordination needed. Batched pthread_create/join pattern matches
       bulk_del_shard_worker. Serial fallback when thread count ≤ 1 or workload
       is small enough that spawn/join overhead would dominate. */
    parallel_for(bulk_insert_shard_worker, workers, nshard_groups,
                 sizeof(BulkInsShardWork));
    uint64_t t2 = now_ms_coarse();  /* end of Phase 2 (parallel shard write) */

    /* Phase-2 breakdown across workers. */
    uint64_t grow_ms_total = 0, wall_ms_max = 0, wall_ms_sum = 0;
    int grow_count_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        grow_ms_total  += workers[wi].grow_ms;
        grow_count_total += workers[wi].grow_count;
        wall_ms_sum    += workers[wi].wall_ms;
        if (workers[wi].wall_ms > wall_ms_max) wall_ms_max = workers[wi].wall_ms;
    }

    /* Merge per-worker results into the caller's global counters and index arrays,
       then release per-worker scratch. BtEntry.value ownership transfers into
       the global idx_pairs — freed later by the idx-build cleanup. */
    int skipped_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;
        skipped_total += ws->skipped;

        for (int fi = 0; fi < nfields; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                size_t new_cap = idx_pair_caps[fi];
                while (idx_pair_counts[fi] + add > new_cap) new_cap *= 2;
                BtEntry *t = xrealloc_or_free(idx_pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    log_msg(1, "INDEX_OOM merge field_idx=%d (dropped %zu pairs; rerun reindex)",
                            fi, idx_pair_counts[fi] + add);
                    idx_pairs[fi] = NULL;
                    idx_pair_counts[fi] = 0;
                    idx_pair_caps[fi] = 0;
                    /* free the per-worker key_bufs we'd have copied in */
                    for (size_t k = 0; k < add; k++) free((void *)ws->idx_pairs[fi][k].value);
                    free(ws->idx_pairs[fi]);
                    continue;
                }
                idx_pairs[fi] = t;
                idx_pair_caps[fi] = new_cap;
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
    uint64_t t3 = now_ms_coarse();  /* end of Phase 3 (activate) */

    /* ucache keeps mmaps open — OS flushes dirty pages */
    if (json_mmaped) munmap(json, len + 1);  /* len+1 matches mmap size */
    else free(json);

    /* Bulk write indexes — one worker per field; the worker streams the per-
       shard merges sequentially. Halves dispatch overhead vs the old
       per-(field, shard) layout that flooded the 16-thread pool with
       nfields × idx_n tiny tasks. */
    if (nfields > 0) {
        IdxFieldArg *fa = malloc((size_t)nfields * sizeof(IdxFieldArg));
        int fa_count = 0;
        for (int fi = 0; fi < nfields; fi++) {
            if (idx_pair_counts[fi] == 0) continue;
            fa[fa_count++] = (IdxFieldArg){
                .db_root = db_root, .object = object, .field = idx_fields[fi],
                .splits = sc.splits,
                .new_entries = idx_pairs[fi], .new_count = idx_pair_counts[fi],
            };
        }
        parallel_for(idx_build_field_worker, fa, fa_count, sizeof(IdxFieldArg));
        free(fa);
        /* Free the value strings — owned by idx_pairs[fi]. */
        for (int fi = 0; fi < nfields; fi++) {
            for (size_t ei = 0; ei < idx_pair_counts[fi]; ei++)
                free((char *)idx_pairs[fi][ei].value);
            free(idx_pairs[fi]);
        }
    }
    free(idx_pairs);
    free(idx_pair_counts);
    free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);

    uint64_t t4 = now_ms_coarse();  /* end of Phase 4 (index build) */
    log_msg(3, "BULK-INSERT %s: rows=%d phase1_parse=%lums phase2_write=%lums (grows=%d grow_total=%lums per_worker_max=%lums) phase3_activate=%lums phase4_index=%lums total=%lums",
            object, count,
            (unsigned long)(t1 - t0),
            (unsigned long)(t2 - t1),
            grow_count_total,
            (unsigned long)grow_ms_total,
            (unsigned long)wall_ms_max,
            (unsigned long)(t3 - t2),
            (unsigned long)(t4 - t3),
            (unsigned long)(t4 - t0));

    if (errors) {
        OUT("{\"inserted\":%d,\"skipped\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n",
            count, skipped_total, errors);
        fprintf(stderr, "%d errors during bulk insert (see info log for dropped keys)\n", errors);
    } else if (skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, skipped_total);
    } else {
        /* Backward-compat: no skipped column when caller didn't ask for CAS. */
        OUT("{\"count\":%d}\n", count);
    }
    return errors > 0 ? 1 : 0;
}

/* Bulk insert from a string already in memory — no temp file needed */
int cmd_bulk_insert_string(const char *db_root, const char *object, char *json_str,
                           int if_not_exists) {
    /* Write to a memfd (in-memory file) so cmd_bulk_insert can mmap it */
    size_t slen = strlen(json_str);
    int memfd = memfd_create("shard-db_bulk", 0);
    if (memfd < 0) {
        /* Fallback: temp file */
        char tmp[PATH_MAX];
        snprintf(tmp, sizeof(tmp), "/tmp/shard-db_bulk_%d_%d.json", getpid(), (int)pthread_self());
        FILE *tf = fopen(tmp, "w");
        if (tf) { fwrite(json_str, 1, slen, tf); fclose(tf); }
        int r = cmd_bulk_insert(db_root, object, tmp, if_not_exists);
        unlink(tmp);
        return r;
    }
    ftruncate(memfd, slen);
    write(memfd, json_str, slen);
    char fdpath[64];
    snprintf(fdpath, sizeof(fdpath), "/proc/self/fd/%d", memfd);
    int r = cmd_bulk_insert(db_root, object, fdpath, if_not_exists);
    close(memfd);
    return r;
}

/* ========== BULK INSERT (DELIMITED TEXT FILE) ========== */

int cmd_bulk_insert_delimited(const char *db_root, const char *object,
                               const char *filepath, char delimiter,
                               int if_not_exists) {
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
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */

    /* mmap the file */
    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st;
    if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
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

    /* Invariant check hoisted out of the per-record loop — ts->total_size
       and sc.max_value don't change during a bulk insert. */
    if (ts->total_size > sc.max_value) {
        if (data_mmaped) munmap((void *)data, st.st_size); else free((void *)data);
        OUT("{\"error\":\"typed record size exceeds max_value\"}\n");
        return 1;
    }

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

        /* Skip oversized keys before any encode work. (ts->total_size >
           sc.max_value was already hoisted above.) */
        if ((int)klen > sc.max_key) { errors++; continue; }

        /* Walk remaining spans into vals[] without copying. */
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

        /* Arena-allocated key + typed payload — survive until arena_free()
           post-Phase-2. Encode directly into the arena payload (zero-init
           first, then encode_field_len writes each field in place); skips
           the typed_tmp bounce + memcpy that used to sit between them. */
        char *id = arena_strndup(&arena, id_start, klen);
        uint8_t *payload = arena_alloc(&arena, ts->total_size);
        memset(payload, 0, ts->total_size);

        if (!has_tombstones) {
            for (int i = 0; i < active_count && i < nvals; i++) {
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[i], vals[i].ptr, vals[i].len,
                                     payload + ts->fields[i].offset);
            }
        } else {
            for (int i = 0; i < active_count && i < nvals; i++) {
                int fi = active_indices[i];
                if (vals[i].len > 0)
                    encode_field_len(&ts->fields[fi], vals[i].ptr, vals[i].len,
                                     payload + ts->fields[fi].offset);
            }
        }

        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            BulkInsRecord *t = xrealloc_or_free(records, rec_cap * sizeof(*t));
            if (!t) {
                /* xrealloc_or_free already freed records; reset rec_count
                   so phase 1.5 below sees an empty set and skips the
                   shard-bucket loop instead of dereferencing NULL records. */
                records = NULL;
                rec_count = 0;
                break;
            }
            records = t;
        }
        BulkInsRecord *r = &records[rec_count++];
        r->id = id;
        r->payload = payload;
        r->klen = klen;
        compute_addr(id, klen, sc.splits, r->hash, &r->shard_id, &r->start_slot);
    }

    /* ===== Phase 1.5: bucket by shard — identical to the JSON path.
       OOM at any of the three allocs frees every prior allocation in
       reverse order before bailing. */
    int *shard_counts = calloc(sc.splits, sizeof(int));
    if (!shard_counts) {
        free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        if (data_mmaped) munmap((void *)data, st.st_size); else free((void *)data);
        OUT("{\"error\":\"oom: bulk_insert shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkInsShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkInsShardWork)) : NULL;
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    if ((nshard_groups > 0 && !workers) || !shard_to_worker) {
        free(workers); free(shard_to_worker);
        free(shard_counts); free(records);
        arena_free(arena);
        for (int i = 0; i < nidx; i++) free(idx_pairs[i]);
        free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);
        if (data_mmaped) munmap((void *)data, st.st_size); else free((void *)data);
        OUT("{\"error\":\"oom: bulk_insert workers\"}\n");
        return 1;
    }
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
    /* Same invariant as cmd_bulk_insert (JSON path): rec_count > 0 ⇒
       nshard_groups > 0 ⇒ workers non-NULL post-OOM-guard. */
    for (size_t i = 0; i < rec_count; i++) {
        int w = shard_to_worker[records[i].shard_id];
        /* coverity[forward_null] workers non-NULL when rec_count > 0 */
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
        ws->skipped = 0;
        ws->if_not_exists = if_not_exists;
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
    int delim_skipped_total = 0;
    for (int wi = 0; wi < nshard_groups; wi++) {
        BulkInsShardWork *ws = &workers[wi];
        count  += ws->inserted;
        errors += ws->errors;
        delim_skipped_total += ws->skipped;

        for (int fi = 0; fi < nidx; fi++) {
            size_t add = ws->idx_pair_counts[fi];
            if (add == 0) { free(ws->idx_pairs[fi]); continue; }
            if (idx_pair_counts[fi] + add > idx_pair_caps[fi]) {
                size_t new_cap = idx_pair_caps[fi];
                while (idx_pair_counts[fi] + add > new_cap) new_cap *= 2;
                BtEntry *t = xrealloc_or_free(idx_pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    log_msg(1, "INDEX_OOM merge field_idx=%d (dropped %zu pairs; rerun reindex)",
                            fi, idx_pair_counts[fi] + add);
                    idx_pairs[fi] = NULL;
                    idx_pair_counts[fi] = 0;
                    idx_pair_caps[fi] = 0;
                    for (size_t k = 0; k < add; k++) free((void *)ws->idx_pairs[fi][k].value);
                    free(ws->idx_pairs[fi]);
                    continue;
                }
                idx_pairs[fi] = t;
                idx_pair_caps[fi] = new_cap;
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

    /* Parallel index builds — one worker per field; the worker streams the
       per-shard merges sequentially. See cmd_bulk_insert (JSON path) above
       for why we no longer fan out per (field, shard). */
    if (nidx > 0) {
        IdxFieldArg *fa = malloc((size_t)nidx * sizeof(IdxFieldArg));
        int fa_count = 0;
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_pair_counts[fi] == 0) continue;
            fa[fa_count++] = (IdxFieldArg){
                .db_root = db_root, .object = object, .field = idx_fields[fi],
                .splits = sc.splits,
                .new_entries = idx_pairs[fi], .new_count = idx_pair_counts[fi],
            };
        }
        parallel_for(idx_build_field_worker, fa, fa_count, sizeof(IdxFieldArg));
        free(fa);
        for (int fi = 0; fi < nidx; fi++) {
            for (size_t ei = 0; ei < idx_pair_counts[fi]; ei++)
                free((char *)idx_pairs[fi][ei].value);
            free(idx_pairs[fi]);
        }
    }
    free(idx_pairs); free(idx_pair_counts); free(idx_pair_caps);

    if (count > 0) update_count(db_root, object, count);
    if (errors) {
        OUT("{\"inserted\":%d,\"skipped\":%d,\"errors\":%d,\"error\":\"some_records_dropped\"}\n",
            count, delim_skipped_total, errors);
        fprintf(stderr, "%d errors during delimited import (see info log for dropped keys)\n", errors);
    } else if (delim_skipped_total > 0) {
        OUT("{\"inserted\":%d,\"skipped\":%d}\n", count, delim_skipped_total);
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
    /* Collected index deletions: [nidx][key_count] — parallel (val, vlen).
       val is malloc'd index-key bytes (or NULL). */
    uint8_t ***idx_vals;
    size_t  **idx_lens;
} BulkDelShardWork;

static void *bulk_del_shard_worker(void *arg) {
    BulkDelShardWork *sw = (BulkDelShardWork *)arg;
    if (sw->key_count == 0) return NULL;

    /* shard_slots stores start_slot per key; for the per-shard file we
       need shard_id derived from the first key's hash. */
    int shard_id, dummy;
    addr_from_hash(sw->hashes[0], sw->sch->splits, &shard_id, &dummy);

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), sw->db_root, sw->object, shard_id);

    /* Use the persistent ucache write path: per-entry rwlock serializes
       writers but readers stay live, and the mmap is shared with the
       reader pool. The previous open + flock + mmap MAP_SHARED + munmap
       per worker dropped + reopened the kernel mapping every time, so
       bulk-delete paid the page-fault hit on each call AND held a
       cross-process flock that blocked concurrent readers using the same
       file via ucache. */
    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) return NULL;
    uint8_t *map = wh.map;
    ShardHeader *sh = (ShardHeader *)map;
    if (sh->magic != SHARD_MAGIC) { ucache_write_release(wh); return NULL; }
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

                /* Extract index values (as index-key bytes) before tombstoning */
                if (sw->nidx > 0 && sw->ts) {
                    const uint8_t *raw = map + zoneB_off(s, slots, sw->sch->slot_size) + h->key_len;
                    for (int fi = 0; fi < sw->nidx; fi++) {
                        uint8_t *buf = NULL; size_t blen = 0;
                        if (build_index_key_from_record(sw->ts, raw, sw->idx_fields[fi], &buf, &blen)) {
                            sw->idx_vals[fi][ki] = buf;
                            sw->idx_lens[fi][ki] = blen;
                        } else {
                            sw->idx_vals[fi][ki] = NULL;
                            sw->idx_lens[fi][ki] = 0;
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
    ucache_write_release(wh);
    return NULL;
}

/* Per-index bulk delete worker — deletes all collected values from one index */
typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    int splits;
    uint8_t **vals;
    size_t   *vlens;
    uint8_t (*hashes)[16];
    int count;
} BulkDelIdxWork;

static void *bulk_del_idx_worker(void *arg) {
    BulkDelIdxWork *iw = (BulkDelIdxWork *)arg;
    for (int i = 0; i < iw->count; i++) {
        if (iw->vals[i] && iw->vlens[i] > 0)
            delete_index_entry(iw->db_root, iw->object, iw->field, iw->splits,
                               iw->vals[i], iw->vlens[i], iw->hashes[i]);
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
            if (pos >= cap - 1) {
                cap *= 2;
                char *t = xrealloc_or_free(raw, cap);
                if (!t) { raw = NULL; break; }
                raw = t;
            }
            raw[pos++] = c;
        }
        if (!raw) { fprintf(stderr, "Error: out of memory reading stdin\n"); return 1; }
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
                if (key_count >= key_cap) {
                    key_cap *= 2;
                    /* Plain realloc + walk: keys[] holds heap-malloc'd entries. */
                    char **t = realloc(keys, key_cap * sizeof(char *));
                    if (!t) {
                        for (int k = 0; k < key_count; k++) free(keys[k]);
                        free(keys);
                        keys = NULL;
                        key_count = 0;
                        break;
                    }
                    keys = t;
                }
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
                if (key_count >= key_cap) {
                    key_cap *= 2;
                    /* Plain realloc + walk: keys[] holds heap-malloc'd entries. */
                    char **t = realloc(keys, key_cap * sizeof(char *));
                    if (!t) {
                        for (int k = 0; k < key_count; k++) free(keys[k]);
                        free(keys);
                        keys = NULL;
                        key_count = 0;
                        break;
                    }
                    keys = t;
                }
                keys[key_count++] = strdup(line);
            }
            line = strtok_r(NULL, "\n", &_line_save);
        }
    }

    if (key_count == 0) { free(keys); free(raw); return 0; }

    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
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
        /* Allocate idx_vals + idx_lens (parallel arrays) */
        workers[g].idx_vals = calloc(nidx, sizeof(uint8_t **));
        workers[g].idx_lens = calloc(nidx, sizeof(size_t *));
        for (int fi = 0; fi < nidx; fi++) {
            workers[g].idx_vals[fi] = calloc(cnt, sizeof(uint8_t *));
            workers[g].idx_lens[fi] = calloc(cnt, sizeof(size_t));
        }

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
        /* Flatten idx_vals+idx_lens across all shard groups: per index,
           collect (val, vlen, hash) triples. */
        BulkDelIdxWork *idx_workers = malloc(nidx * sizeof(BulkDelIdxWork));
        for (int fi = 0; fi < nidx; fi++) {
            idx_workers[fi].db_root = db_root;
            idx_workers[fi].object = object;
            idx_workers[fi].field = idx_fields[fi];
            idx_workers[fi].splits = sch.splits;
            idx_workers[fi].vals = malloc(key_count * sizeof(uint8_t *));
            idx_workers[fi].vlens = malloc(key_count * sizeof(size_t));
            idx_workers[fi].hashes = malloc(key_count * sizeof(uint8_t[16]));
            idx_workers[fi].count = 0;
            for (int g = 0; g < nshard_groups; g++) {
                for (int ki = 0; ki < workers[g].key_count; ki++) {
                    if (workers[g].idx_vals[fi][ki] && workers[g].idx_lens[fi][ki] > 0) {
                        int c = idx_workers[fi].count;
                        idx_workers[fi].vals[c] = workers[g].idx_vals[fi][ki];
                        idx_workers[fi].vlens[c] = workers[g].idx_lens[fi][ki];
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
            free(idx_workers[fi].vlens);
            free(idx_workers[fi].hashes);
        }
        free(idx_workers);
    }

    /* Cleanup workers */
    for (int g = 0; g < nshard_groups; g++) {
        free(workers[g].keys);
        free(workers[g].hashes);
        free(workers[g].shard_slots);
        for (int fi = 0; fi < nidx; fi++) {
            free(workers[g].idx_vals[fi]);
            free(workers[g].idx_lens[fi]);
        }
        free(workers[g].idx_vals);
        free(workers[g].idx_lens);
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
    _Atomic int count;
    int cap;
    int limit;
    QueryDeadline *deadline;
    int dl_counter;
    size_t buffer_bytes;    /* running total for QUERY_BUFFER_MB cap */
    /* `count` and `budget_exceeded` are read lock-free in the per-record
       callback as fast-skip checks (count >= limit / budget_exceeded != 0).
       bc->lock only serializes the keys-array append, not the per-record
       match. _Atomic gives the lock-free reader a torn-read-free view
       against the writers under bc->lock. */
    _Atomic int budget_exceeded;
    pthread_mutex_t lock;   /* serializes only the keys-array append,
                               not the per-record match */
} BulkCriteriaCtx;

static int bulk_criteria_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    BulkCriteriaCtx *bc = (BulkCriteriaCtx *)ctx;
    /* coverity[lock_evasion] coverity[missing_lock] intentional fast-skip in
       the per-record hot path — `_Atomic int` gives torn-read-free visibility
       against the writers under bc->lock; staleness here just costs one
       extra iteration before the locked re-check at line 2232 catches the
       budget overflow. Taking the mutex per record would gate every callback
       through a kernel call. */
    if (bc->budget_exceeded) return 1;
    /* coverity[lock_evasion] coverity[missing_lock] same rationale — count is _Atomic. */
    if (bc->limit > 0 && bc->count >= bc->limit) return 1;
    if (query_deadline_tick(bc->deadline, &bc->dl_counter)) return 1;

    const uint8_t *raw = block + hdr->key_len;

    if (!criteria_match_tree(raw, bc->tree, bc->fs)) return 0;

    /* Match — grow array and append under internal mutex. criteria_match_tree
       above runs lock-free; only the shared-state mutation is serialized. */
    size_t key_bytes = sizeof(char *) + hdr->key_len + 1;
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    pthread_mutex_lock(&bc->lock);
    if (bc->budget_exceeded || (bc->limit > 0 && bc->count >= bc->limit)) {
        pthread_mutex_unlock(&bc->lock);
        free(key);
        return 1;
    }
    if (bc->buffer_bytes + key_bytes > g_query_buffer_max_bytes) {
        bc->budget_exceeded = 1;
        pthread_mutex_unlock(&bc->lock);
        free(key);
        return 1;
    }
    bc->buffer_bytes += key_bytes;
    if (bc->count >= bc->cap) {
        int new_cap = bc->cap ? bc->cap * 2 : 1024;
        char **t = xrealloc_or_free(bc->keys, (size_t)new_cap * sizeof(char *));
        if (!t) {
            bc->keys = NULL;
            bc->count = 0;
            bc->cap = 0;
            bc->budget_exceeded = 1;
            pthread_mutex_unlock(&bc->lock);
            free(key);
            return 1;
        }
        bc->keys = t;
        bc->cap = new_cap;
    }
    bc->keys[bc->count++] = key;
    pthread_mutex_unlock(&bc->lock);
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
    /* CAS: optional `if` condition re-verified per record under the wrlock.
       NULL/empty = no CAS check. Same SearchCriterion[] shape single-op
       cmd_update uses, which makes cas_check directly applicable. */
    SearchCriterion *cas_crit;
    int              cas_ncrit;
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

        /* Per-record CAS — re-verify the `if` condition under the wrlock.
           Race window between phase-1 scan and phase-2 write is closed
           here; failures count as skipped, not errors. */
        if (w->cas_crit && w->cas_ncrit > 0 &&
            !cas_check(w->ts, value_ptr, w->cas_crit, w->cas_ncrit)) {
            w->skipped++; continue;
        }

        /* Collect old index values (as index-key bytes) */
        uint8_t *old_idx_bufs[MAX_FIELDS];
        size_t   old_idx_lens[MAX_FIELDS];
        int      old_idx_have[MAX_FIELDS];
        memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
        memset(old_idx_lens, 0, sizeof(old_idx_lens));
        memset(old_idx_have, 0, sizeof(old_idx_have));
        for (int fi = 0; fi < w->nidx; fi++) {
            old_idx_have[fi] = build_index_key_from_record(w->ts, value_ptr, w->idx_fields[fi],
                                                          &old_idx_bufs[fi], &old_idx_lens[fi]);
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
                uint8_t *new_buf = NULL; size_t new_len = 0;
                int have_new = build_index_key_from_record(w->ts, new_val, w->idx_fields[fi],
                                                          &new_buf, &new_len);
                int changed = 0;
                if (have_new && !old_idx_have[fi]) changed = 1;
                else if (!have_new && old_idx_have[fi]) changed = 1;
                else if (have_new && old_idx_have[fi]) {
                    if (new_len != old_idx_lens[fi] ||
                        memcmp(new_buf, old_idx_bufs[fi], new_len) != 0) changed = 1;
                }
                if (changed) {
                    if (old_idx_have[fi])
                        delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                           w->sch->splits,
                                           old_idx_bufs[fi], old_idx_lens[fi], rec->hash);
                    if (have_new)
                        write_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                          w->sch->splits,
                                          new_buf, new_len, rec->hash);
                }
                free(old_idx_bufs[fi]);
                free(new_buf);
            }
        }

        w->updated++;
    }

    ucache_write_release(wh);
    return NULL;
}

int cmd_bulk_update(const char *db_root, const char *object,
                    const char *criteria_json, const char *value_json,
                    const char *if_json, int limit, int dry_run) {
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

    /* Parse optional `if` once into the SearchCriterion[] shape that
       cas_check expects. Workers share the parsed array read-only. */
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0]) {
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit);
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    scan_shards(data_dir, sch.slot_size, bulk_criteria_scan_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"updated\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
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
        workers[wi].cas_crit = cas_crit;
        workers[wi].cas_ncrit = cas_ncrit;
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

    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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

        /* Capture old index values (as index-key bytes) before patching. */
        uint8_t *old_idx_bufs[MAX_FIELDS];
        size_t   old_idx_lens[MAX_FIELDS];
        int      old_idx_have[MAX_FIELDS];
        memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
        memset(old_idx_lens, 0, sizeof(old_idx_lens));
        memset(old_idx_have, 0, sizeof(old_idx_have));
        for (int fi = 0; fi < w->nidx; fi++) {
            old_idx_have[fi] = build_index_key_from_record(w->ts, value_ptr, w->idx_fields[fi],
                                                          &old_idx_bufs[fi], &old_idx_lens[fi]);
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
            uint8_t *new_buf = NULL; size_t new_len = 0;
            int have_new = build_index_key_from_record(w->ts, value_ptr, w->idx_fields[fi],
                                                      &new_buf, &new_len);
            int changed = 0;
            if (have_new && !old_idx_have[fi]) changed = 1;
            else if (!have_new && old_idx_have[fi]) changed = 1;
            else if (have_new && old_idx_have[fi]) {
                if (new_len != old_idx_lens[fi] ||
                    memcmp(new_buf, old_idx_bufs[fi], new_len) != 0) changed = 1;
            }
            if (changed) {
                if (old_idx_have[fi])
                    delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                       w->sch->splits,
                                       old_idx_bufs[fi], old_idx_lens[fi], rec->hash);
                if (have_new)
                    write_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                      w->sch->splits,
                                      new_buf, new_len, rec->hash);
            }
            free(old_idx_bufs[fi]);
            free(new_buf);
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
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */

    int ifd = open(filepath, O_RDONLY);
    if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
    struct stat st;
    if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
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
            /* Plain realloc + nested cleanup (same pattern as the bulk-update-json
               OOM fix): per-record `key` is heap-malloc'd, so xrealloc_or_free's
               atomic-free leaves no chance to walk records[] for cleanup. */
            BulkUpdDelimRec *t = realloc(records, rec_cap * sizeof(*t));
            if (!t) {
                for (size_t k = 0; k < rec_count; k++) free(records[k].key);
                free(records);
                records = NULL;
                rec_count = 0;
                break;
            }
            records = t;
        }
        BulkUpdDelimRec *r = &records[rec_count++];
        r->key = malloc(klen + 1);
        memcpy(r->key, line_start, klen); r->key[klen] = '\0';
        r->klen = klen;
        r->body_start = key_end + 1;
        r->line_end   = line_end;
        compute_addr(r->key, klen, sch.splits, r->hash, &r->shard_id, &r->start_slot);
    }

    /* ===== Phase 1.5: bucket by shard_id.
       OOM bails free the mmap/buf-backed `data` along with records[]. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        for (size_t i = 0; i < rec_count; i++) free(records[i].key);
        free(records);
        if (data_mmaped) munmap((void *)data, st.st_size); else free((void *)data);
        OUT("{\"error\":\"oom: bulk_update_delim shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;
    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdDelimShardWork *workers = nshard_groups > 0
        ? calloc(nshard_groups, sizeof(BulkUpdDelimShardWork)) : NULL;
    int *shard_to_worker = malloc(sch.splits * sizeof(int));
    if ((nshard_groups > 0 && !workers) || !shard_to_worker) {
        free(workers); free(shard_to_worker); free(shard_counts);
        for (size_t i = 0; i < rec_count; i++) free(records[i].key);
        free(records);
        if (data_mmaped) munmap((void *)data, st.st_size); else free((void *)data);
        OUT("{\"error\":\"oom: bulk_update_delim workers\"}\n");
        return 1;
    }
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

/* ===== bulk-update JSON form =====
   Shape: [{"id":"k","data":{...}}, ...]
   Semantics: update-only, key must exist; only fields present in `data`
   are overwritten, fields absent from `data` keep their existing value.
   Same shard-grouped parallel pattern as bulk-update-delimited; the worker
   patches each touched field in place at its known offset and applies the
   drop-old/insert-new index delta only where the indexed value changed. */

typedef struct {
    char        *key;            /* heap-owned, null-terminated */
    size_t       klen;
    uint8_t      hash[16];
    int          start_slot;
    int          shard_id;
    /* Field deltas: aligned arrays of (typed-field index, owned-string value).
       Only fields present in `data` populate these — missing fields are
       left alone in the worker. */
    int          n_fields;
    int         *field_indices;  /* heap-owned, length n_fields */
    char       **field_values;   /* heap-owned strings, length n_fields */
} BulkUpdJsonRec;

typedef struct {
    const char       *db_root;
    const char       *object;
    const Schema     *sch;
    TypedSchema      *ts;
    const char      (*idx_fields)[256];
    int               nidx;
    int               shard_id;
    BulkUpdJsonRec   *recs;
    int               count;
    /* Results */
    int               updated;
    int               skipped;
} BulkUpdJsonShardWork;

static void *bulk_upd_json_shard_worker(void *arg) {
    BulkUpdJsonShardWork *w = (BulkUpdJsonShardWork *)arg;
    if (w->count == 0) return NULL;

    char shard[PATH_MAX];
    build_shard_path(shard, sizeof(shard), w->db_root, w->object, w->shard_id);
    FcacheRead wh = ucache_get_write(shard, 0, 0);
    if (!wh.map) { w->skipped += w->count; return NULL; }
    uint8_t *map = wh.map;
    uint32_t slots = wh.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ki = 0; ki < w->count; ki++) {
        BulkUpdJsonRec *rec = &w->recs[ki];

        /* Probe — must already exist (update-only semantics) */
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

        /* Capture old index keys before patching. */
        uint8_t *old_idx_bufs[MAX_FIELDS];
        size_t   old_idx_lens[MAX_FIELDS];
        int      old_idx_have[MAX_FIELDS];
        memset(old_idx_bufs, 0, sizeof(old_idx_bufs));
        memset(old_idx_lens, 0, sizeof(old_idx_lens));
        memset(old_idx_have, 0, sizeof(old_idx_have));
        for (int fi = 0; fi < w->nidx; fi++) {
            old_idx_have[fi] = build_index_key_from_record(w->ts, value_ptr, w->idx_fields[fi],
                                                          &old_idx_bufs[fi], &old_idx_lens[fi]);
        }

        /* Patch each touched field at its fixed offset. Tombstoned fields
           are silently skipped — the touched-list was filtered in phase 1. */
        for (int i = 0; i < rec->n_fields; i++) {
            int tidx = rec->field_indices[i];
            if (tidx < 0 || tidx >= w->ts->nfields) continue;
            if (w->ts->fields[tidx].removed) continue;
            encode_field(&w->ts->fields[tidx], rec->field_values[i],
                         value_ptr + w->ts->fields[tidx].offset);
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

        /* Drop-old / insert-new btree entries where the indexed value moved. */
        for (int fi = 0; fi < w->nidx; fi++) {
            uint8_t *new_buf = NULL; size_t new_len = 0;
            int have_new = build_index_key_from_record(w->ts, value_ptr, w->idx_fields[fi],
                                                      &new_buf, &new_len);
            int changed = 0;
            if (have_new && !old_idx_have[fi]) changed = 1;
            else if (!have_new && old_idx_have[fi]) changed = 1;
            else if (have_new && old_idx_have[fi]) {
                if (new_len != old_idx_lens[fi] ||
                    memcmp(new_buf, old_idx_bufs[fi], new_len) != 0) changed = 1;
            }
            if (changed) {
                if (old_idx_have[fi])
                    delete_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                       w->sch->splits,
                                       old_idx_bufs[fi], old_idx_lens[fi], rec->hash);
                if (have_new)
                    write_index_entry(w->db_root, w->object, w->idx_fields[fi],
                                      w->sch->splits,
                                      new_buf, new_len, rec->hash);
            }
            if (old_idx_have[fi]) free(old_idx_bufs[fi]);
            free(new_buf);
        }

        w->updated++;
    }

    ucache_write_release(wh);
    return NULL;
}

/* Internal helper: read input (file path or in-memory string) into a heap buffer.
   `input_is_file` is 1 for file path, 0 for in-memory string passed verbatim. */
static int bulk_upd_json_run(const char *db_root, const char *object,
                              const char *input, int input_is_file) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"bulk-update-json requires typed fields (fields.conf)\"}\n");
        return 1;
    }
    Schema sch = load_schema(db_root, object);
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */

    char *json = NULL;
    size_t len = 0;
    int json_mmaped = 0;
    int ifd = -1;
    if (input_is_file) {
        ifd = open(input, O_RDONLY);
        if (ifd < 0) { OUT("{\"error\":\"Cannot open file\"}\n"); return 1; }
        struct stat st;
        if (fstat(ifd, &st) < 0) { close(ifd); OUT("{\"error\":\"Cannot stat file\"}\n"); return 1; }
        if (st.st_size == 0) { close(ifd); OUT("{\"error\":\"Empty file\"}\n"); return 1; }
        len = st.st_size;
        json = mmap(NULL, len, PROT_READ, MAP_SHARED, ifd, 0);
        if (json == MAP_FAILED) {
            json = malloc(len + 1);
            lseek(ifd, 0, SEEK_SET);
            size_t rd = 0;
            while (rd < len) {
                ssize_t n = read(ifd, json + rd, len - rd);
                if (n <= 0) break;
                rd += n;
            }
            json[rd] = '\0';
        } else {
            madvise((void *)json, len, MADV_SEQUENTIAL);
            json_mmaped = 1;
        }
        close(ifd);
    } else {
        json = (char *)input;
        len = strlen(input);
    }

    /* Phase 1: parse the array, extract per-record (key, touched fields, hash, shard). */
    BulkUpdJsonRec *records = NULL;
    size_t rec_cap = 1024, rec_count = 0;
    records = malloc(rec_cap * sizeof(BulkUpdJsonRec));

    int matched = 0, skipped = 0;

    const char *p = json_skip(json);
    int is_object_format = (*p == '{'); /* {"k1":{...},"k2":{...}}    — round-trips with get-multi */
    int is_array_format  = (*p == '[');  /* [{"id":"k1","data":{...}},...] */
    if (!is_object_format && !is_array_format) {
        OUT("{\"error\":\"bulk-update JSON must be a top-level object or array\"}\n");
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 1;
    }
    p++;

    /* Pre-name the typed fields once so we can reuse the names array per
       record without rebuilding it. */
    const char *field_names[MAX_FIELDS];
    for (int i = 0; i < ts->nfields; i++) field_names[i] = ts->fields[i].name;

    while (*p) {
        p = json_skip(p);
        if (*p == ']' || *p == '}') break;
        if (*p == ',') { p++; continue; }

        char *key = NULL; size_t klen = 0;
        const char *data_str = NULL;
        const char *obj_end = NULL;
        char obj_buf[8192];
        char *obj_str = NULL;
        int obj_heap = 0;

        if (is_object_format) {
            /* "key": {...} */
            if (*p != '"') { p++; continue; }
            p++;
            const char *key_start = p;
            while (*p && *p != '"') p++;
            klen = p - key_start;
            key = malloc(klen + 1);
            memcpy(key, key_start, klen);
            key[klen] = '\0';
            if (*p == '"') p++;
            p = json_skip(p);
            if (*p == ':') p = json_skip(p + 1);

            /* Data span points into the original mmap; json_get_fields scans
               by brace-count so the trailing comma/} doesn't matter. */
            data_str = p;
            obj_end = json_skip_value(p);
        } else {
            if (*p != '{') { p++; continue; }
            const char *obj_start = p;
            obj_end = json_skip_value(p);
            size_t obj_len = obj_end - obj_start;

            if (obj_len < sizeof(obj_buf)) {
                memcpy(obj_buf, obj_start, obj_len);
                obj_buf[obj_len] = '\0';
                obj_str = obj_buf;
            } else {
                obj_str = malloc(obj_len + 1);
                memcpy(obj_str, obj_start, obj_len);
                obj_str[obj_len] = '\0';
                obj_heap = 1;
            }

            JsonObj rec;
            json_parse_object(obj_str, obj_len, &rec);

            const char *iv; size_t ivl;
            if (json_obj_unquoted(&rec, "id", &iv, &ivl)) {
                key = malloc(ivl + 1);
                memcpy(key, iv, ivl);
                key[ivl] = '\0';
                klen = ivl;
            }

            const char *dv; size_t dl;
            if (json_obj_get(&rec, "data", &dv, &dl)) {
                data_str = dv;
                (void)dl;
            }
        }

        if (!key || !data_str) {
            skipped++;
            free(key);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }
        if ((int)klen > sch.max_key) {
            skipped++;
            free(key);
            if (obj_heap) free(obj_str);
            p = obj_end;
            continue;
        }

        /* Pull out every typed-field name from `data`. Fields not present in
           `data` come back NULL → not touched. */
        char *vals_buf[MAX_FIELDS];
        json_get_fields(data_str, field_names, ts->nfields, vals_buf);

        int n_touched = 0;
        for (int i = 0; i < ts->nfields; i++) if (vals_buf[i]) n_touched++;

        BulkUpdJsonRec *r;
        if (rec_count >= rec_cap) {
            rec_cap *= 2;
            /* Plain realloc (not xrealloc_or_free) so we can walk records[]
               for nested free() before releasing the array. xrealloc_or_free
               frees atomically, leaving no window to clean up the per-record
               heap-owned key / field_indices / field_values mallocs. */
            BulkUpdJsonRec *t = realloc(records, rec_cap * sizeof(*t));
            if (!t) {
                /* OOM: free per-record nested allocations, then the array,
                   then the current iteration's locals (whose ownership
                   hadn't transferred to records[rec_count] yet). Reset
                   rec_count so the downstream phase-2 loop sees an empty
                   set and the rec_count==0 branch handles the response. */
                for (size_t k = 0; k < rec_count; k++) {
                    free(records[k].key);
                    for (int j = 0; j < records[k].n_fields; j++)
                        free(records[k].field_values[j]);
                    free(records[k].field_values);
                    free(records[k].field_indices);
                }
                free(records);
                records = NULL;
                rec_count = 0;
                free(key);
                for (int i = 0; i < ts->nfields; i++) free(vals_buf[i]);
                if (obj_heap) free(obj_str);
                break;
            }
            records = t;
        }
        r = &records[rec_count++];
        r->key = key;
        r->klen = klen;
        compute_addr(key, klen, sch.splits, r->hash, &r->shard_id, &r->start_slot);
        if (n_touched > 0) {
            r->n_fields = n_touched;
            r->field_indices = malloc(n_touched * sizeof(int));
            r->field_values = malloc(n_touched * sizeof(char *));
            int j = 0;
            for (int i = 0; i < ts->nfields; i++) {
                if (vals_buf[i]) {
                    r->field_indices[j] = i;
                    r->field_values[j] = vals_buf[i];   /* take ownership */
                    j++;
                }
            }
        } else {
            r->n_fields = 0;
            r->field_indices = NULL;
            r->field_values = NULL;
        }
        matched++;

        if (obj_heap) free(obj_str);
        p = obj_end;
    }

    if (rec_count == 0) {
        OUT("{\"matched\":0,\"updated\":0,\"skipped\":%d}\n", skipped);
        if (json_mmaped) munmap((void *)json, len);
        else if (input_is_file) free(json);
        free(records);
        return 0;
    }

    /* Bucket per shard — same pattern as bulk-update-delimited / bulk-delete. */
    int *shard_counts = calloc(sch.splits, sizeof(int));
    if (!shard_counts) {
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
        }
        free(records);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: shard_counts\"}\n");
        return 1;
    }
    for (size_t i = 0; i < rec_count; i++) shard_counts[records[i].shard_id]++;

    int nshard_groups = 0;
    for (int s = 0; s < sch.splits; s++) if (shard_counts[s] > 0) nshard_groups++;

    BulkUpdJsonShardWork *workers = calloc(nshard_groups, sizeof(BulkUpdJsonShardWork));
    if (nshard_groups > 0 && !workers) {
        free(shard_counts);
        for (size_t i = 0; i < rec_count; i++) {
            free(records[i].key);
            for (int j = 0; j < records[i].n_fields; j++) free(records[i].field_values[j]);
            free(records[i].field_values);
            free(records[i].field_indices);
        }
        free(records);
        if (json_mmaped) munmap((void *)json, len); else if (input_is_file) free(json);
        OUT("{\"error\":\"oom: workers\"}\n");
        return 1;
    }
    int wi = 0;
    for (int s = 0; s < sch.splits; s++) {
        if (shard_counts[s] > 0) {
            workers[wi].db_root = db_root;
            workers[wi].object = object;
            workers[wi].sch = &sch;
            workers[wi].ts = ts;
            workers[wi].idx_fields = idx_fields;
            workers[wi].nidx = nidx;
            workers[wi].shard_id = s;
            workers[wi].recs = malloc(shard_counts[s] * sizeof(BulkUpdJsonRec));
            workers[wi].count = 0;
            workers[wi].updated = 0;
            workers[wi].skipped = 0;
            wi++;
        }
    }

    for (size_t i = 0; i < rec_count; i++) {
        for (int gi = 0; gi < nshard_groups; gi++) {
            if (workers[gi].shard_id == records[i].shard_id) {
                workers[gi].recs[workers[gi].count++] = records[i];
                break;
            }
        }
        /* Null the heap-owned pointers in records[i] unconditionally — the
           inner loop above ALWAYS finds a matching worker by construction
           (every records[i].shard_id was used to compute shard_counts, and
           every shard with count > 0 got a worker), but Coverity can't
           trace the invariant. NULLing outside the if covers the
           unreachable no-match case so free(records) below is leak-free
           in the static analyzer's view too. */
        records[i].key = NULL;
        records[i].field_values = NULL;
        records[i].field_indices = NULL;
    }
    free(shard_counts);

    /* Phase 2: parallel shard workers. */
    parallel_for(bulk_upd_json_shard_worker, workers, nshard_groups,
                 sizeof(BulkUpdJsonShardWork));

    int updated = 0;
    for (int gi = 0; gi < nshard_groups; gi++) {
        updated += workers[gi].updated;
        skipped += workers[gi].skipped;
        for (int i = 0; i < workers[gi].count; i++) {
            BulkUpdJsonRec *r = &workers[gi].recs[i];
            free(r->key);
            for (int j = 0; j < r->n_fields; j++) free(r->field_values[j]);
            free(r->field_values);
            free(r->field_indices);
        }
        free(workers[gi].recs);
    }
    free(workers);
    free(records);

    if (json_mmaped) munmap((void *)json, len);
    else if (input_is_file) free(json);

    log_msg(3, "BULK-UPDATE-JSON %s matched=%d updated=%d skipped=%d",
            object, matched, updated, skipped);
    OUT("{\"matched\":%d,\"updated\":%d,\"skipped\":%d}\n", matched, updated, skipped);
    return 0;
}

int cmd_bulk_update_json(const char *db_root, const char *object, const char *input) {
    return bulk_upd_json_run(db_root, object, input, 1);
}

int cmd_bulk_update_json_string(const char *db_root, const char *object, char *json_str) {
    return bulk_upd_json_run(db_root, object, json_str, 0);
}

int cmd_bulk_delete_criteria(const char *db_root, const char *object,
                             const char *criteria_json, const char *if_json,
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

    /* Optional `if` for per-record CAS, re-verified under wrlock in phase 2. */
    SearchCriterion *cas_crit = NULL;
    int cas_ncrit = 0;
    if (if_json && if_json[0]) {
        parse_criteria_json(if_json, &cas_crit, &cas_ncrit);
    }

    /* Phase 1: Scan — collect matching keys (read-only) */
    FieldSchema fs;
    init_field_schema(&fs, db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    compile_criteria_tree(tree, fs.ts);
    QueryDeadline dl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
    BulkCriteriaCtx ctx = { tree, &fs, NULL, 0, 0, limit, &dl, 0, 0, 0,
                            PTHREAD_MUTEX_INITIALIZER };
    scan_shards(data_dir, sch.slot_size, bulk_criteria_scan_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
    int matched = ctx.count;

    if (dl.timed_out) {
        OUT("{\"error\":\"query_timeout\"}\n");
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }
    if (ctx.budget_exceeded) {
        OUT(QUERY_BUFFER_ERR);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
        for (int i = 0; i < matched; i++) free(ctx.keys[i]);
        free(ctx.keys); free_criteria_tree(tree);
        return -1;
    }

    if (dry_run) {
        OUT("{\"matched\":%d,\"deleted\":0,\"skipped\":0,\"dry_run\":true}\n", matched);
        if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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

        /* Per-record CAS — same optimistic-concurrency check single-op
           cmd_delete uses. Failures count as skipped, not errors. */
        if (cas_crit && cas_ncrit > 0 &&
            !cas_check(ts, value_ptr, cas_crit, cas_ncrit)) {
            ucache_write_release(wh); skipped++; continue;
        }

        /* Extract indexed field values (as index-key bytes) BEFORE tombstoning */
        char idx_fields[MAX_FIELDS][256];
        int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
        for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';  /* re-term for static analyzer; see storage.c:991 comment */
        uint8_t *idx_bufs[MAX_FIELDS];
        size_t   idx_lens[MAX_FIELDS];
        int      idx_have[MAX_FIELDS];
        memset(idx_bufs, 0, sizeof(idx_bufs));
        memset(idx_lens, 0, sizeof(idx_lens));
        memset(idx_have, 0, sizeof(idx_have));
        if (nidx > 0) {
            for (int fi = 0; fi < nidx; fi++) {
                idx_have[fi] = build_index_key_from_record(ts, value_ptr, idx_fields[fi],
                                                          &idx_bufs[fi], &idx_lens[fi]);
            }
        }

        /* Tombstone */
        h->flag = 2;
        ucache_bump_record_count(wh.slot, -1);
        ucache_write_release(wh);

        /* Index cleanup */
        for (int fi = 0; fi < nidx; fi++) {
            if (idx_have[fi])
                delete_index_entry(db_root, object, idx_fields[fi], sch.splits,
                                   idx_bufs[fi], idx_lens[fi], hash);
            free(idx_bufs[fi]);
        }

        deleted++;
    }

    if (deleted > 0) {
        update_count(db_root, object, -deleted);
        update_deleted_count(db_root, object, deleted);
    }

    log_msg(3, "BULK-DELETE %s matched=%d deleted=%d skipped=%d", object, matched, deleted, skipped);
    OUT("{\"matched\":%d,\"deleted\":%d,\"skipped\":%d}\n", matched, deleted, skipped);

    if (cas_crit) free_criteria(cas_crit, cas_ncrit);
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
    if (!is_valid_splits(new_splits)) {
        OUT("{\"error\":\"splits=%d invalid; must be a power of 2 in {16, 32, 64, 128, 256, 512, 1024, 2048, 4096}\"}\n",
            new_splits);
        return 1;
    }
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

    /* Iterate current shards, copy live records into data.new/. Use the
       persistent ucache for the read side — vacuum holds objlock_wrlock on
       this object so no concurrent ops are racing, but the consistent
       cache path keeps memory footprint shared with whatever ucache
       entries already existed and avoids a second kernel mmap region per
       shard during the rebuild. */
    int live_count = 0;
    for (int olds = 0; olds < old_splits; olds++) {
        char old_path[PATH_MAX];
        build_shard_filename(old_path, sizeof(old_path), data_dir, olds);
        FcacheRead ofc = fcache_get_read(old_path);
        if (!ofc.map) continue;
        uint8_t *omap = ofc.map;
        ShardHeader *oshdr = (ShardHeader *)omap;
        if (oshdr->magic != SHARD_MAGIC || oshdr->slots_per_shard == 0) {
            fcache_release(ofc); continue;
        }
        uint32_t old_slots = oshdr->slots_per_shard;
        if (ofc.size < shard_zoneA_end(old_slots)) { fcache_release(ofc); continue; }
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
        fcache_release(ofc);
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
        /* Best-effort rollback — if this also fails the operator must
           manually restore data_old → data_dir; either way we're already
           returning an error so just log the secondary failure. */
        if (rename(data_old, data_dir) != 0)
            log_msg(1, "vacuum: rollback rename(%s → %s) failed: %s",
                    data_old, data_dir, strerror(errno));
        if (fields_changed) unlink(fpath_new);
        OUT("{\"error\":\"Failed to rename data.new → data\"}\n");
        return 1;
    }

    if (fields_changed) {
        if (rename(fpath, fpath_old) != 0)
            log_msg(1, "vacuum: rename(%s → %s) failed: %s",
                    fpath, fpath_old, strerror(errno));
        if (rename(fpath_new, fpath) != 0) {
            log_msg(1, "vacuum: rename(%s → %s) failed: %s — restoring old fields.conf",
                    fpath_new, fpath, strerror(errno));
            /* Best-effort restore of the previous fields.conf from .old. */
            (void)rename(fpath_old, fpath);
        }
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

    /* Per-shard idx layout uses index_splits = splits/4. Changing splits
       changes the idx-shard count, so the on-disk idx files no longer match
       the routing math: writes go to the new shard count, reads fan out
       across the new count, and any old high-numbered shard files are
       both unreachable AND poisonous (the old layout stored all entries
       across the wider hash range, so dropping them leaves stale rows
       indexed and missing rows unindexed). Rebuild every index from the
       data shards atomically with the splits change. Compact-only changes
       slot_size but not the hash routing, so idx layout stays valid. */
    int idx_rebuilt = 0;
    if (splits_changed) idx_rebuilt = reindex_object(db_root, object);

    log_msg(3, "REBUILD %s/%s: live=%d, splits=%d→%d, slot_size=%d→%d, compact=%d, idx_rebuilt=%d",
            db_root, object, live_count, old_splits, new_splits,
            old_sch.slot_size, new_sch.slot_size, drop_tombstoned, idx_rebuilt);
    OUT("{\"status\":\"rebuilt\",\"live\":%d,\"splits\":%d,\"slot_size\":%d,\"compact\":%s,\"indexes_rebuilt\":%d}\n",
        live_count, new_splits, new_sch.slot_size, drop_tombstoned ? "true" : "false",
        idx_rebuilt);
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
    if (!shards) { fprintf(stderr, "Error: oom\n"); return 1; }

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(shards); fprintf(stderr, "Error: No data directory for [%s]\n", object); return 1; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (shard_count >= shard_cap) {
            shard_cap *= 2;
            VacuumWork *t = xrealloc_or_free(shards, shard_cap * sizeof(*t));
            if (!t) {
                /* xrealloc_or_free already freed shards; reset count so the
                   parallel_for + cleaned-sum below don't dereference NULL. */
                shards = NULL;
                shard_count = 0;
                break;
            }
            shards = t;
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
    if (!fc.map) { OUT("false\n"); return 1; }
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
            OUT("true\n");
            return 0;
        }
    }
    fcache_release(fc);
    OUT("false\n");
    return 1;
}


/* ========== KEYS ========== */

typedef struct {
    int offset; int limit; int count; int printed;
    char csv_delim;   /* 0 = JSON mode; else CSV (delim-less, single column) */
    pthread_mutex_t lock;  /* serializes the emit section across parallel shards */
} KeysCtx;

int keys_cb(const SlotHeader *hdr, const uint8_t *block,
                    void *ctx) {
    KeysCtx *kc = (KeysCtx *)ctx;

    /* Copy the key bytes to a thread-local buffer outside the lock. */
    char kbuf[1024];
    size_t kl = hdr->key_len < sizeof(kbuf) - 1 ? hdr->key_len : sizeof(kbuf) - 1;
    memcpy(kbuf, block, kl); kbuf[kl] = '\0';

    pthread_mutex_lock(&kc->lock);
    if (kc->limit > 0 && kc->printed >= kc->limit) {
        pthread_mutex_unlock(&kc->lock);
        return 1;
    }
    kc->count++;
    if (kc->count <= kc->offset) {
        pthread_mutex_unlock(&kc->lock);
        return 0;
    }
    if (kc->csv_delim) {
        csv_emit_cell(kbuf, kc->csv_delim);
        OUT("\n");
    } else {
        OUT("%s\"%s\"", kc->printed ? "," : "", kbuf);
    }
    kc->printed++;
    pthread_mutex_unlock(&kc->lock);
    return 0;
}

int cmd_keys(const char *db_root, const char *object, int offset, int limit,
             const char *format, const char *delimiter) {
    if (limit <= 0) limit = g_global_limit;
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    Schema sch = load_schema(db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    KeysCtx ctx = { offset, limit, 0, 0, csv_delim, PTHREAD_MUTEX_INITIALIZER };
    if (csv_delim) OUT("key\n");  /* header */
    else OUT("[");
    scan_shards(data_dir, sch.slot_size, keys_cb, &ctx);
    pthread_mutex_destroy(&ctx.lock);
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

/* Emit a record as a dict entry: "key":<value-json> (with leading comma when needed) */
void print_record_dict(const SlotHeader *hdr, const uint8_t *block,
                       const char **proj_fields, int proj_count,
                       int *printed, FieldSchema *fs) {
    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s\"%s\":", *printed ? "," : "", key);
    if (proj_count > 0) {
        OUT("{");
        int first = 1;
        for (int i = 0; i < proj_count; i++) {
            char *pv = decode_field(raw, hdr->value_len, proj_fields[i], fs);
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}");
    } else {
        char *val = decode_value(raw, hdr->value_len, fs);
        OUT("%s", val);
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
    int dict_fmt = (format && strcmp(format, "dict") == 0);
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
    if (!paths) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    DIR *d1 = opendir(data_dir);
    if (d1) {
        struct dirent *e1;
        while ((e1 = readdir(d1))) {
            if (e1->d_name[0] == '.') continue;
            size_t nlen = strlen(e1->d_name);
            if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
            if (path_count >= path_cap) {
                path_cap *= 2;
                /* Plain realloc (not xrealloc_or_free) so we can walk paths[]
                   to free already-strdup'd entries before releasing the array. */
                char **t = realloc(paths, path_cap * sizeof(char *));
                if (!t) {
                    /* OOM: free strdup'd entries + the array, zero path_count
                       so downstream loops (the scan at line 4191 and the
                       cleanup at 4235) skip without dereferencing NULL paths. */
                    for (int k = 0; k < path_count; k++) free(paths[k]);
                    free(paths);
                    paths = NULL;
                    path_count = 0;
                    break;
                }
                paths = t;
            }
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
    else if (dict_fmt)
        OUT("{\"results\":{");
    else
        OUT("{\"results\":[");

    /* paths is NULL only when path_count was reset to 0 alongside it
       (initial-malloc fail returns early; realloc fail also zeroes
       path_count). The loop condition `pi < path_count` short-circuits
       when path_count == 0, so paths[pi] is never reached with NULL paths.
       Coverity can't trace the paired invariant — suppress.
       UNINIT for the same reason: paths[pi] is only read when pi < path_count,
       and path_count is incremented only after a successful strdup writes
       paths[path_count]. So paths[0..path_count-1] are always initialized. */
    for (int pi = start_path; pi < path_count && printed < limit; pi++) {
        /* coverity[forward_null] coverity[uninit_use_in_call] paths non-NULL and paths[pi] initialized when pi < path_count */
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
            else if (dict_fmt)
                print_record_dict(hdr, block, proj_fields, proj_count, &printed, fs_ptr);
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
    } else {
        const char *close = dict_fmt ? "}" : "]";
        if (printed >= limit && next_path >= 0)
            OUT("%s,\"cursor\":\"%d:%zu\"}\n", close, next_path, next_slot);
        else
            OUT("%s,\"cursor\":null}\n", close);
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
        if (ex.count >= cap) {
            cap *= 2;
            char **t = realloc(ex.keys, (size_t)cap * sizeof(char *));
            if (!t) break; /* keep what we already collected */
            ex.keys = t;
        }
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
    /* `count` and `printed` are read lock-free in the per-record callback
       (fast-skip when limit reached). The lock only serializes the emit +
       counter section across parallel shard scans, so the read path doesn't
       take it. _Atomic gives the lock-free reader a torn-read-free view. */
    _Atomic int count;
    _Atomic int printed;
    const char **proj_fields;
    int proj_count;
    ExcludedKeys excluded;
    FieldSchema *fs;
    int rows_fmt;
    int dict_fmt;
    char csv_delim;       /* 0 = not CSV; else delimiter char, overrides rows_fmt */
    /* Joins (when njoins > 0, output is always tabular even if rows_fmt==0) */
    const char *driver_object;
    JoinSpec *joins;
    int njoins;
    const char *db_root;
    QueryDeadline *deadline;
    int dl_counter;
    pthread_mutex_t lock;  /* serializes the emit + counter section across
                              parallel shard scans */
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
            /* SQL-style LIKE with `%` wildcard (no `_`). Case-sensitive —
               use `ilike` for case-insensitive. */
            const char *pat = c->value;
            if (pat[0] == '%' && pat[strlen(pat)-1] == '%') {
                char sub[MAX_LINE];
                snprintf(sub, sizeof(sub), "%s", pat + 1);
                sub[strlen(sub)-1] = '\0';
                return strstr(val_str, sub) != NULL;
            }
            if (pat[0] == '%') return strstr(val_str, pat + 1) != NULL;
            if (pat[strlen(pat)-1] == '%') {
                return strncmp(val_str, pat, strlen(pat)-1) == 0;
            }
            return strcmp(val_str, pat) == 0;
        }
        case OP_NOT_LIKE: {
            SearchCriterion tmp = *c;
            tmp.op = OP_LIKE;
            return !match_criterion(val_str, &tmp);
        }
        case OP_CONTAINS:
            return strstr(val_str, c->value) != NULL;
        case OP_NOT_CONTAINS:
            return strstr(val_str, c->value) == NULL;
        case OP_STARTS_WITH:
            return strncmp(val_str, c->value, strlen(c->value)) == 0;
        case OP_ENDS_WITH: {
            size_t vl = strlen(val_str), cl = strlen(c->value);
            if (vl < cl) return 0;
            return strcmp(val_str + vl - cl, c->value) == 0;
        }
        /* Case-insensitive variants (ASCII tolower). Indexed paths fall
           through to the default full-scan branch in btree_dispatch and
           filter per entry in the callbacks below. */
        case OP_ILIKE: {
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
        case OP_INOT_LIKE: {
            SearchCriterion tmp = *c;
            tmp.op = OP_ILIKE;
            return !match_criterion(val_str, &tmp);
        }
        case OP_ICONTAINS:
            return strcasestr(val_str, c->value) != NULL;
        case OP_INOT_CONTAINS:
            return strcasestr(val_str, c->value) == NULL;
        case OP_ISTARTS_WITH:
            return strncasecmp(val_str, c->value, strlen(c->value)) == 0;
        case OP_IENDS_WITH: {
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
        case OP_REGEX:
        case OP_NOT_REGEX:
            /* Legacy/composite path: no pre-compiled regex available
               (CompiledCriterion lives in the typed path). Compile inline
               per-call — slow but rare; typed-binary records use the fast
               path in match_typed_varchar. */
            {
                regex_t r;
                if (regcomp(&r, c->value, REG_EXTENDED | REG_NOSUB) != 0) return 0;
                int hit = (regexec(&r, val_str, 0, NULL, 0) == 0);
                regfree(&r);
                return c->op == OP_REGEX ? hit : !hit;
            }
        case OP_LEN_EQ:
        case OP_LEN_NEQ:
        case OP_LEN_LESS:
        case OP_LEN_GREATER:
        case OP_LEN_LESS_EQ:
        case OP_LEN_GREATER_EQ:
        case OP_LEN_BETWEEN: {
            /* Legacy/composite path: strlen reads the user-visible length.
               Embedded NULs in varchar (rare) under-report here; the typed
               fast path uses the precise length-prefix and is the canonical
               implementation. */
            int64_t L = (int64_t)strlen(val_str);
            int64_t q1 = (int64_t)strtoll(c->value, NULL, 10);
            int64_t q2 = (int64_t)strtoll(c->value2, NULL, 10);
            switch (c->op) {
                case OP_LEN_EQ:         return L == q1;
                case OP_LEN_NEQ:        return L != q1;
                case OP_LEN_LESS:       return L <  q1;
                case OP_LEN_GREATER:    return L >  q1;
                case OP_LEN_LESS_EQ:    return L <= q1;
                case OP_LEN_GREATER_EQ: return L >= q1;
                case OP_LEN_BETWEEN:    return L >= q1 && L <= q2;
                default:                return 0;
            }
        }
        /* Field-vs-field: only reachable through the typed fast path
           (match_typed); legacy/composite path returns no match. */
        case OP_EQ_FIELD: case OP_NEQ_FIELD:
        case OP_LT_FIELD: case OP_GT_FIELD:
        case OP_LTE_FIELD: case OP_GTE_FIELD:
            return 0;
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

    /* Length ops parse value/value2 as integers regardless of the field's
       native type. Compile time, so the hot path skips strtoll per record. */
    if (cc->op == OP_LEN_EQ || cc->op == OP_LEN_NEQ ||
        cc->op == OP_LEN_LESS || cc->op == OP_LEN_GREATER ||
        cc->op == OP_LEN_LESS_EQ || cc->op == OP_LEN_GREATER_EQ ||
        cc->op == OP_LEN_BETWEEN) {
        cc->i1 = (int64_t)strtoll(c->value, NULL, 10);
        cc->i2 = (int64_t)strtoll(c->value2, NULL, 10);
        return;
    }

    /* Field-vs-field ops: c->value names the RHS sibling field. Resolve to
       a TypedField pointer and require type match — mismatched types yield
       cc->rhs_tf=NULL, which match_typed treats as "no match" for every
       record (graceful degradation; planner sees the leaf as non-indexable). */
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        /* ts is non-NULL here — the early-return at the top of compile_one
           (cc->tf = NULL on idx < 0) only proceeds when ts && idx >= 0. */
        int rhs_idx = typed_field_index(ts, c->value);
        if (rhs_idx >= 0) {
            const TypedField *rhs = &ts->fields[rhs_idx];
            if (rhs->type == cc->ftype) cc->rhs_tf = rhs;
            /* else: leave rhs_tf NULL; match returns 0 every time. */
        }
        return;
    }

    /* Regex ops: compile pattern once with REG_EXTENDED. REG_NOSUB is
       deliberately omitted — REG_STARTEND in match_typed_varchar relies
       on pmatch[0] being honored, which REG_NOSUB suppresses. The
       per-call subgroup-tracking cost is small for the patterns users
       actually write. Failed regcomp leaves re_compiled=0 → no record
       matches OP_REGEX, every record matches OP_NOT_REGEX. */
    if (cc->op == OP_REGEX || cc->op == OP_NOT_REGEX) {
        cc->re = malloc(sizeof(regex_t));
        if (cc->re && regcomp(cc->re, c->value, REG_EXTENDED) == 0) {
            cc->re_compiled = 1;
        } else if (cc->re) {
            free(cc->re); cc->re = NULL;
        }
        return;
    }

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

    /* LIKE/CONTAINS/STARTS/ENDS needle prep for varchar ops.
       Stores the needle pre-stripped (LIKE strips `%`) in cc->needle_lc.
       CS family (LIKE/NOT_LIKE/CONTAINS/NOT_CONTAINS/STARTS_WITH/ENDS_WITH):
         needle stored raw — matchers use memcmp/memmem.
       CI family (ILIKE/INOT_LIKE/ICONTAINS/INOT_CONTAINS/ISTARTS_WITH/IENDS_WITH):
         needle stored ASCII-lowered — matchers tolower the haystack per char.
       The field name `needle_lc` is now misleading; kept for diff continuity. */
    int is_like_op = (cc->op == OP_LIKE || cc->op == OP_NOT_LIKE ||
                      cc->op == OP_ILIKE || cc->op == OP_INOT_LIKE);
    int is_substr_op = (cc->op == OP_CONTAINS || cc->op == OP_NOT_CONTAINS ||
                        cc->op == OP_ICONTAINS || cc->op == OP_INOT_CONTAINS ||
                        cc->op == OP_STARTS_WITH || cc->op == OP_ENDS_WITH ||
                        cc->op == OP_ISTARTS_WITH || cc->op == OP_IENDS_WITH);
    int is_ci_op = (cc->op == OP_ILIKE || cc->op == OP_INOT_LIKE ||
                    cc->op == OP_ICONTAINS || cc->op == OP_INOT_CONTAINS ||
                    cc->op == OP_ISTARTS_WITH || cc->op == OP_IENDS_WITH);

    if (cc->ftype == FT_VARCHAR && (is_like_op || is_substr_op)) {
        const char *pat = c->value;
        size_t pl = cc->s1_len;
        const char *needle_start = pat;
        size_t needle_len = pl;
        cc->like_kind = LK_EXACT;

        if (is_like_op) {
            if (pl >= 2 && pat[0] == '%' && pat[pl-1] == '%') {
                cc->like_kind = LK_CONTAINS;
                needle_start = pat + 1;
                needle_len = pl - 2;
            } else if (pl >= 1 && pat[0] == '%') {
                cc->like_kind = LK_CONTAINS;
                needle_start = pat + 1;
                needle_len = pl - 1;
            } else if (pl >= 1 && pat[pl-1] == '%') {
                cc->like_kind = LK_PREFIX;
                needle_len = pl - 1;
            }
        }

        cc->needle_len = needle_len;
        if (is_ci_op) {
            cc->needle_lc = strdup_lower(needle_start, needle_len);
        } else {
            cc->needle_lc = malloc(needle_len > 0 ? needle_len : 1);
            if (needle_len > 0) memcpy(cc->needle_lc, needle_start, needle_len);
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
        if (arr[i].re) {
            if (arr[i].re_compiled) regfree(arr[i].re);
            free(arr[i].re);
        }
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
    /* CS string ops — needle stored raw in cc->needle_lc (despite the
       legacy field name; compile_one only lowers for CI variants below). */
    case OP_LIKE:
        switch (cc->like_kind) {
        case LK_EXACT:
            return elen == (int)cc->needle_len &&
                   memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
        case LK_PREFIX:
            return elen >= (int)cc->needle_len &&
                   memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
        case LK_CONTAINS:
            return memmem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
        }
        return 0;
    case OP_NOT_LIKE: {
        CompiledCriterion tmp = *cc; tmp.op = OP_LIKE;
        return !match_typed_varchar(p, size, &tmp);
    }
    case OP_CONTAINS:
        return memmem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
    case OP_NOT_CONTAINS:
        return memmem(hay, elen, cc->needle_lc, cc->needle_len) == NULL;
    case OP_STARTS_WITH:
        return elen >= (int)cc->needle_len &&
               memcmp(hay, cc->needle_lc, cc->needle_len) == 0;
    case OP_ENDS_WITH:
        return elen >= (int)cc->needle_len &&
               memcmp(hay + elen - cc->needle_len, cc->needle_lc, cc->needle_len) == 0;
    /* CI variants — needle is pre-lowered in compile_one; haystack is
       lowered per char on the hot path. memcasemem fuses both. */
    case OP_ILIKE:
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
    case OP_INOT_LIKE: {
        CompiledCriterion tmp = *cc; tmp.op = OP_ILIKE;
        return !match_typed_varchar(p, size, &tmp);
    }
    case OP_ICONTAINS:
        return memcasemem(hay, elen, cc->needle_lc, cc->needle_len) != NULL;
    case OP_INOT_CONTAINS:
        return memcasemem(hay, elen, cc->needle_lc, cc->needle_len) == NULL;
    case OP_ISTARTS_WITH: {
        if (elen < (int)cc->needle_len) return 0;
        for (size_t i = 0; i < cc->needle_len; i++) {
            char a = hay[i], b = cc->needle_lc[i];
            if (a >= 'A' && a <= 'Z') a = (char)(a + 32);
            if (a != b) return 0;
        }
        return 1;
    }
    case OP_IENDS_WITH: {
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
    case OP_LEN_EQ:         return elen == (int)cc->i1;
    case OP_LEN_NEQ:        return elen != (int)cc->i1;
    case OP_LEN_LESS:       return elen <  (int)cc->i1;
    case OP_LEN_GREATER:    return elen >  (int)cc->i1;
    case OP_LEN_LESS_EQ:    return elen <= (int)cc->i1;
    case OP_LEN_GREATER_EQ: return elen >= (int)cc->i1;
    case OP_LEN_BETWEEN:    return elen >= (int)cc->i1 && elen <= (int)cc->i2;
    case OP_REGEX:
    case OP_NOT_REGEX: {
        if (!cc->re_compiled) return cc->op == OP_REGEX ? 0 : 1;
        /* REG_STARTEND lets regexec consume (ptr, len) directly so we
           don't need to copy + NUL-terminate every record's value. */
        regmatch_t pm[1];
        pm[0].rm_so = 0;
        pm[0].rm_eo = elen;
        int rc = regexec(cc->re, hay, 0, pm, REG_STARTEND);
        return cc->op == OP_REGEX ? (rc == 0) : (rc != 0);
    }
    /* Field-vs-field is intercepted in match_typed before per-type dispatch;
       these labels exist only to keep the switch exhaustive (silences -Wswitch). */
    case OP_EQ_FIELD: case OP_NEQ_FIELD:
    case OP_LT_FIELD: case OP_GT_FIELD:
    case OP_LTE_FIELD: case OP_GTE_FIELD:
        return 0;
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

/* Generic 3-way comparator for a typed field given two pointers into the
   same record. Returns negative / 0 / positive. Both pointers must reference
   the same TypedField type — compile_one enforces this for field-vs-field
   ops by setting rhs_tf only when types match. */
static int cmp_typed_field_pair(const uint8_t *a, const uint8_t *b,
                                const TypedField *f) {
    switch (f->type) {
    case FT_VARCHAR: {
        int la = varchar_eff_len(a, f->size);
        int lb = varchar_eff_len(b, f->size);
        size_t n = la < lb ? (size_t)la : (size_t)lb;
        int r = memcmp(a + 2, b + 2, n);
        if (r != 0) return r;
        return la - lb;
    }
    case FT_LONG:    { int64_t va = ld_be_i64(a), vb = ld_be_i64(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_INT:     { int32_t va = ld_be_i32(a), vb = ld_be_i32(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_SHORT:   { int16_t va = ld_be_i16(a), vb = ld_be_i16(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_NUMERIC: { int64_t va = ld_be_i64(a), vb = ld_be_i64(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_DOUBLE:  { double va, vb; memcpy(&va, a, 8); memcpy(&vb, b, 8);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_BOOL:
    case FT_BYTE:    return (int)a[0] - (int)b[0];
    case FT_DATE:    { int32_t va = ld_be_i32(a), vb = ld_be_i32(b);
                       return va < vb ? -1 : (va > vb ? 1 : 0); }
    case FT_DATETIME: {
        int64_t va = (int64_t)ld_be_i32(a) * 100000LL + ld_be_u16(a + 4);
        int64_t vb = (int64_t)ld_be_i32(b) * 100000LL + ld_be_u16(b + 4);
        return va < vb ? -1 : (va > vb ? 1 : 0);
    }
    default: return 0;
    }
}

static int field_vs_field_match(int cmp, enum SearchOp op) {
    switch (op) {
    case OP_EQ_FIELD:  return cmp == 0;
    case OP_NEQ_FIELD: return cmp != 0;
    case OP_LT_FIELD:  return cmp <  0;
    case OP_GT_FIELD:  return cmp >  0;
    case OP_LTE_FIELD: return cmp <= 0;
    case OP_GTE_FIELD: return cmp >= 0;
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

    /* Field-vs-field: compare LHS bytes against the sibling RHS field on
       the same record. cc->rhs_tf is NULL when types didn't match at
       compile time — every record fails (caller's fault). */
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        if (!cc->rhs_tf) return 0;
        int r = cmp_typed_field_pair(rec + cc->tf->offset,
                                     rec + cc->rhs_tf->offset, cc->tf);
        return field_vs_field_match(r, cc->op);
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
            JoinSpec *t = xrealloc_or_free(arr, cap * sizeof(*t));
            if (!t) { arr = NULL; break; }
            arr = t;
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
            if (!btree_idx_exists(db_root, j->object, j->remote_field, j->remote_sch.splits)) {
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
                    j->proj_fields[j->proj_count][255] = '\0';
                    j->proj_tfs[j->proj_count] = &j->remote_fs.ts->fields[k];
                    j->proj_count++;
                }
            }
        } else {
            /* Belt-and-suspenders re-term — proj_fields[] is null-terminated
               at population (parse_one_join via memcpy + explicit '\0', and
               the auto-fill above via strncpy + explicit '\0'), but Coverity
               can't propagate the post-condition to consumers. */
            for (int k = 0; k < j->proj_count; k++) j->proj_fields[k][255] = '\0';
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

/* Forward decl — definition lives near btree_dispatch below. */
static const TypedField *resolve_idx_field(const TypedSchema *ts, const char *field);

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
        /* Encode local_key as an index key for the REMOTE field's type —
           that's how the remote .idx stores its values. */
        const TypedField *rem_tf = resolve_idx_field(j->remote_fs.ts, j->remote_field);
        uint8_t keybuf[1032];
        size_t keylen;
        if (rem_tf) {
            encode_field_for_index(rem_tf, local_key, local_len, keybuf, &keylen);
            btree_idx_search(db_root, j->object, j->remote_field, j->remote_sch.splits,
                             (const char *)keybuf, keylen, join_bt_first_cb, &hit);
        } else {
            /* Composite remote field or untyped — raw passthrough. */
            btree_idx_search(db_root, j->object, j->remote_field, j->remote_sch.splits,
                             local_key, local_len, join_bt_first_cb, &hit);
        }
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
    /* Best-effort pre-checks (no lock). The emit section below re-checks
       limit under the lock to ensure we never over-emit.
       coverity[lock_evasion] coverity[missing_lock] — `_Atomic int printed`
       gives torn-read-free visibility; staleness costs at most one wasted
       record-fetch before the locked re-check at line 5731 catches it. */
    if (sc->limit > 0 && sc->printed >= sc->limit) return 1;
    if (query_deadline_tick(sc->deadline, &sc->dl_counter)) return 1;

    char *key = malloc(hdr->key_len + 1);
    memcpy(key, block, hdr->key_len);
    key[hdr->key_len] = '\0';

    /* Check excluded keys first */
    if (is_excluded(&sc->excluded, key)) { free(key); return 0; }

    const char *raw = (const char *)block + hdr->key_len;

    /* Criteria match + join resolution are thread-local reads, lock-free. */
    int match = criteria_match_tree((const uint8_t *)raw, sc->tree, sc->fs);

    if (match) {
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
            /* Emit section — must serialize: OUT() bytes, sc->printed / sc->count
               updates, and the "printed>0 ? comma" decision all depend on a
               consistent view of shared state. */
            pthread_mutex_lock(&sc->lock);
            if (sc->limit > 0 && sc->printed >= sc->limit) {
                pthread_mutex_unlock(&sc->lock);
                if (sc->njoins > 0) {
                    for (int i = 0; i < sc->njoins; i++)
                        if (join_handles[i].map) fcache_release(join_handles[i]);
                    free(join_handles); free(join_raws);
                }
                free(key);
                return 1;
            }
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
                } else if (sc->dict_fmt) {
                    OUT("%s\"%s\":", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        OUT("{");
                        int first = 1;
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = decode_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            if (!pv) continue;
                            OUT("%s\"%s\":\"%s\"", first ? "" : ",", sc->proj_fields[i], pv);
                            first = 0;
                            free(pv);
                        }
                        OUT("}");
                    } else {
                        char *val = decode_value(raw, hdr->value_len, sc->fs);
                        OUT("%s", val);
                        free(val);
                    }
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
            pthread_mutex_unlock(&sc->lock);
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

/* Collecting callback for find — gathers hashes, applies primary filter.
   Lock-free: the entries buffer is a single mmap MAP_ANONYMOUS|MAP_NORESERVE
   reservation up to QUERY_BUFFER_MB. Workers claim slot indices via
   atomic_fetch_add; the kernel lazy-commits pages on first write, so a
   query that only collects 100 hashes pays for ~1 page (4 KB) of physical
   RAM despite the large virtual reservation. No realloc → no pointer
   migration → no need for a lock around appends. The btree_idx_* wrappers
   fan out per shard with parallel_for so this callback is invoked from
   multiple worker threads simultaneously. */
typedef struct {
    CollectedHash *entries;     /* mmap'd region; pointer is stable */
    size_t reservation_bytes;   /* total mmap size for munmap */
    size_t count;               /* updated via __atomic_fetch_add */
    size_t cap;                 /* entries-capacity (immutable after init) */
    int splits;
    int collect_cap;            /* caller-supplied early-stop limit (0 = none) */
    SearchCriterion *primary_crit;
    int check_primary;
    QueryDeadline *deadline;
    int dl_counter;
    int budget_exceeded;        /* updated via __atomic_store_n */
} CollectCtx;

/* Init/destroy helpers. The mmap is kernel-side cheap (lazy commit) — the
   reservation only consumes virtual address space until pages are touched.
   Caller still doesn't need to set cc.cap / cc.entries; collect_ctx_init
   sizes the buffer to the per-query buffer cap. */
static inline void collect_ctx_init(CollectCtx *cc) {
    memset(cc, 0, sizeof(*cc));
    cc->reservation_bytes = g_query_buffer_max_bytes;
    cc->entries = mmap(NULL, cc->reservation_bytes,
                       PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (cc->entries == MAP_FAILED) {
        cc->entries = NULL;
        cc->cap = 0;
    } else {
        cc->cap = cc->reservation_bytes / sizeof(CollectedHash);
    }
}

static inline void collect_ctx_destroy(CollectCtx *cc) {
    if (cc->entries) munmap(cc->entries, cc->reservation_bytes);
    cc->entries = NULL;
}

/* Forward decls — defined alongside btree_dispatch below; declared here so
   collect_hash_cb can route LEN_* ops through the vlen-only fast path. */
static int op_is_length(enum SearchOp op);
static int match_length_vlen(size_t vlen, const SearchCriterion *c);

static int collect_hash_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    CollectCtx *cc = (CollectCtx *)ctx;
    if (query_deadline_tick(cc->deadline, &cc->dl_counter)) return -1;

    /* Per-entry filter — fully lock-free, parallel-safe. */
    if (cc->primary_crit && op_is_length(cc->primary_crit->op)) {
        if (!match_length_vlen(vlen, cc->primary_crit)) return 0;
    } else if (cc->check_primary && cc->primary_crit) {
        char tmp[1028];
        size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
        memcpy(tmp, val, cl); tmp[cl] = '\0';
        if (!match_criterion(tmp, cc->primary_crit)) return 0;
    }

    /* Atomic slot allocation. Beyond cap or the caller-supplied early-stop
       limit → bail; the buffer is mmap'd up to the per-query cap so the
       former realloc-with-mutex path is gone. */
    size_t idx = __atomic_fetch_add(&cc->count, 1, __ATOMIC_RELAXED);
    if (cc->collect_cap > 0 && idx >= (size_t)cc->collect_cap) return -1;
    if (idx >= cc->cap) {
        __atomic_store_n(&cc->budget_exceeded, 1, __ATOMIC_RELAXED);
        return -1;
    }

    CollectedHash *e = &cc->entries[idx];
    memcpy(e->hash, hash16, 16);
    addr_from_hash(hash16, cc->splits, &e->shard_id, &e->start_slot);
    return 0;
}

/* Look up TypedField for a criterion's indexed field. Returns NULL for
   composite indexes (pc->field contains '+') or when the field isn't in
   the typed schema — both cases fall through to raw-byte index semantics. */
static const TypedField *resolve_idx_field(const TypedSchema *ts, const char *field) {
    if (!ts || !field || !field[0]) return NULL;
    if (strchr(field, '+')) return NULL;
    int fi = typed_field_index(ts, field);
    return (fi >= 0) ? &ts->fields[fi] : NULL;
}

/* Encode a criterion value for index lookup. If tf is NULL (composite index
   or unknown field), returns the text as raw bytes. Otherwise emits
   memcmp-sortable bytes matching what the write side stored. Output written
   into caller's buf; *out_len set. */
static void encode_criterion_value(const TypedField *tf,
                                   const char *val, size_t vlen,
                                   uint8_t *buf, size_t *out_len) {
    if (!tf) {
        memcpy(buf, val, vlen);
        *out_len = vlen;
        return;
    }
    encode_field_for_index(tf, val, vlen, buf, out_len);
}

/* Count-mode shortcut: operator has a positive counterpart so
   count(neg) = count(*) - count(pos) is cheaper. NEQ / NOT_IN hit a
   targeted positive set instead of walking the complement; NOT_EXISTS
   also fixes a pre-existing count-inversion (the old path returned the
   full index size instead of the inverse). */
static int op_is_negatable(enum SearchOp op) {
    return op == OP_NOT_EQUAL || op == OP_NOT_LIKE || op == OP_NOT_CONTAINS ||
           op == OP_NOT_IN || op == OP_NOT_EXISTS;
}

static enum SearchOp op_invert(enum SearchOp op) {
    switch (op) {
    case OP_NOT_EQUAL:    return OP_EQUAL;
    case OP_NOT_LIKE:     return OP_LIKE;
    case OP_NOT_CONTAINS: return OP_CONTAINS;
    case OP_NOT_IN:       return OP_IN;
    case OP_NOT_EXISTS:   return OP_EXISTS;
    default:              return op;
    }
}

/* Ops where btree_dispatch falls through to the default full-leaf scan
   (no precise range), so the callback must validate per entry against
   the criterion. The CI variants (ILIKE/ICONTAINS/...) join because the
   btree is byte-sorted and case-folding isn't byte-monotonic — there's
   no range shortcut. Length ops are handled separately via op_is_length. */
static int op_needs_check_primary(enum SearchOp op) {
    return op == OP_CONTAINS || op == OP_LIKE || op == OP_ENDS_WITH ||
           op == OP_NOT_LIKE || op == OP_NOT_CONTAINS || op == OP_NOT_IN ||
           op == OP_ILIKE || op == OP_ICONTAINS ||
           op == OP_ISTARTS_WITH || op == OP_IENDS_WITH ||
           op == OP_INOT_LIKE || op == OP_INOT_CONTAINS;
}

/* True if the op is a length comparator answerable from (val, vlen) alone —
   no record fetch needed, just inspect the btree leaf entry's vlen. */
static int op_is_length(enum SearchOp op) {
    return op == OP_LEN_EQ || op == OP_LEN_NEQ ||
           op == OP_LEN_LESS || op == OP_LEN_GREATER ||
           op == OP_LEN_LESS_EQ || op == OP_LEN_GREATER_EQ ||
           op == OP_LEN_BETWEEN;
}

/* Length match against the btree entry's vlen — exact even when the value
   contains embedded NULs (which strlen would under-report). Bypasses the
   tmp-string roundtrip in match_criterion for hot-path callbacks. */
static int match_length_vlen(size_t vlen, const SearchCriterion *c) {
    int64_t L = (int64_t)vlen;
    int64_t q1 = (int64_t)strtoll(c->value, NULL, 10);
    int64_t q2 = (int64_t)strtoll(c->value2, NULL, 10);
    switch (c->op) {
        case OP_LEN_EQ:         return L == q1;
        case OP_LEN_NEQ:        return L != q1;
        case OP_LEN_LESS:       return L <  q1;
        case OP_LEN_GREATER:    return L >  q1;
        case OP_LEN_LESS_EQ:    return L <= q1;
        case OP_LEN_GREATER_EQ: return L >= q1;
        case OP_LEN_BETWEEN:    return L >= q1 && L <= q2;
        default:                return 0;
    }
}

/* Dispatch B+ tree query based on search operator. Used by find, count, aggregate.
   tf is the TypedField for pc->field (NULL for composite indexes or
   untyped objects — in that case values are passed as raw bytes, matching
   the legacy ASCII storage of composite indexes). With the per-shard index
   layout each call fans out across splits/4 shard files internally. */
static void btree_dispatch(const char *db_root, const char *object,
                           const char *field, int splits,
                           SearchCriterion *pc, const TypedField *tf,
                           bt_result_cb cb, void *ctx) {
    /* Stack buffers for encoded key bytes. Max fixed-type output is 8 B;
       varchar caps at f->size - 2 ≤ 65533 — BT_MAX_VAL_LEN = 512 in practice.
       Keep generous for the "contains text + 4 sentinel bytes" suffix cases. */
    uint8_t buf1[1032], buf2[1032];
    size_t len1 = 0, len2 = 0;

    switch (pc->op) {
        case OP_EQUAL:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_search(db_root, object, field, splits,
                             (const char *)buf1, len1, cb, ctx);
            break;
        case OP_GREATER_EQ:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, len1,
                            "\xff\xff\xff\xff", 4, cb, ctx);
            break;
        case OP_GREATER:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               (const char *)buf1, len1, 1,
                               "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        case OP_LESS_EQ:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range(db_root, object, field, splits,
                            "", 0, (const char *)buf1, len1, cb, ctx);
            break;
        case OP_LESS:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               "", 0, 0,
                               (const char *)buf1, len1, 1, cb, ctx);
            break;
        case OP_BETWEEN:
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            encode_criterion_value(tf, pc->value2, strlen(pc->value2), buf2, &len2);
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, len1,
                            (const char *)buf2, len2, cb, ctx);
            break;
        case OP_IN:
            for (int iv = 0; iv < pc->in_count; iv++) {
                encode_criterion_value(tf, pc->in_values[iv],
                                       strlen(pc->in_values[iv]), buf1, &len1);
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)buf1, len1, cb, ctx);
            }
            break;
        case OP_STARTS_WITH: {
            /* Prefix-match only makes sense on raw-byte keys (varchar or
               composite). For signed numeric keys with their top-bit flip,
               "starts with X" isn't meaningful — fall through to raw bytes. */
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);
            size_t plen;
            if (raw_prefix) {
                plen = strlen(pc->value);
                memcpy(buf1, pc->value, plen);
            } else {
                encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &plen);
            }
            memcpy(buf2, buf1, plen);
            memset(buf2 + plen, 0xff, 4);
            btree_idx_range(db_root, object, field, splits,
                            (const char *)buf1, plen,
                            (const char *)buf2, plen + 4, cb, ctx);
            break;
        }
        case OP_LIKE: {
            /* CS LIKE: leverage the pattern's % placement to narrow the btree
               read. Only meaningful on varchar / composite (raw-byte) keys.
                 "foo"   (no %)  → point lookup, same as OP_EQUAL.
                 "foo%"  (trail) → prefix range, same as OP_STARTS_WITH.
                 "%foo"  (lead)  → suffix match — no shortcut, full leaf scan.
                 "%foo%" (both)  → substring — no shortcut, full leaf scan.
               compile_one already classified this into pc->value's % placement;
               we re-derive here because btree_dispatch sees only SearchCriterion.
               Callbacks still apply check_primary; for LK_EXACT/LK_PREFIX that's
               a redundant per-entry confirm but cheap. */
            const char *pat = pc->value;
            size_t pl = strlen(pat);
            int lead = (pl >= 1 && pat[0] == '%');
            int trail = (pl >= 1 && pat[pl-1] == '%');
            int raw_prefix = (!tf || tf->type == FT_VARCHAR);

            if (!lead && !trail && raw_prefix) {
                /* Exact byte match — point lookup. */
                encode_criterion_value(tf, pat, pl, buf1, &len1);
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)buf1, len1, cb, ctx);
                break;
            }
            if (!lead && trail && raw_prefix) {
                /* "foo%" — strip trailing % and use prefix range scan. */
                size_t needle_len = pl - 1;
                memcpy(buf1, pat, needle_len);
                memcpy(buf2, buf1, needle_len);
                memset(buf2 + needle_len, 0xff, 4);
                btree_idx_range(db_root, object, field, splits,
                                (const char *)buf1, needle_len,
                                (const char *)buf2, needle_len + 4, cb, ctx);
                break;
            }
            /* Substring / suffix / non-varchar → full leaf scan; per-entry
               filter via check_primary in the callback handles correctness. */
            btree_idx_range(db_root, object, field, splits,
                            "", 0, "\xff\xff\xff\xff", 4, cb, ctx);
            break;
        }
        case OP_NOT_EQUAL:
            /* Boolean has exactly 2 values — neq X is equivalent to eq
               other-value. Single point lookup instead of two range scans,
               which saves a second tree descent at query time. Only
               applies to FT_BOOL; every other type has too large a domain
               to enumerate "everything except X" cheaply. */
            if (tf && tf->type == FT_BOOL) {
                int is_true = (pc->value[0] == 't' || pc->value[0] == 'T' ||
                               pc->value[0] == '1');
                uint8_t inv[1] = { (uint8_t)(is_true ? 0 : 1) };
                btree_idx_search(db_root, object, field, splits,
                                 (const char *)inv, 1, cb, ctx);
                break;
            }
            /* General case: two exclusive ranges covering everything except X. */
            encode_criterion_value(tf, pc->value, strlen(pc->value), buf1, &len1);
            btree_idx_range_ex(db_root, object, field, splits,
                               "", 0, 0,
                               (const char *)buf1, len1, 1, cb, ctx);
            btree_idx_range_ex(db_root, object, field, splits,
                               (const char *)buf1, len1, 1,
                               "\xff\xff\xff\xff", 4, 0, cb, ctx);
            break;
        default:
            /* Full index scan: contains, like, ends_with, not_like, not_contains, not_in, exists */
            btree_idx_range(db_root, object, field, splits,
                            "", 0, "\xff\xff\xff\xff", 4, cb, ctx);
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

/* Single-criterion inline counter. No record fetch — btree visit = match.
   Thread-safe: count increment is atomic (the btree_idx_* wrappers fan out
   per shard via parallel_for so this callback fires from multiple threads
   concurrently). dl_counter races are tolerated — query_deadline_tick is a
   coarse heuristic, the only consequence of a few lost increments is the
   deadline check happening slightly more or less often. */
static int idx_count_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)hash16;
    IdxCountCtx *ic = (IdxCountCtx *)ctx;
    if (query_deadline_tick(ic->deadline, &ic->dl_counter)) return -1;
    if (ic->primary_crit && op_is_length(ic->primary_crit->op)) {
        if (!match_length_vlen(vlen, ic->primary_crit)) return 0;
    } else if (ic->check_primary && ic->primary_crit) {
        char tmp[1028];
        size_t cl = vlen < sizeof(tmp) - 1 ? vlen : sizeof(tmp) - 1;
        memcpy(tmp, val, cl); tmp[cl] = '\0';
        if (!match_criterion(tmp, ic->primary_crit)) return 0;
    }
    __atomic_add_fetch(&ic->count, 1, __ATOMIC_RELAXED);
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

    /* Use the persistent shard mmap cache. Per-call open + mmap + munmap
       was paying ~100µs of page-fault + TLB-flush per shard per query —
       visible on `find` indexed paths as a 2-3x slowdown vs the README
       baseline despite the actual btree work being microseconds. */
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint8_t *map = fc.map;
    uint32_t slots = fc.slots_per_shard;
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
                        size_t new_cap = sw->result_cap ? sw->result_cap * 2 : 64;
                        MatchResult *t = xrealloc_or_free(sw->results, new_cap * sizeof(MatchResult));
                        if (!t) {
                            sw->results = NULL;
                            sw->result_count = 0;
                            sw->result_cap = 0;
                            free(key);
                            if (sw->njoins > 0) {
                                for (int i = 0; i < sw->njoins; i++)
                                    if (jhs[i].map) fcache_release(jhs[i]);
                                free(jhs); free(jraws);
                            }
                            fcache_release(fc);
                            return NULL;
                        }
                        sw->results = t;
                        sw->result_cap = new_cap;
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
                        /* Sum the actual sizes per field — the prior 256-byte-per-
                           field heuristic underestimated when long varchars were
                           projected. CodeQL flagged the unchecked snprintf advance. */
                        size_t bsz = 256;
                        char **decoded = calloc(sw->proj_count, sizeof(char *));
                        for (int fi = 0; fi < sw->proj_count; fi++) {
                            decoded[fi] = decode_field(raw, raw_len,
                                                       sw->proj_fields[fi], sw->fs);
                            if (decoded[fi])
                                bsz += strlen(sw->proj_fields[fi]) +
                                       strlen(decoded[fi]) + 16;
                        }
                        mr->json = malloc(bsz);
                        size_t pos = 0;
                        SB_APPEND(mr->json, pos, bsz, "{");
                        int first = 1;
                        for (int fi = 0; fi < sw->proj_count; fi++) {
                            if (!decoded[fi]) continue;
                            SB_APPEND(mr->json, pos, bsz, "%s\"%s\":\"%s\"",
                                      first ? "" : ",",
                                      sw->proj_fields[fi], decoded[fi]);
                            first = 0;
                            free(decoded[fi]);
                        }
                        free(decoded);
                        SB_APPEND(mr->json, pos, bsz, "}");
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
    fcache_release(fc);
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

    /* Use the persistent shard mmap cache — same path as shard_find_worker.
       Direct mmap+munmap per query was paying ~100µs of page-fault + TLB
       work per shard, swamping the actual count work for selective queries. */
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    size_t local = 0;
    for (int ei = 0; ei < sc->entry_count; ei++) {
        if (query_deadline_tick(sc->deadline, &sc->dl_counter)) break;
        CollectedHash *e = &sc->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;

            const uint8_t *raw = fc.map + zoneB_off(s, slots, sc->sch->slot_size) + h->key_len;
            if (criteria_match_tree(raw, sc->tree, sc->fs)) local++;
            break;
        }
    }
    fcache_release(fc);
    sc->count = local;
    return NULL;
}

/* Orchestrate parallel indexed count: qsort by shard, fan out per-shard workers. */
static size_t parallel_indexed_count(const char *db_root, const char *object,
                                     const Schema *sch, CollectedHash *batch,
                                     int batch_count, CriteriaNode *tree,
                                     FieldSchema *fs, QueryDeadline *dl) {
    int group_starts[1024], group_sizes[1024];
    int nshard_groups = shard_group_batch(batch, batch_count, group_starts, group_sizes, 1024);

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

    int group_starts[1024], group_sizes[1024]; /* max 1K shards in a batch */
    int nshard_groups = shard_group_batch(batch, batch_count, group_starts, group_sizes, 1024);

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
    collect_ctx_init(&cc);
    cc.splits = sch->splits;
    cc.collect_cap = collect_target;
    cc.primary_crit = primary_crit;
    cc.check_primary = check_primary;
    cc.deadline = dl;

    (void)primary_idx_path; /* path now derived per-shard inside btree_idx_*; arg kept for API stability */
    btree_dispatch(db_root, object, primary_crit->field, sch->splits,
                   primary_crit,
                   resolve_idx_field(fs ? fs->ts : NULL, primary_crit->field),
                   collect_hash_cb, &cc);

    if (cc.budget_exceeded) { collect_ctx_destroy(&cc); return -2; }
    if (cc.count == 0)      { collect_ctx_destroy(&cc); return 0; }

    int count = 0, printed = 0;
    process_batch(cc.entries, cc.count, db_root, object, sch,
                 tree, excluded,
                 proj_fields, proj_count, fs, offset, limit, &count, &printed,
                 rows_fmt, csv_delim, joins, njoins, dl);

    collect_ctx_destroy(&cc);
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
    if (strcmp(s, "len_eq") == 0) return OP_LEN_EQ;
    if (strcmp(s, "len_neq") == 0) return OP_LEN_NEQ;
    if (strcmp(s, "len_lt") == 0) return OP_LEN_LESS;
    if (strcmp(s, "len_gt") == 0) return OP_LEN_GREATER;
    if (strcmp(s, "len_lte") == 0) return OP_LEN_LESS_EQ;
    if (strcmp(s, "len_gte") == 0) return OP_LEN_GREATER_EQ;
    if (strcmp(s, "len_between") == 0) return OP_LEN_BETWEEN;
    if (strcmp(s, "ilike") == 0) return OP_ILIKE;
    if (strcmp(s, "inlike") == 0 || strcmp(s, "inot_like") == 0 ||
        strcmp(s, "not_ilike") == 0) return OP_INOT_LIKE;
    if (strcmp(s, "icontains") == 0) return OP_ICONTAINS;
    if (strcmp(s, "incontains") == 0 || strcmp(s, "inot_contains") == 0 ||
        strcmp(s, "not_icontains") == 0) return OP_INOT_CONTAINS;
    if (strcmp(s, "istarts") == 0 || strcmp(s, "istarts_with") == 0) return OP_ISTARTS_WITH;
    if (strcmp(s, "iends") == 0 || strcmp(s, "iends_with") == 0) return OP_IENDS_WITH;
    if (strcmp(s, "eq_field") == 0) return OP_EQ_FIELD;
    if (strcmp(s, "neq_field") == 0) return OP_NEQ_FIELD;
    if (strcmp(s, "lt_field") == 0) return OP_LT_FIELD;
    if (strcmp(s, "gt_field") == 0) return OP_GT_FIELD;
    if (strcmp(s, "lte_field") == 0) return OP_LTE_FIELD;
    if (strcmp(s, "gte_field") == 0) return OP_GTE_FIELD;
    if (strcmp(s, "regex") == 0) return OP_REGEX;
    if (strcmp(s, "not_regex") == 0 || strcmp(s, "nregex") == 0) return OP_NOT_REGEX;
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
                    /* The skip-ws/comma loop can advance ap to the NUL
                       terminator if the input ends with a trailing comma
                       and no closing ']' (the upstream json_skip_value
                       can be tricked into truncating the span at an
                       embedded NUL — see fuzzer-found bug). Without this
                       guard, the else `ap++` below walks past NUL → OOB
                       read on next iteration. */
                    if (!*ap) break;
                    if (*ap == ']') break;
                    if (*ap == '"') {
                        ap++;
                        const char *start = ap;
                        while (*ap && *ap != '"') ap++;
                        size_t len = ap - start;
                        if (c->in_count >= c->in_cap) {
                            int new_cap = c->in_cap * 2;
                            char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                            if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                            c->in_values = t;
                            c->in_cap = new_cap;
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
                        int new_cap = c->in_cap * 2;
                        char **t = xrealloc_or_free(c->in_values, (size_t)new_cap * sizeof(char *));
                        if (!t) { c->in_values = NULL; c->in_count = 0; c->in_cap = 0; break; }
                        c->in_values = t;
                        c->in_cap = new_cap;
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
            /* Guard: json_skip may have walked p to the NUL terminator
               on a buffer that has trailing whitespace (or never had a
               closing `}`). Without this break, the unrecognised-char
               fall-through (`if (*p != '"') { p++; continue; }`) below
               advances past the NUL → heap-OOB read on the next loop
               iteration. Found by libFuzzer; same fix pattern as the
               json_skip_value and parse_one_criterion bugs. */
            if (!*p) break;
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
    /* Field-vs-field ops can't use a btree on the LHS alone — the RHS is
       per-record, so even a perfect btree walk still pays record fetches.
       Force full-scan to keep the planner honest. */
    if (c->op == OP_EQ_FIELD || c->op == OP_NEQ_FIELD ||
        c->op == OP_LT_FIELD || c->op == OP_GT_FIELD ||
        c->op == OP_LTE_FIELD || c->op == OP_GTE_FIELD) return 0;
    /* Regex needs full content of every value to match — btree leaf bytes
       could carry it, but the per-entry regexec cost dominates so the
       indexed path's only saving is record-fetch avoidance, marginal vs
       the full scan. Keep regex on the full-scan path; revisit only with
       a real workload signal. */
    if (c->op == OP_REGEX || c->op == OP_NOT_REGEX) return 0;

    /* Per-shard layout: index lives at <obj>/indexes/<field>/<NNN>.idx with
       splits/4 shards. btree_idx_exists checks for any non-empty shard.
       Old single-file <field>.idx layout is gone; load_splits trips into
       the schema cache so this stays cheap on the planner hot path. */
    Schema sch = load_schema(db_root, object);
    if (!btree_idx_exists(db_root, object, c->field, sch.splits)) return 0;
    if (out_idx_path) {
        /* out_idx_path is now an opaque tag for callers that want a
           non-empty string to mean "indexed". The per-shard wrappers
           rebuild the real per-shard paths internally from (field, splits). */
        snprintf(out_idx_path, out_sz, "%s/%s/indexes/%s",
                 db_root, object, c->field);
    }
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
    PRIMARY_NONE,         /* full scan */
    PRIMARY_LEAF,         /* single indexed leaf drives btree_range */
    PRIMARY_KEYSET,       /* OR sub-tree drives index-union into a KeySet */
    PRIMARY_INTERSECT     /* AND of 2+ indexed leaves intersected via KeySet */
} PrimaryKind;

#define MAX_INTERSECT_LEAVES 8

typedef struct {
    PrimaryKind kind;
    SearchCriterion *primary_leaf;   /* PRIMARY_LEAF */
    char primary_idx_path[PATH_MAX];
    CriteriaNode *or_node;           /* PRIMARY_KEYSET */

    /* PRIMARY_INTERSECT: ordered (most-selective-first) by op_selectivity_rank.
       Excess indexed leaves beyond MAX_INTERSECT_LEAVES stay as post-filters
       in criteria_match_tree. */
    SearchCriterion *intersect_leaves[MAX_INTERSECT_LEAVES];
    char intersect_paths[MAX_INTERSECT_LEAVES][PATH_MAX];
    int intersect_count;
} QueryPlan;

/* True if the operator yields a precise btree candidate set without needing
   per-record verification. Excludes substring/suffix ops (LIKE/CONTAINS/...
   need check_primary) and large-set ops (NEQ/NOT_IN/NOT_LIKE/NOT_CONTAINS —
   intersecting with a near-everything set wastes work; the existing primary-
   leaf path with subtraction shortcut is already tighter for these). */
static int op_eligible_for_intersect(enum SearchOp op) {
    return op == OP_EQUAL ||
           op == OP_LESS || op == OP_GREATER ||
           op == OP_LESS_EQ || op == OP_GREATER_EQ ||
           op == OP_BETWEEN || op == OP_IN ||
           op == OP_STARTS_WITH;
}

/* Lower rank = more selective ⇒ walk first to bound the running intersection.
   No stats yet; this is an operator-class heuristic, refined later. */
static int op_selectivity_rank(enum SearchOp op) {
    switch (op) {
        case OP_EQUAL:       return 0;
        case OP_STARTS_WITH: return 1;
        case OP_BETWEEN:     return 2;
        case OP_IN:          return 3;
        case OP_LESS_EQ:
        case OP_GREATER_EQ:
        case OP_LESS:
        case OP_GREATER:     return 4;
        default:             return 9;
    }
}

/* Collect indexable AND-children whose ops are intersection-eligible, sorted
   by selectivity rank. Returns the count (≥2 → intersection plan viable).

   Stage 1 restriction: ALL AND children must be eligible+indexed. Mixed
   trees (e.g., one LIKE leaf alongside indexed eq/range leaves) stay on
   PRIMARY_LEAF where the existing path post-filters via criteria_match_tree.
   Stage 2+ will lift this to feed intersection survivors into the post-filter
   pipeline for mixed trees. */
static int find_intersect_leaves(CriteriaNode *root,
                                 const char *db_root, const char *object,
                                 SearchCriterion *out_leaves[MAX_INTERSECT_LEAVES],
                                 char out_paths[MAX_INTERSECT_LEAVES][PATH_MAX]) {
    if (!root || root->kind != CNODE_AND) return 0;
    if (root->n_children < 2 || root->n_children > MAX_INTERSECT_LEAVES) return 0;
    int n = 0;
    for (int i = 0; i < root->n_children; i++) {
        CriteriaNode *c = root->children[i];
        if (c->kind != CNODE_LEAF) return 0;
        if (!op_eligible_for_intersect(c->leaf.op)) return 0;
        char path[PATH_MAX];
        if (!leaf_is_indexed(&c->leaf, db_root, object, path, sizeof(path))) return 0;
        out_leaves[n] = &c->leaf;
        memcpy(out_paths[n], path, PATH_MAX);
        n++;
    }
    if (n < 2) return 0;
    /* Insertion sort by selectivity rank — n is tiny (≤ MAX_INTERSECT_LEAVES). */
    for (int i = 1; i < n; i++) {
        for (int j = i; j > 0; j--) {
            int a = op_selectivity_rank(out_leaves[j]->op);
            int b = op_selectivity_rank(out_leaves[j-1]->op);
            if (a >= b) break;
            SearchCriterion *tl = out_leaves[j];
            out_leaves[j] = out_leaves[j-1];
            out_leaves[j-1] = tl;
            char tp[PATH_MAX];
            memcpy(tp, out_paths[j], PATH_MAX);
            memcpy(out_paths[j], out_paths[j-1], PATH_MAX);
            memcpy(out_paths[j-1], tp, PATH_MAX);
        }
    }
    return n;
}

static QueryPlan choose_primary_source(CriteriaNode *tree,
                                       const char *db_root, const char *object) {
    QueryPlan p = {0};
    if (!tree) { p.kind = PRIMARY_NONE; return p; }

    /* Try intersection first — if 2+ indexable AND-leaves on rangeable ops
       exist, intersecting candidate hashes is faster than primary-leaf +
       per-record post-filter for selective queries. */
    int ni = find_intersect_leaves(tree, db_root, object,
                                   p.intersect_leaves, p.intersect_paths);
    if (ni >= 2) {
        p.kind = PRIMARY_INTERSECT;
        p.intersect_count = ni;
        return p;
    }

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

/* ========== AND index-intersection (KeySet fast path) ==========

   For pure AND trees where every child is an indexed leaf on a btree-rangeable
   operator: walk each leaf's btree, intersect candidate hash sets via KeySet,
   skip the per-record fetch + criteria_match_tree loop entirely. Win compounds
   when the primary leaf has many matches but the intersection is small —
   today's primary-leaf path pays O(primary_matches) record fetches; this path
   pays O(sum of btree walks). */

typedef struct {
    KeySet *running;       /* KeySet from prior leaves (probe target) */
    KeySet *out;           /* survivors that hit `running` for this leaf */
    QueryDeadline *deadline;
    int dl_counter;
} IntersectProbeCtx;

/* btree callback for the second-and-later leaves: drop hashes that aren't
   already in `running`, surviving hashes go into `out`. */
static int intersect_probe_cb(const char *val, size_t vlen,
                              const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    IntersectProbeCtx *p = (IntersectProbeCtx *)ctx;
    if (query_deadline_tick(p->deadline, &p->dl_counter)) return -1;
    if (keyset_contains(p->running, hash16))
        keyset_insert(p->out, hash16);
    return 0;
}

/* Estimate KeySet capacity from index file sizes (summed across all shards).
   anchor count ≈ size / page, ~16 leaf entries per anchor block. Generous
   oversize is fine — KeySet is open-addressed and tolerates load factor
   up to 0.5 by construction. */
static size_t leaf_capacity_hint(const char *db_root, const char *object,
                                 const char *field, int splits) {
    int n = index_splits_for(splits);
    size_t total = 0;
    for (int s = 0; s < n; s++) {
        char p[PATH_MAX];
        build_idx_path(p, sizeof(p), db_root, object, field, s);
        struct stat st;
        if (stat(p, &st) == 0) total += (size_t)st.st_size;
    }
    if (total == 0) return 256;
    size_t hint = (total / 4096) * 16;
    if (hint < 256) hint = 256;
    if (hint > 10000000) hint = 10000000;
    return hint;
}

typedef struct {
    KeySet *ks;
    QueryDeadline *deadline;
    int dl_counter;
} IntersectCollectCtx;

/* btree callback for the first leaf: every hit drops into the seed KeySet. */
static int intersect_collect_cb(const char *val, size_t vlen,
                                const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    IntersectCollectCtx *c = (IntersectCollectCtx *)ctx;
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    keyset_insert(c->ks, hash16);
    return 0;
}

/* Walk the first leaf's btree into a fresh KeySet. */
static KeySet *build_keyset_from_leaf(const char *db_root, const char *object,
                                      int splits,
                                      SearchCriterion *leaf,
                                      QueryDeadline *dl) {
    KeySet *ks = keyset_new(leaf_capacity_hint(db_root, object, leaf->field, splits));
    if (!ks) return NULL;
    TypedSchema *ts = load_typed_schema(db_root, object);
    IntersectCollectCtx c = { ks, dl, 0 };
    btree_dispatch(db_root, object, leaf->field, splits,
                   leaf, resolve_idx_field(ts, leaf->field),
                   intersect_collect_cb, &c);
    if (dl->timed_out) { keyset_free(ks); return NULL; }
    return ks;
}

/* Below this threshold for the most-selective leaf, fan out the candidates as
   record fetches and post-filter via criteria_match_tree — cheaper than
   walking remaining indexed leaves' btrees in full just to confirm.

   Cost model (1M-row table, 1M-entry second leaf):
     N candidates, fetch+filter:           N × ~1.2µs
     full second-leaf btree walk + probe:  ~M × ~0.1µs
   Crossover at N ≈ M/12. 10000 is conservative-correct: falls back when
   fallback is clearly faster, stays on intersection when both sides large. */
#define INTERSECT_MIN_PRIMARY 10000

/* Intersect N indexed leaves' candidate hash sets. Most-selective leaf walked
   first; subsequent walks probe the running set and accumulate survivors.

   When the first (most-selective) leaf returns < INTERSECT_MIN_PRIMARY hits,
   sets *out_small_primary=1 and returns the first leaf's KeySet — caller fans
   out those hashes as record fetches and applies the full tree as post-filter,
   which beats walking subsequent leaves' btrees in full.

   Caller frees the returned KeySet. */
static KeySet *intersect_indexed_leaves(const char *db_root, const char *object,
                                        int splits,
                                        SearchCriterion **leaves, int n,
                                        QueryDeadline *dl,
                                        int *out_small_primary) {
    *out_small_primary = 0;
    if (n < 2) return NULL;
    KeySet *running = build_keyset_from_leaf(db_root, object, splits, leaves[0], dl);
    if (!running || dl->timed_out) { keyset_free(running); return NULL; }

    /* Small-primary heuristic: skip the second-leaf btree walk; let the
       caller fan out a record-fetch + post-filter pass instead. */
    if (keyset_size(running) < INTERSECT_MIN_PRIMARY) {
        *out_small_primary = 1;
        return running;
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    for (int i = 1; i < n; i++) {
        size_t cur = keyset_size(running);
        if (cur == 0) break;  /* empty intersection — short-circuit */

        KeySet *next = keyset_new(cur);  /* intersection cardinality ≤ |running| */
        if (!next) { keyset_free(running); return NULL; }

        IntersectProbeCtx p = { running, next, dl, 0 };
        btree_dispatch(db_root, object, leaves[i]->field, splits,
                       leaves[i], resolve_idx_field(ts, leaves[i]->field),
                       intersect_probe_cb, &p);

        keyset_free(running);
        running = next;
        if (dl->timed_out) { keyset_free(running); return NULL; }
    }
    return running;
}

/* Iterate a KeySet, computing shard_id + start_slot from each hash, into a
   CollectedHash[] suitable for parallel_indexed_count / process_batch /
   parallel_indexed_agg. Caller frees *out_entries. */
typedef struct {
    CollectedHash *entries;
    size_t count;
    size_t cap;
    int splits;
} KeysetToBatchCtx;

static int keyset_to_batch_cb(const uint8_t hash[16], void *ctx) {
    KeysetToBatchCtx *kc = (KeysetToBatchCtx *)ctx;
    if (kc->count >= kc->cap) return -1;  /* shouldn't happen — cap = keyset_size */
    CollectedHash *e = &kc->entries[kc->count++];
    memcpy(e->hash, hash, 16);
    addr_from_hash(hash, kc->splits, &e->shard_id, &e->start_slot);
    return 0;
}

static int keyset_to_collected_hashes(KeySet *ks, int splits,
                                      CollectedHash **out_entries, size_t *out_count) {
    size_t cap = keyset_size(ks);
    if (cap == 0) { *out_entries = NULL; *out_count = 0; return 0; }
    CollectedHash *entries = malloc(cap * sizeof(CollectedHash));
    if (!entries) return -1;
    KeysetToBatchCtx kc = { entries, 0, cap, splits };
    keyset_iter(ks, keyset_to_batch_cb, &kc);
    *out_entries = entries;
    *out_count = kc.count;
    return 0;
}

/* count(intersection) — caller checks dl->timed_out for error reporting.
   Small-primary path: hand the first leaf's hashes off to parallel_indexed_count
   with the full tree as post-filter (cheaper than walking subsequent btrees). */
static size_t keyset_count_from_intersect(const char *db_root, const char *object,
                                          const Schema *sch, QueryPlan *plan,
                                          CriteriaNode *tree, FieldSchema *fs,
                                          QueryDeadline *dl) {
    int small_primary = 0;
    KeySet *result = intersect_indexed_leaves(db_root, object, sch->splits,
                                              plan->intersect_leaves,
                                              plan->intersect_count, dl,
                                              &small_primary);
    if (!result) return 0;

    if (!small_primary) {
        size_t n = keyset_size(result);
        keyset_free(result);
        return n;
    }

    /* Small first leaf: skip the second-leaf btree walk(s). Convert to a
       CollectedHash[] and feed parallel_indexed_count with the full tree. */
    CollectedHash *batch = NULL;
    size_t batch_count = 0;
    int rc = keyset_to_collected_hashes(result, sch->splits, &batch, &batch_count);
    keyset_free(result);
    if (rc != 0 || batch_count == 0) { free(batch); return 0; }
    size_t n = parallel_indexed_count(db_root, object, sch, batch,
                                      (int)batch_count, tree, fs, dl);
    free(batch);
    return n;
}

/* ========== OR index-union (KeySet fast path) ========== */

typedef struct {
    const char *db_root;
    const char *object;
    int splits;
    const SearchCriterion *leaf;
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
    TypedSchema *ts = load_typed_schema(w->db_root, w->object);
    btree_dispatch(w->db_root, w->object, w->leaf->field, w->splits,
                   (SearchCriterion *)w->leaf,
                   resolve_idx_field(ts, w->leaf->field),
                   or_collect_cb, w);
    return NULL;
}

/* Build a KeySet by unioning the index lookups of every child of `or_node`.
   Every child must be a LEAF with an index (verified by the planner).
   Caller owns the returned KeySet and must keyset_free() it.
   If the OR's estimated capacity would exceed the per-query buffer cap, returns
   NULL and sets *out_budget_exceeded = 1 when the pointer is non-NULL. */
static KeySet *build_or_keyset(const char *db_root, const char *object, int splits,
                               const CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    int n = or_node->n_children;
    if (n <= 0) return NULL;

    /* Estimate total candidate size by summing every shard file's size for
       each child's index field (very rough: 1 entry per 64 bytes of leaf
       storage). Floor at 1024, cap at 1M to keep memory bounded. */
    int idx_n = index_splits_for(splits);
    size_t est_total = 0;
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        for (int s = 0; s < idx_n; s++) {
            char p[PATH_MAX];
            build_idx_path(p, sizeof(p), db_root, object, c->leaf.field, s);
            struct stat st;
            if (stat(p, &st) == 0) est_total += (size_t)st.st_size / 64;
        }
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
    for (int i = 0; i < n; i++) {
        CriteriaNode *c = or_node->children[i];
        ctxs[i].db_root = db_root;
        ctxs[i].object = object;
        ctxs[i].splits = splits;
        ctxs[i].leaf = &c->leaf;
        ctxs[i].ks = ks;
        ctxs[i].deadline = dl;
    }

    /* Serial across OR children — each or_child_worker calls btree_dispatch
       which itself fans out across the per-shard btrees via parallel_for.
       Nesting parallel_for here would risk pool exhaustion / deadlock per
       parallel.c's contract. Sequential children × parallel shards-per-child
       gives the same total in-flight parallelism (~splits/4) as the prior
       parallel children × serial shards layout, with much higher parallelism
       per child. */
    for (int i = 0; i < n; i++) or_child_worker(&ctxs[i]);

    free(ctxs);
    return ks;
}

/* Read the record at `hash` from the object's shards. Writes the value payload
   start + length into *out_val, *out_len. Returns 0 on success, -1 not found.
   Caller holds the returned pointer only for the duration of the mmap lease. */
typedef struct {
    FcacheRead fc;            /* persistent shard mmap; fcache_release on free */
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

    /* Persistent ucache mapping — same as every other read worker. The
       previous open + mmap MAP_PRIVATE per call paid page-fault + close
       overhead per KeySet entry, swamping the actual record probe. */
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return -1;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask  = slots - 1;

    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)slot + p) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash, 16) != 0) continue;

        out->fc = fc;
        out->val = fc.map + zoneB_off(s, slots, sch->slot_size) + h->key_len;
        out->val_len = h->value_len;
        return 0;
    }
    fcache_release(fc);
    return -1;
}

static void release_record_read(KeysetRecordRead *r) {
    if (r && r->fc.map) { fcache_release(r->fc); r->fc.map = NULL; }
}

/* Emit records matching `tree` by iterating a KeySet built from the OR branch.
   Honours offset/limit at emit time. Excluded keys, joins, projections, and
   rows_fmt all mirror the behaviour of idx_find_parallel / adv_search_cb.
   Returns the number of rows printed. */
/* Walk a pre-built KeySet, fetch each record, optionally re-match against a
   tree, and emit per the requested format. Used by both OR-union and AND-
   intersection paths (the difference is just how the KeySet was built and
   whether rematch is needed). Caller owns the KeySet lifecycle. */
static int keyset_emit_find(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks,
                            CriteriaNode *tree_for_rematch,  /* NULL = skip */
                            ExcludedKeys *excluded, int offset, int limit,
                            const char **proj_fields, int proj_count,
                            FieldSchema *fs, int rows_fmt, char csv_delim,
                            JoinSpec *joins, int njoins, QueryDeadline *dl) {
    int need_rematch = (tree_for_rematch != NULL);
    CriteriaNode *tree = tree_for_rematch;
    (void)tree;  /* used only when need_rematch */

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

        /* Re-walk for key + value together via the persistent ucache. The
           prior open + mmap MAP_PRIVATE per KeySet hit was paying ~100µs
           page-fault overhead per emitted row. */
        int sid, slt;
        addr_from_hash(ks->keys[b], sch->splits, &sid, &slt);
        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, sid);
        FcacheRead fc = fcache_get_read(shard);
        if (!fc.map) continue;
        uint8_t *map = fc.map;
        uint32_t slots = fc.slots_per_shard;
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
                        /* Tabular row for joins. SB_APPEND keeps `pos` clamped
                           inside the 16K buffer if the row is unusually wide;
                           buf_driver_values / buf_join_values already cap their
                           own writes, so a clamp here is the safety net for the
                           literal-string segments. */
                        char buf[16384];
                        size_t pos = 0;
                        SB_APPEND(buf, pos, sizeof(buf), "[\"%s\"", keybuf);
                        size_t wrote = buf_driver_values(
                                            raw, fs,
                                            proj_count > 0 ? proj_fields : NULL,
                                            proj_count,
                                            buf + pos, sizeof(buf) - pos);
                        pos += wrote;
                        if (pos >= sizeof(buf)) pos = sizeof(buf) - 1;
                        for (int i = 0; i < njoins; i++) {
                            if (!jraws[i]) {
                                int ncols = joins[i].proj_count > 0
                                            ? joins[i].proj_count
                                            : (joins[i].remote_fs.ts
                                               ? joins[i].remote_fs.ts->nfields : 0);
                                if (joins[i].include_remote_key) ncols++;
                                for (int k = 0; k < ncols; k++)
                                    SB_APPEND(buf, pos, sizeof(buf), ",null");
                            } else {
                                wrote = buf_join_values(&joins[i], jraws[i],
                                                        buf + pos, sizeof(buf) - pos);
                                pos += wrote;
                                if (pos >= sizeof(buf)) pos = sizeof(buf) - 1;
                            }
                        }
                        SB_APPEND(buf, pos, sizeof(buf), "]");
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
        fcache_release(fc);
        if (!found) continue;
    }

    return printed;
}

/* OR index-union find: build KeySet from indexed-OR children, then emit. */
static int keyset_find_from_or(const char *db_root, const char *object,
                               const Schema *sch, CriteriaNode *tree,
                               CriteriaNode *or_node,
                               ExcludedKeys *excluded, int offset, int limit,
                               const char **proj_fields, int proj_count,
                               FieldSchema *fs, int rows_fmt, char csv_delim,
                               JoinSpec *joins, int njoins,
                               QueryDeadline *dl, int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl, out_budget_exceeded);
    if (!ks) return 0;
    if (dl->timed_out) { keyset_free(ks); return 0; }

    /* Pure-OR (root itself is the or_node) — every KeySet member matches,
       no need to re-evaluate the tree against each record. */
    int need_rematch = (tree != or_node && tree->kind != CNODE_OR);

    int rc = keyset_emit_find(db_root, object, sch, ks,
                              need_rematch ? tree : NULL,
                              excluded, offset, limit,
                              proj_fields, proj_count, fs, rows_fmt, csv_delim,
                              joins, njoins, dl);
    keyset_free(ks);
    return rc;
}

/* AND index-intersection find: build KeySet from intersection of N indexed
   leaves (most-selective first), then emit. When the first leaf is small,
   skip the second-leaf btree walks and pass the full tree as post-filter via
   keyset_emit_find. */
static int keyset_find_from_intersect(const char *db_root, const char *object,
                                      const Schema *sch, QueryPlan *plan,
                                      CriteriaNode *tree,
                                      ExcludedKeys *excluded, int offset, int limit,
                                      const char **proj_fields, int proj_count,
                                      FieldSchema *fs, int rows_fmt, char csv_delim,
                                      JoinSpec *joins, int njoins, QueryDeadline *dl) {
    int small_primary = 0;
    KeySet *ks = intersect_indexed_leaves(db_root, object, sch->splits,
                                          plan->intersect_leaves,
                                          plan->intersect_count, dl,
                                          &small_primary);
    if (!ks) return 0;
    /* Small primary: pass the full tree so emit re-checks every leaf via
       criteria_match_tree. Big primary: intersection already exact, skip rematch. */
    int rc = keyset_emit_find(db_root, object, sch, ks,
                              small_primary ? tree : NULL,
                              excluded, offset, limit,
                              proj_fields, proj_count, fs, rows_fmt, csv_delim,
                              joins, njoins, dl);
    keyset_free(ks);
    return rc;
}

/* Forward decl — defined below. */
static int agg_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *raw_ctx);

/* Run the aggregate pipeline over records keyed by an OR index-union KeySet.
   Tree-match is performed by agg_scan_cb itself (it checks ctx->tree), so
   both Shape C (pure OR) and hybrid (AND + OR) reach the correct records. */
/* Walk a pre-built KeySet and feed each record through agg_scan_cb. agg_scan_cb
   reads ctx->tree internally — caller nullifies tree to skip rematch (used by
   AND-intersection where the keyset is already exact). */
static void keyset_emit_agg(const char *db_root, const char *object,
                            const Schema *sch, KeySet *ks, void *agg_ctx,
                            QueryDeadline *dl) {
    int dl_counter = 0;
    for (size_t b = 0; b < ks->cap; b++) {
        if (query_deadline_tick(dl, &dl_counter)) break;
        if (ks->state[b] != 2) continue;

        int sid, slt;
        addr_from_hash(ks->keys[b], sch->splits, &sid, &slt);
        char shard[PATH_MAX];
        build_shard_path(shard, sizeof(shard), db_root, object, sid);
        FcacheRead fc = fcache_get_read(shard);
        if (!fc.map) continue;
        uint32_t slots = fc.slots_per_shard;
        uint32_t mask = slots - 1;
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)slt + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, ks->keys[b], 16) != 0) continue;
            const uint8_t *block = fc.map + zoneB_off(s, slots, sch->slot_size);
            agg_scan_cb(h, block, agg_ctx);
            break;
        }
        fcache_release(fc);
    }
}

static void keyset_agg_from_or(const char *db_root, const char *object,
                               const Schema *sch, void *agg_ctx,
                               CriteriaNode *or_node, QueryDeadline *dl,
                               int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl, out_budget_exceeded);
    if (!ks) return;
    if (dl->timed_out) { keyset_free(ks); return; }
    keyset_emit_agg(db_root, object, sch, ks, agg_ctx, dl);
    keyset_free(ks);
}

/* AND index-intersection aggregate: build KeySet from intersection, then walk.
   For the big-primary path agg_ctx->tree must be NULL (intersection exact;
   skip redundant rematch). For small-primary agg_ctx->tree must be the full
   original tree so agg_scan_cb post-filters via criteria_match_tree.
   Returns 1 if it took the small-primary path (caller already set tree=NULL
   and may want to know it was the fallback). */
static int keyset_agg_from_intersect(const char *db_root, const char *object,
                                     const Schema *sch, void *agg_ctx,
                                     QueryPlan *plan, CriteriaNode *full_tree,
                                     CriteriaNode **agg_ctx_tree_field,
                                     QueryDeadline *dl) {
    int small_primary = 0;
    KeySet *ks = intersect_indexed_leaves(db_root, object, sch->splits,
                                          plan->intersect_leaves,
                                          plan->intersect_count, dl,
                                          &small_primary);
    if (!ks) return 0;

    if (small_primary) {
        /* Restore tree so agg_scan_cb post-filters via criteria_match_tree.
           AggCtx lives further down the file — caller passes &ctx.tree to
           avoid a forward decl. */
        *agg_ctx_tree_field = full_tree;
    }
    keyset_emit_agg(db_root, object, sch, ks, agg_ctx, dl);
    keyset_free(ks);
    return small_primary;
}

/* Count records matching `tree` by iterating a KeySet built from the OR branch.
   For pure-OR trees (root IS the or_node), returns |KeySet| directly.
   For hybrid (AND + OR), re-matches each keyed record against the full tree. */
static size_t keyset_count_from_or(const char *db_root, const char *object,
                                   const Schema *sch, CriteriaNode *tree,
                                   CriteriaNode *or_node, FieldSchema *fs,
                                   QueryDeadline *dl, int *out_budget_exceeded) {
    KeySet *ks = build_or_keyset(db_root, object, sch->splits, or_node, dl, out_budget_exceeded);
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
    /* Concurrent shard workers all accumulate here — atomic increment so no
       mutex is needed on the scan hot path. */
    __atomic_fetch_add(&cc->count, 1, __ATOMIC_RELAXED);
    return 0;
}

int cmd_count(const char *db_root, const char *object, const char *criteria_json) {
    /* No criteria = O(1) from metadata */
    if (!criteria_json || criteria_json[0] == '\0') {
        int n = get_live_count(db_root, object);
        OUT("%d\n", n);
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
        OUT("%d\n", n);
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
        int check_primary = op_needs_check_primary(op);

        /* Single-leaf tree → inline btree count, no record fetch. */
        int is_single_leaf =
            (tree->kind == CNODE_LEAF) ||
            (tree->kind == CNODE_AND && tree->n_children == 1 &&
             tree->children[0]->kind == CNODE_LEAF);

        const TypedField *pc_tf = resolve_idx_field(fs.ts, pc->field);
        if (is_single_leaf && op_is_negatable(op)) {
            /* count(neg) = count(*) - count(pos). Big win for NEQ/NOT_IN
               where the positive match set is small; also the correct
               answer for NOT_EXISTS (the old default-branch path visited
               every indexed entry and returned count(exists) instead). */
            SearchCriterion pos = *pc;
            pos.op = op_invert(op);
            int pos_cp = op_needs_check_primary(pos.op);
            IdxCountCtx ic = { &pos, pos_cp, 0, &dl, 0 };
            btree_dispatch(db_root, object, pc->field, sch.splits,
                           &pos, pc_tf, idx_count_cb, &ic);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else {
                int total = get_live_count(db_root, object);
                size_t neg = ((size_t)total > ic.count) ? (size_t)total - ic.count : 0;
                OUT("%zu\n", neg);
            }
        } else if (is_single_leaf) {
            IdxCountCtx ic = { pc, check_primary, 0, &dl, 0 };
            btree_dispatch(db_root, object, pc->field, sch.splits,
                           pc, pc_tf, idx_count_cb, &ic);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%zu\n", ic.count);
        } else {
            CollectCtx cc;
            collect_ctx_init(&cc);
            cc.splits = sch.splits;
            cc.primary_crit = pc;
            cc.check_primary = check_primary;
            cc.deadline = &dl;
            btree_dispatch(db_root, object, pc->field, sch.splits,
                           pc, pc_tf, collect_hash_cb, &cc);

            if (cc.budget_exceeded) {
                OUT(QUERY_BUFFER_ERR);
                collect_ctx_destroy(&cc); free_criteria_tree(tree);
                return -1;
            }
            size_t count = parallel_indexed_count(db_root, object, &sch,
                                                  cc.entries, (int)cc.count,
                                                  tree, &fs, &dl);
            if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
            else OUT("%zu\n", count);
            collect_ctx_destroy(&cc);
        }
    } else if (plan.kind == PRIMARY_INTERSECT) {
        /* AND of indexed leaves on rangeable ops — intersect candidate hash
           sets via KeySet, no record fetch needed for count. */
        size_t count = keyset_count_from_intersect(db_root, object, &sch, &plan,
                                                   tree, &fs, &dl);
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("%zu\n", count);
    } else if (plan.kind == PRIMARY_KEYSET) {
        /* Shape C / hybrid: build KeySet from OR index-union. */
        int budget_exceeded = 0;
        size_t count = keyset_count_from_or(db_root, object, &sch, tree, plan.or_node,
                                            &fs, &dl, &budget_exceeded);
        if (budget_exceeded) OUT(QUERY_BUFFER_ERR);
        else if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("%zu\n", count);
    } else {
        CountCtx ctx = { tree, &fs, 0, &dl, 0 };
        scan_shards(data_dir, sch.slot_size, count_scan_cb, &ctx);
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else OUT("%d\n", ctx.count);
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
    /* Lock-free reads (line 7959 fast-skip in the per-record callback;
       line 8553 post-scan check) intentionally don't take oc->lock — _Atomic
       gives them a torn-read-free view against the writers at lines 7985, 7998
       which set the flag under oc->lock. */
    _Atomic int budget_exceeded;
    pthread_mutex_t lock;
} OrderedCollectCtx;

static int ordered_collect_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    OrderedCollectCtx *oc = (OrderedCollectCtx *)ctx;
    /* coverity[lock_evasion] coverity[missing_lock] intentional fast-skip —
       `_Atomic int` makes the read torn-read-free; if stale, the locked
       re-check at line 7984 (`if (oc->buffer_bytes + row_bytes >
       g_query_buffer_max_bytes)`) catches it. Per-callback mutex acquire
       would gate every record. */
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
        int new_cap = oc->cap ? oc->cap * 2 : 256;
        OrderedRow *t = xrealloc_or_free(oc->rows, (size_t)new_cap * sizeof(OrderedRow));
        if (!t) {
            oc->rows = NULL;
            oc->count = 0;
            oc->cap = 0;
            oc->budget_exceeded = 1;
            pthread_mutex_unlock(&oc->lock);
            free(sv);
            return 1;
        }
        oc->rows = t;
        oc->cap = new_cap;
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

/* ========== Find cursor (keyset pagination) ==========
   Client submits the cursor from the previous page's response as
       "cursor": {"<order_by>": "<value>", "key": "<primary_key>"}
   and we seek past that position in the order_by field's btree. Within a
   run of equal order_by values, we tie-break on hash16(primary_key) so
   pagination is stable even if the btree has many records sharing the
   same order_by value.

   Shape constraints (locked 2026-04-24 in cursor_design.md):
   - transparent JSON, not opaque blob
   - indexed order_by is hard-required; reject if not
   - single-field order_by only; multi-field composite indexes are for
     filter acceleration, not pagination
   - strict shape validation at parse; cursor contents are NOT validated
     against live data (stale cursors just seek to the last-known byte
     position, matching standard keyset semantics) */
typedef struct {
    int    present;
    char   value[1024];      /* textual value of the order_by field */
    size_t vlen;
    char   key[1024];        /* primary key string */
    size_t klen;
} FindCursor;

/* Return codes:
   0 with out->present=0 — "cursor" key present but empty/null (page 1 of
                           cursor pagination: use wrapper response, walk
                           from start, emit initial cursor)
   0 with out->present=1 — cursor object populated (subsequent page)
   -1                     — malformed cursor content (error message in *err)
   1                      — cursor key entirely absent (not a cursor query;
                           caller uses the regular find path) */
static int parse_cursor_object(const char *cursor_json, const char *order_by,
                               FindCursor *out, const char **err) {
    out->present = 0;
    out->vlen = 0;
    out->klen = 0;
    if (!cursor_json || !cursor_json[0]) return 1;

    /* Opt-in-to-cursor-pagination shapes: "null" or "{}" → page 1. */
    const char *p = json_skip(cursor_json);
    if (strncmp(p, "null", 4) == 0) return 0;        /* page 1, no position */
    if (*p != '{') { *err = "cursor must be a JSON object"; return -1; }

    JsonObj c;
    json_parse_object(cursor_json, strlen(cursor_json), &c);

    /* Detect empty object {}. json_obj_strdup returns NULL for both empty
       object and missing key; we distinguish by walking raw JSON. Treat
       {} as page 1 (no position). */
    int saw_any_pair = 0;
    for (const char *q = p + 1; *q; q++) {
        if (*q == '}') break;
        if (*q != ' ' && *q != '\t' && *q != '\n') { saw_any_pair = 1; break; }
    }
    if (!saw_any_pair) return 0;                     /* page 1, empty object */

    char *kv = json_obj_strdup(&c, "key");
    if (!kv || !kv[0]) { free(kv); *err = "cursor missing 'key' field"; return -1; }
    size_t klen = strlen(kv);
    if (klen >= sizeof(out->key)) { free(kv); *err = "cursor key too long"; return -1; }
    memcpy(out->key, kv, klen + 1);
    out->klen = klen;
    free(kv);

    if (!order_by || !order_by[0]) {
        *err = "cursor requires order_by";
        return -1;
    }
    char *vv = json_obj_strdup(&c, order_by);
    if (!vv) { *err = "cursor missing order_by field value"; return -1; }
    size_t vlen = strlen(vv);
    if (vlen >= sizeof(out->value)) { free(vv); *err = "cursor value too long"; return -1; }
    memcpy(out->value, vv, vlen + 1);
    out->vlen = vlen;
    free(vv);

    out->present = 1;
    return 0;
}

/* Per-callback state for the cursor-driven btree walk. */
typedef struct {
    /* Walk bounds */
    const uint8_t *cursor_value_bytes;  /* NULL on page 1 (no cursor) */
    size_t         cursor_value_len;
    uint8_t        cursor_hash16[16];   /* derived from cursor.key */
    int            has_cursor;
    int            desc;                /* 0=ASC, 1=DESC */

    /* Record fetch context */
    const char    *db_root;
    const char    *object;
    const Schema  *sch;
    FieldSchema   *fs;
    CriteriaNode  *remaining;           /* full criteria tree; order_by leaf stays in
                                           because order_by leaf is either equality
                                           (trivially satisfied by a cursor walk that
                                           already visits matching btree entries) or
                                           a range that we still want enforced. */

    /* Emission */
    const char   **proj_fields;
    int            proj_count;
    int            rows_fmt;
    int            dict_fmt;
    int            limit;
    int            printed;

    /* order_by resolution */
    const TypedField *order_tf;         /* for value-str extraction */
    int               order_field_idx;

    /* Captured last-emitted cursor (raw bytes). Heap-owned, freed by caller. */
    char          *last_value_str;
    char          *last_key_str;

    QueryDeadline *deadline;
    int            dl_counter;
} CursorFindCtx;

static int cursor_find_cb(const char *val, size_t vlen,
                          const uint8_t *hash16, void *ctx) {
    CursorFindCtx *c = (CursorFindCtx *)ctx;
    if (query_deadline_tick(c->deadline, &c->dl_counter)) return -1;
    if (c->printed >= c->limit) return -1;

    /* Cursor tiebreak. Within a run of identical order_by values the btree
       physical order is hash16-based (unstable w.r.t. insertion order, but
       deterministic per-tree), so "strictly after cursor" means value >
       cursor.value OR (value == cursor.value AND hash16 > cursor.hash16).
       Mirror-flip for DESC. */
    if (c->has_cursor) {
        /* Length-aware compare: memcmp up to min length, then length tiebreak. */
        size_t mlen = vlen < c->cursor_value_len ? vlen : c->cursor_value_len;
        int vcmp = memcmp(val, c->cursor_value_bytes, mlen);
        if (vcmp == 0) {
            if (vlen < c->cursor_value_len) vcmp = -1;
            else if (vlen > c->cursor_value_len) vcmp = 1;
        }
        if (!c->desc) {
            if (vcmp < 0) return 0;                     /* shouldn't happen with range bounds */
            if (vcmp == 0) {
                int hcmp = memcmp(hash16, c->cursor_hash16, 16);
                if (hcmp <= 0) return 0;                /* at or before cursor → skip */
            }
        } else {
            if (vcmp > 0) return 0;                     /* shouldn't happen */
            if (vcmp == 0) {
                int hcmp = memcmp(hash16, c->cursor_hash16, 16);
                if (hcmp >= 0) return 0;
            }
        }
    }

    /* Resolve hash16 → (shard, slot) and probe the shard for the record. */
    int shard_id, start_slot;
    addr_from_hash(hash16, c->sch->splits, &shard_id, &start_slot);
    char shard_path[PATH_MAX];
    build_shard_path(shard_path, sizeof(shard_path), c->db_root, c->object, shard_id);
    FcacheRead fc = fcache_get_read(shard_path);
    if (!fc.map) return 0;

    uint32_t slots = fc.slots_per_shard;
    uint32_t mask  = slots - 1;
    int slot = -1;
    for (uint32_t p = 0; p < slots; p++) {
        uint32_t s = ((uint32_t)start_slot + p) & mask;
        SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
        if (h->flag == 0 && h->key_len == 0) break;
        if (h->flag != 1) continue;
        if (memcmp(h->hash, hash16, 16) == 0) { slot = (int)s; break; }
    }
    if (slot < 0) { fcache_release(fc); return 0; }

    SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(slot));
    const uint8_t *key_start = fc.map + zoneB_off(slot, slots, c->sch->slot_size);
    const uint8_t *raw = key_start + h->key_len;

    /* Remaining criteria (full tree). Order_by leaf still gets checked — it
       stays correct because the btree walk only visits entries that would
       pass a range/eq on the indexed field, and range checks in the tree
       agree with the range in the walk. */
    if (c->remaining && !criteria_match_tree(raw, c->remaining, c->fs)) {
        fcache_release(fc);
        return 0;
    }

    /* Emit row. Supports json-default and rows_fmt. */
    char key_buf[1024];
    size_t klen = h->key_len < sizeof(key_buf) - 1 ? h->key_len : sizeof(key_buf) - 1;
    memcpy(key_buf, key_start, klen);
    key_buf[klen] = '\0';

    if (c->rows_fmt) {
        OUT("%s[\"%s\"", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = decode_field((const char *)raw, h->value_len,
                                        c->proj_fields[i], c->fs);
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        } else if (c->fs && c->fs->ts) {
            for (int i = 0; i < c->fs->ts->nfields; i++) {
                if (c->fs->ts->fields[i].removed) continue;
                char *pv = typed_get_field_str(c->fs->ts, raw, i);
                OUT(",\"%s\"", pv ? pv : "");
                free(pv);
            }
        }
        OUT("]");
    } else if (c->dict_fmt) {
        OUT("%s\"%s\":", c->printed ? "," : "", key_buf);
        if (c->proj_count > 0) {
            OUT("{");
            int first = 1;
            for (int i = 0; i < c->proj_count; i++) {
                char *pv = decode_field((const char *)raw, h->value_len,
                                        c->proj_fields[i], c->fs);
                if (!pv) continue;
                OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
                first = 0;
                free(pv);
            }
            OUT("}");
        } else {
            char *dv = decode_value((const char *)raw, h->value_len, c->fs);
            OUT("%s", dv ? dv : "{}");
            free(dv);
        }
    } else if (c->proj_count > 0) {
        OUT("%s{\"key\":\"%s\",\"value\":{", c->printed ? "," : "", key_buf);
        int first = 1;
        for (int i = 0; i < c->proj_count; i++) {
            char *pv = decode_field((const char *)raw, h->value_len,
                                    c->proj_fields[i], c->fs);
            if (!pv) continue;
            OUT("%s\"%s\":\"%s\"", first ? "" : ",", c->proj_fields[i], pv);
            first = 0;
            free(pv);
        }
        OUT("}}");
    } else {
        char *dv = decode_value((const char *)raw, h->value_len, c->fs);
        OUT("%s{\"key\":\"%s\",\"value\":%s}",
            c->printed ? "," : "", key_buf, dv ? dv : "{}");
        free(dv);
    }

    /* Capture this row's (order_by_value, key) as the next-page cursor. Each
       emit overwrites, so after the walk the stored pair is the last row
       emitted — which is exactly what we send back. */
    free(c->last_value_str);
    free(c->last_key_str);
    /* When order_tf is non-NULL it was resolved from c->fs->ts, so both
       pointers are non-NULL here in practice — the explicit checks mirror
       the defensive pattern at the OrderedCollectCtx callback (line 8000)
       and silence Coverity's flow-insensitive FORWARD_NULL on c->fs. */
    c->last_value_str = (c->order_tf && c->fs && c->fs->ts)
        ? typed_get_field_str(c->fs->ts, raw, c->order_field_idx)
        : NULL;
    c->last_key_str = strndup(key_buf, klen);

    c->printed++;
    fcache_release(fc);
    return 0;
}

int cmd_find(const char *db_root, const char *object,
                    const char *criteria_json, int offset, int limit,
                    const char *proj_str, const char *excluded_csv,
                    const char *format, const char *delimiter,
                    const char *join_json,
                    const char *order_by, const char *order_dir,
                    const char *cursor_json) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
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

    if (dict_fmt && has_joins) {
        OUT("{\"error\":\"format=dict is not supported with join\"}\n");
        free_joins(joins, njoins);
        return -1;
    }

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

    /* ===== CURSOR PATH (keyset pagination) =====
       If the request carries "cursor": {...}, drive the walk off the
       order_by field's btree directly, skipping the buffer-sort path. */
    if (cursor_json && cursor_json[0]) {
        const char *cerr = NULL;
        FindCursor cur;
        int cr = parse_cursor_object(cursor_json, order_by, &cur, &cerr);
        if (cr < 0) {
            OUT("{\"error\":\"%s\"}\n", cerr ? cerr : "invalid cursor");
            free_joins(joins, njoins); free_criteria_tree(tree); free_excluded(&excluded);
            return -1;
        }
        if (!order_by || !order_by[0]) {
            OUT("{\"error\":\"cursor requires order_by\"}\n");
            free_joins(joins, njoins); free_criteria_tree(tree); free_excluded(&excluded);
            return -1;
        }
        if (has_joins) {
            OUT("{\"error\":\"cursor with join is not supported\"}\n");
            free_joins(joins, njoins); free_criteria_tree(tree); free_excluded(&excluded);
            return -1;
        }
        if (csv_delim) {
            OUT("{\"error\":\"cursor with format=csv is not supported\"}\n");
            free_joins(joins, njoins); free_criteria_tree(tree); free_excluded(&excluded);
            return -1;
        }

        /* order_by must be indexed (hard requirement for cursor). */
        if (!btree_idx_exists(db_root, object, order_by, sch.splits)) {
            OUT("{\"error\":\"cursor requires order_by field to be indexed\",\"field\":\"%s\"}\n",
                order_by);
            free_joins(joins, njoins); free_criteria_tree(tree); free_excluded(&excluded);
            return -1;
        }

        int desc = (order_dir && (strcmp(order_dir, "desc") == 0 ||
                                   strcmp(order_dir, "DESC") == 0));

        /* Resolve order_by's TypedField for encoding + value extraction. */
        const TypedField *order_tf = NULL;
        int order_field_idx = -1;
        if (driver_fs.ts) {
            for (int i = 0; i < driver_fs.ts->nfields; i++) {
                if (strcmp(driver_fs.ts->fields[i].name, order_by) == 0) {
                    order_tf = &driver_fs.ts->fields[i];
                    order_field_idx = i;
                    break;
                }
            }
        }

        /* Encode cursor value bytes for walk bounds. If cursor absent (page 1),
           walk from start (ASC) or end (DESC); else walk from cursor position,
           with tiebreak happening inside the callback. */
        uint8_t cur_value_buf[1024];
        size_t  cur_value_len = 0;
        int     has_cur_bytes = 0;
        if (cur.present) {
            if (order_tf) {
                encode_field_for_index(order_tf, cur.value, cur.vlen,
                                       cur_value_buf, &cur_value_len);
            } else {
                /* Composite/unknown — raw bytes. */
                size_t cap = sizeof(cur_value_buf);
                cur_value_len = cur.vlen < cap ? cur.vlen : cap;
                memcpy(cur_value_buf, cur.value, cur_value_len);
            }
            has_cur_bytes = 1;
        }

        /* Derive cursor hash16 from the primary key for tiebreak. */
        QueryDeadline cdl = { now_ms_coarse(), resolve_timeout_ms(), 0 };
        CursorFindCtx cc;
        memset(&cc, 0, sizeof(cc));
        cc.cursor_value_bytes = has_cur_bytes ? cur_value_buf : NULL;
        cc.cursor_value_len   = cur_value_len;
        cc.has_cursor         = cur.present;
        cc.desc               = desc;
        if (cur.present) {
            compute_hash_raw(cur.key, cur.klen, cc.cursor_hash16);
        }
        cc.db_root = db_root;
        cc.object  = object;
        cc.sch     = &sch;
        cc.fs      = &driver_fs;
        cc.remaining = tree;            /* full tree; filters apply per record */
        cc.proj_fields = proj_count > 0 ? proj_fields : NULL;
        cc.proj_count  = proj_count;
        cc.rows_fmt    = rows_fmt;
        cc.dict_fmt    = dict_fmt;
        cc.limit       = limit;
        cc.printed     = 0;
        cc.order_tf    = order_tf;
        cc.order_field_idx = order_field_idx;
        cc.deadline    = &cdl;

        /* Cursor response always uses the {rows:..., cursor:...} wrapper so
           clients get a single stable shape regardless of format. dict_fmt
           swaps the inner array for an object. */
        OUT(dict_fmt ? "{\"rows\":{" : "{\"rows\":[");
        if (desc) {
            /* DESC: walk backward. Upper bound = cursor value (inclusive),
               lower bound = "". Ties still handled in the callback. */
            const char *max_val_bytes = has_cur_bytes
                ? (const char *)cur_value_buf : "\xff\xff\xff\xff";
            size_t max_val_len = has_cur_bytes ? cur_value_len : 4;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   "", 0, 0,
                                   max_val_bytes, max_val_len, 0,
                                   1, cursor_find_cb, &cc);
        } else {
            /* ASC: walk forward. Lower bound = cursor value (inclusive),
               upper bound = "\xff\xff\xff\xff". Ties handled in callback. */
            const char *min_val_bytes = has_cur_bytes
                ? (const char *)cur_value_buf : "";
            size_t min_val_len = has_cur_bytes ? cur_value_len : 0;
            btree_idx_walk_ordered(db_root, object, order_by, sch.splits,
                                   min_val_bytes, min_val_len, 0,
                                   "\xff\xff\xff\xff", 4, 0,
                                   0, cursor_find_cb, &cc);
        }
        OUT(dict_fmt ? "}" : "]");

        /* Emit next-page cursor if we actually hit the limit (there might be
           more). If printed < limit the walk drained to the end → null. */
        if (cc.printed >= limit && cc.last_value_str && cc.last_key_str) {
            OUT(",\"cursor\":{\"%s\":\"%s\",\"key\":\"%s\"}}",
                order_by, cc.last_value_str, cc.last_key_str);
        } else {
            OUT(",\"cursor\":null}");
        }
        OUT("\n");

        free(cc.last_value_str);
        free(cc.last_key_str);
        free_joins(joins, njoins);
        free_criteria_tree(tree);
        free_excluded(&excluded);
        return 0;
    }

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

    /* dict_fmt threading is complete for full-scan + ordered + cursor; the
       indexed planner paths (PRIMARY_LEAF / PRIMARY_INTERSECT / PRIMARY_KEYSET)
       still emit {key,value} records via process_batch and the keyset emit
       helpers. Reject dict_fmt when those paths would fire so we never
       produce malformed JSON. Tracked for follow-up; threading is mechanical
       but touches several helpers. has_order && !has_joins → ordered path,
       which is dict-aware. */
    if (dict_fmt && !has_joins && !(order_by && order_by[0])) {
        if (plan.kind == PRIMARY_LEAF || plan.kind == PRIMARY_INTERSECT ||
            plan.kind == PRIMARY_KEYSET) {
            OUT("{\"error\":\"format=dict with indexed criteria is not yet supported; use order_by, drop the criteria, or use default array format\"}\n");
            free_joins(joins, njoins);
            free_criteria_tree(tree);
            free_excluded(&excluded);
            return -1;
        }
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
    } else if (dict_fmt) {
        OUT("{");
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
            } else if (dict_fmt) {
                OUT("%s\"%s\":", printed ? "," : "", r->key);
                if (proj_count > 0) {
                    OUT("{");
                    int first = 1;
                    for (int j = 0; j < proj_count; j++) {
                        char *pv = decode_field((const char *)val, r->value_len, proj_fields[j], &driver_fs);
                        if (!pv) continue;
                        OUT("%s\"%s\":\"%s\"", first ? "" : ",", proj_fields[j], pv);
                        first = 0;
                        free(pv);
                    }
                    OUT("}");
                } else {
                    char *v = decode_value((const char *)val, r->value_len, &driver_fs);
                    OUT("%s", v);
                    free(v);
                }
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
    } else if (plan.kind == PRIMARY_INTERSECT) {
        /* ===== AND INDEX-INTERSECTION FIND ===== */
        keyset_find_from_intersect(db_root, object, &sch, &plan, tree,
                                   &excluded, offset, limit,
                                   proj_fields, proj_count,
                                   (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                                   rows_fmt, csv_delim, joins, njoins, &dl);
    } else if (plan.kind == PRIMARY_LEAF) {
        /* ===== INDEXED FIND: collect → group by shard → parallel process ===== */
        SearchCriterion *pc = plan.primary_leaf;
        enum SearchOp op = pc->op;
        int check_primary = op_needs_check_primary(op);
        int rc = idx_find_parallel(db_root, object, &sch, plan.primary_idx_path, tree,
                         pc, check_primary, &excluded, offset, limit,
                         proj_fields, proj_count,
                         (driver_fs.ts || driver_fs.nfields > 0) ? &driver_fs : NULL,
                         rows_fmt, csv_delim, joins, njoins, &dl);
        if (rc == -2) {
            if (csv_delim) { /* nothing to close */ }
            else if (has_joins || rows_fmt) OUT("]}\n");
            else if (dict_fmt) OUT("}\n");
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
            else if (dict_fmt) OUT("}\n");
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
                             rows_fmt, dict_fmt, csv_delim,
                             object, joins, njoins, db_root, &dl, 0,
                             PTHREAD_MUTEX_INITIALIZER };
        scan_shards(data_dir, sch.slot_size, adv_search_cb, &ctx);
        pthread_mutex_destroy(&ctx.lock);
    }

    if (has_joins)
        OUT("]}\n");
    else if (csv_delim)
        { /* CSV body already ends with its own \n per row — nothing to close */ }
    else if (rows_fmt)
        OUT("]}\n");
    else if (dict_fmt)
        OUT("}\n");
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
        if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
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
        if (lockfd < 0) {
            OUT("{\"error\":\"sequence: open(%s) failed: %s\"}\n", lock_path, strerror(errno));
            return 1;
        }
        flock(lockfd, LOCK_EX);

        long long val = 0;
        FILE *f = fopen(seq_path, "r");
        if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }

        if (batch_size <= 1) {
            val++;
            f = fopen(seq_path, "w");
            if (!f) {
                flock(lockfd, LOCK_UN); close(lockfd);
                OUT("{\"error\":\"sequence: fopen(%s) failed: %s\"}\n", seq_path, strerror(errno));
                return 1;
            }
            fprintf(f, "%lld\n", val);
            fclose(f);
            flock(lockfd, LOCK_UN);
            close(lockfd);
            OUT("{\"sequence\":\"%s\",\"value\":%lld}\n", seq_name, val);
        } else {
            long long start = val + 1;
            val += batch_size;
            f = fopen(seq_path, "w");
            if (!f) {
                flock(lockfd, LOCK_UN); close(lockfd);
                OUT("{\"error\":\"sequence: fopen(%s) failed: %s\"}\n", seq_path, strerror(errno));
                return 1;
            }
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

/* Copy one regular file. Preserves mode bits. Returns 0 on success. */
static int copy_file(const char *src, const char *dst, mode_t mode) {
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) return -1;
    int dfd = open(dst, O_WRONLY | O_CREAT | O_TRUNC, mode & 0777);
    if (dfd < 0) { close(sfd); return -1; }
    char buf[64 * 1024];
    ssize_t n;
    int rc = 0;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) {
        size_t total = (size_t)n;     /* n > 0 here; bounded by sizeof(buf) */
        size_t off = 0;
        while (off < total) {
            /* Loop guard `off < total` (both size_t) makes total-off > 0
               and well within size_t range — the only writer to off is
               `off += (size_t)w` where w >= 0 (the w<0 path goto-dones).
               Coverity's INTEGER_OVERFLOW heuristic chains taintedness
               from read()'s ssize_t return, so it doesn't trust the
               cast+invariant chain. The arithmetic is safe by construction.
               coverity[overflow_sink] total - off is unsigned-positive */
            ssize_t w = write(dfd, buf + off, total - off);
            if (w < 0) { rc = -1; goto done; }
            off += (size_t)w;
        }
    }
    if (n < 0) rc = -1;
done:
    close(sfd);
    close(dfd);
    return rc;
}

/* Recursively copy a directory tree. No shell, no path interpolation —
   every path is built component-wise, so the source-object name (which
   originates from a JSON request) cannot inject shell metacharacters.
   Replaces the previous `system("cp -r ...")` invocations that CodeQL
   flagged as "uncontrolled data used in OS command". */
static void cprf(const char *src, const char *dst) {
    /* TOCTOU-safe: open by path once, then fstat the fd and fdopendir the
       same fd. Avoids the classic lstat-then-opendir race where a symlink
       swap between the two calls would steer the recursive copy at a
       different filesystem object. O_NOFOLLOW rejects symlinks at the
       open boundary, which is consistent with the existing "skip symlinks"
       contract documented below. */
    int fd = open(src, O_RDONLY | O_NOFOLLOW);
    if (fd < 0) return;
    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return; }

    if (S_ISDIR(st.st_mode)) {
        mkdirp(dst);
        DIR *d = fdopendir(fd);          /* takes ownership of fd */
        if (!d) { close(fd); return; }
        struct dirent *e;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.' &&
                (e->d_name[1] == '\0' ||
                 (e->d_name[1] == '.' && e->d_name[2] == '\0')))
                continue;
            char child_src[PATH_MAX], child_dst[PATH_MAX];
            snprintf(child_src, sizeof(child_src), "%s/%s", src, e->d_name);
            snprintf(child_dst, sizeof(child_dst), "%s/%s", dst, e->d_name);
            cprf(child_src, child_dst);
        }
        closedir(d);                      /* closes the underlying fd */
    } else if (S_ISREG(st.st_mode)) {
        close(fd);                        /* copy_file opens its own fd */
        copy_file(src, dst, st.st_mode);
    } else {
        close(fd);
    }
    /* Symlinks, fifos, devices etc. are ignored — backup targets are
       always shard-db's own regular files / directories. */
}

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

    /* Recursively copy data/, indexes/, and metadata/ via in-process
       directory walk — no shell. */
    char src_sub[PATH_MAX], dst_sub[PATH_MAX];
    const char *subs[] = { "data", "indexes", "metadata" };
    for (size_t i = 0; i < sizeof(subs) / sizeof(subs[0]); i++) {
        snprintf(src_sub, sizeof(src_sub), "%s/%s", src_dir, subs[i]);
        snprintf(dst_sub, sizeof(dst_sub), "%s/%s", bak_dir, subs[i]);
        cprf(src_sub, dst_sub);
    }

    OUT("{\"status\":\"backed_up\",\"path\":\"%s\"}\n", bak_dir);
    return 0;
}

/* ========== RECOUNT ========== */

/* Recount uses its own parallel scan — historically a duplicate of
   scan_shards that skipped the global output-serialization mutex.
   scan_shards is now lock-free too (callbacks carry their own internal
   sync), so this parallel variant is structurally identical. Kept as-is
   to avoid churn; could be collapsed into scan_shards later. */

typedef struct {
    const char *path;
    int slot_size;
    int *counter; /* shared atomic counter */
} RecountWorkerArg;

static void *recount_worker(void *arg) {
    RecountWorkerArg *w = (RecountWorkerArg *)arg;
    int local = 0;
    /* Use the persistent ucache so recount sees a consistent snapshot — a
       concurrent insert/delete takes the per-shard wrlock and blocks
       briefly per shard rather than racing against this MAP_PRIVATE view.
       The MADV_DONTNEED-after-scan trick is gone with the cache: ucache
       pins the mapping, the page-cache LRU naturally evicts cold pages
       after the scan is over, and the OS's implicit readahead handles
       sequential access without an explicit MADV_SEQUENTIAL hint. */
    FcacheRead fc = fcache_get_read(w->path);
    if (!fc.map) return NULL;
    uint8_t *map = fc.map;
    uint32_t shard_slots = fc.slots_per_shard;
    if (fc.size < shard_zoneA_end(shard_slots)) { fcache_release(fc); return NULL; }
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
    fcache_release(fc);
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
    if (!rows) { OUT("{\"error\":\"oom\"}\n"); return 1; }
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
        if (nrows >= cap) {
            cap *= 2;
            Row *t = xrealloc_or_free(rows, cap * sizeof(*t));
            if (!t) { rows = NULL; nrows = 0; break; }
            rows = t;
        }
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

    /* Hint: sizing is driven by records-per-shard, not by load factor or doublings.
       Bench sweet spot ≈ 78K-200K rec/shard; acceptable up to ~500K; degradation
       past ~1M. `grows` is informational only — every real workload doubles many
       times past INITIAL_SLOTS to stay under the 50% growth threshold, so it
       doesn't distinguish "optimal" from "overloaded". */
    const char *hint = NULL;
    double avg_load = 0.0;
    uint64_t rps = 0;
    if (nrows > 0) {
        avg_load = (double)total_records / ((double)max_slots * nrows);
        rps = total_records / (uint64_t)nrows;
        if (rps > 1000000ULL) {
            hint = (sch.splits < MAX_SPLITS)
                ? "records-per-shard past sweet spot (>1M) — re-split with vacuum --splits=N"
                : "at MAX_SPLITS with >1M records/shard — performance may degrade; consider partitioning across objects";
        } else if (rps > 500000ULL && sch.splits < MAX_SPLITS) {
            hint = "records-per-shard approaching upper band (>500K) — consider vacuum --splits=N";
        } else if (min_records > 0 && max_records > min_records * 4) {
            hint = "shard load is skewed — check key distribution";
        }
    }

    if (as_table) {
        OUT("splits=%d shards_on_disk=%d total_records=%lu total_bytes=%lu avg_rec_per_shard=%lu max_grows=%d avg_load=%.3f\n",
            sch.splits, nrows, (unsigned long)total_records, (unsigned long)total_bytes,
            (unsigned long)rps, grows, avg_load);
        if (nrows != sch.splits)
            OUT("warn: shards_on_disk (%d) ≠ splits (%d) — partial vacuum/reshard or missing shard files?\n",
                nrows, sch.splits);
        OUT("  %-8s %-10s %-10s %-8s %-14s\n", "shard", "slots", "records", "load", "bytes");
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("  %-8d %-10u %-10u %-8.3f %-14lu\n",
                rows[i].shard_id, rows[i].slots, rows[i].records, load,
                (unsigned long)rows[i].file_bytes);
        }
        if (hint) OUT("hint: %s\n", hint);
    } else {
        /* JSON: keep `splits` as the configured count from schema.conf and
           rename the on-disk count to `shards_on_disk` so the two are
           clearly distinct sources rather than identical-looking duplicates.
           Drift between them signals a partial vacuum/reshard or missing
           shard files — operators should investigate. */
        OUT("{\"splits\":%d,\"shards_on_disk\":%d,\"total_records\":%lu,\"total_bytes\":%lu,\"shard_list\":[",
            sch.splits, nrows, (unsigned long)total_records, (unsigned long)total_bytes);
        for (int i = 0; i < nrows; i++) {
            double load = rows[i].slots ? (double)rows[i].records / (double)rows[i].slots : 0.0;
            OUT("%s{\"shard\":%d,\"slots\":%u,\"records\":%u,\"load\":%.3f,\"bytes\":%lu}",
                i ? "," : "", rows[i].shard_id, rows[i].slots, rows[i].records,
                load, (unsigned long)rows[i].file_bytes);
        }
        OUT("],\"avg_rec_per_shard\":%lu,\"max_grows\":%d", (unsigned long)rps, grows);
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
    if (!paths) { OUT("{\"error\":\"oom\"}\n"); return 1; }

    DIR *d1 = opendir(data_dir);
    if (!d1) { free(paths); OUT("{\"count\":0}\n"); set_count(db_root, object, 0); return 0; }
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (e1->d_name[0] == '.') continue;
        size_t nlen = strlen(e1->d_name);
        if (nlen < 5 || strcmp(e1->d_name + nlen - 4, ".bin") != 0) continue;
        if (path_count >= path_cap) {
            path_cap *= 2;
            /* Plain realloc + walk: paths[] holds strdup'd entries that
               xrealloc_or_free's atomic free would orphan. */
            char **t = realloc(paths, path_cap * sizeof(char *));
            if (!t) {
                for (int k = 0; k < path_count; k++) free(paths[k]);
                free(paths);
                paths = NULL;
                path_count = 0;
                break;
            }
            paths = t;
        }
        char bp[PATH_MAX];
        snprintf(bp, sizeof(bp), "%s/%s", data_dir, e1->d_name);
        paths[path_count++] = strdup(bp);
    }
    closedir(d1);

    int total = 0;
    if (path_count > 0) {
        RecountWorkerArg *args = malloc(path_count * sizeof(RecountWorkerArg));
        if (!args) {
            for (int i = 0; i < path_count; i++) free(paths[i]);
            free(paths);
            OUT("{\"error\":\"oom\"}\n");
            return 1;
        }
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
    /* Open the source first, then fstat the fd. CodeQL flagged the prior
       stat()-then-open() as a TOCTOU race: an attacker controlling the
       directory containing `src` could swap the file between the two
       syscalls. The fd binds us to a specific inode for the rest of the
       function. */
    int sfd = open(src, O_RDONLY);
    if (sfd < 0) {
        fprintf(stderr, "Error: Source file %s not found\n", src);
        return 1;
    }
    struct stat st;
    if (fstat(sfd, &st) != 0 || !S_ISREG(st.st_mode)) {
        close(sfd);
        fprintf(stderr, "Error: %s is not a regular file\n", src);
        return 1;
    }

    const char *filename = strrchr(src, '/');
    filename = filename ? filename + 1 : src;

    char dest_dir[PATH_MAX], dest[PATH_MAX];
    snprintf(dest_dir, sizeof(dest_dir), "%s/%s/files", db_root, object);
    snprintf(dest, sizeof(dest), "%s/%s", dest_dir, filename);
    mkdirp(dest_dir);

    int dfd = open(dest, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (dfd < 0) { close(sfd); fprintf(stderr, "Error: Cannot create %s\n", dest); return 1; }
    char buf[65536]; ssize_t n;
    while ((n = read(sfd, buf, sizeof(buf))) > 0) write(dfd, buf, n);
    close(sfd); close(dfd);
    OUT("{\"status\":\"stored\",\"path\":\"%s\"}\n", dest);
    return 0;
}

int cmd_get_file_path(const char *db_root, const char *object, const char *filename) {
    OUT("{\"path\":\"%s/%s/files/%s\"}\n", db_root, object, filename);
    return 0;
}

/* Compute the on-disk destination path for a filename. Caller supplies buffers.
   Files live flat under <object>/files/ — basename is the lookup key, no
   hashing or sub-bucketing. (Pre-2026.05.2 used XX/XX hash buckets; the
   startup sweep in migrate_files_to_flat() lifts those into place.) */
static void file_dest_path(const char *db_root, const char *object, const char *filename,
                           char *dest_dir, size_t dd_sz, char *dest, size_t d_sz) {
    snprintf(dest_dir, dd_sz, "%s/%s/files", db_root, object);
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

/* ========== MIGRATE FILES (XX/XX → flat) ==========
   Pre-2026.05.2 stored files at <obj>/files/<XX>/<XX>/<filename> with the
   XX bytes derived from xxh128(filename). The bucketing was vestigial —
   filenames were already the unique lookup key, so the hash added no
   information and forced a 65K-leaf walk on list-files. Flattening lifts
   each leaf into <obj>/files/<filename> and removes the empty hash dirs.
   Idempotent: a second run finds nothing to move and returns 0. Gated at
   startup by a sentinel file ($DB_ROOT/.files-flat-v1) so cost on
   normal starts is one stat() call. */

static int hex2_name(const char *s) {
    if (strlen(s) != 2) return 0;
    return ((s[0] >= '0' && s[0] <= '9') || (s[0] >= 'a' && s[0] <= 'f')) &&
           ((s[1] >= '0' && s[1] <= '9') || (s[1] >= 'a' && s[1] <= 'f'));
}

/* Migrate one object's files dir. Returns count of files moved (>=0) or
   -1 on a fatal error. Conflicts (flat target already exists) are logged
   and skipped — the bucket leaf is left in place for manual review. */
static int migrate_object_files(const char *db_root, const char *dir,
                                const char *obj, int *conflicts_out) {
    char files_dir[PATH_MAX];
    snprintf(files_dir, sizeof(files_dir), "%s/%s/%s/files", db_root, dir, obj);
    DIR *d1 = opendir(files_dir);
    if (!d1) return 0; /* no files dir = nothing to do */

    int moved = 0;
    int conflicts = 0;
    struct dirent *e1;
    while ((e1 = readdir(d1))) {
        if (!hex2_name(e1->d_name)) continue;
        char sub1[PATH_MAX];
        snprintf(sub1, sizeof(sub1), "%s/%s", files_dir, e1->d_name);
        DIR *d2 = opendir(sub1);
        if (!d2) continue;
        struct dirent *e2;
        while ((e2 = readdir(d2))) {
            if (!hex2_name(e2->d_name)) continue;
            char sub2[PATH_MAX];
            snprintf(sub2, sizeof(sub2), "%s/%s", sub1, e2->d_name);
            DIR *d3 = opendir(sub2);
            if (!d3) continue;
            struct dirent *e3;
            while ((e3 = readdir(d3))) {
                if (e3->d_name[0] == '.') continue;
                char src[PATH_MAX], dst[PATH_MAX];
                snprintf(src, sizeof(src), "%s/%s", sub2, e3->d_name);
                snprintf(dst, sizeof(dst), "%s/%s", files_dir, e3->d_name);
                struct stat st;
                if (stat(dst, &st) == 0) {
                    /* Flat dst already populated — leave bucket leaf alone
                       so admin can resolve manually. */
                    fprintf(stderr,
                        "migrate-files: skip %s/%s/%s (flat target exists at %s)\n",
                        dir, obj, e3->d_name, dst);
                    conflicts++;
                    continue;
                }
                if (rename(src, dst) != 0) {
                    fprintf(stderr,
                        "migrate-files: rename failed for %s -> %s: %s\n",
                        src, dst, strerror(errno));
                    continue;
                }
                moved++;
            }
            closedir(d3);
            /* rmdir is best-effort: leaves with conflicts stay. */
            rmdir(sub2);
        }
        closedir(d2);
        rmdir(sub1);
    }
    closedir(d1);
    if (conflicts_out) *conflicts_out += conflicts;
    return moved;
}

/* Walk schema.conf, migrate every (dir, obj). Emits one-line JSON summary.
   Idempotent — re-running after a successful migration finds nothing to
   move. Intended as a one-time upgrade step for installs that ran the
   pre-2026.05.2 hash-bucketed layout. */
int cmd_migrate_files(const char *db_root) {
    char scpath[PATH_MAX];
    snprintf(scpath, sizeof(scpath), "%s/schema.conf", db_root);
    FILE *sf = fopen(scpath, "r");
    if (!sf) {
        OUT("{\"error\":\"cannot open schema.conf\"}\n");
        return 1;
    }

    uint64_t t0 = now_ms_coarse();
    int objects_seen = 0, objects_migrated = 0, files_moved = 0, conflicts = 0;

    char line[1024];
    while (fgets(line, sizeof(line), sf)) {
        line[strcspn(line, "\n")] = '\0';
        if (!line[0] || line[0] == '#') continue;

        char *c1 = strchr(line, ':');
        if (!c1) continue;
        *c1 = '\0';
        const char *dir = line;
        char *rest = c1 + 1;
        char *c2 = strchr(rest, ':');
        if (!c2) continue;
        *c2 = '\0';
        const char *obj = rest;

        objects_seen++;
        int n = migrate_object_files(db_root, dir, obj, &conflicts);
        if (n > 0) {
            objects_migrated++;
            files_moved += n;
        }
    }
    fclose(sf);

    uint64_t t1 = now_ms_coarse();
    OUT("{\"status\":\"migrated\",\"objects_seen\":%d,\"objects_migrated\":%d,"
        "\"files_moved\":%d,\"conflicts\":%d,\"duration_ms\":%llu}\n",
        objects_seen, objects_migrated, files_moved, conflicts,
        (unsigned long long)(t1 - t0));
    return 0;
}

/* ========== LIST FILES ==========
   Walks <db_root>/<object>/files/, optionally filters by pattern + match
   mode, and returns a sorted page. Files live flat — basename is the
   lookup key; no bucketing. Pre-2026.05.2 used a 2-level XX/XX hash
   bucket tree, lifted in place by the migration sweep at startup
   (migrate_files_to_flat). Pagination is offset/limit on a stable
   alphabetical sort. */

static int cmp_str(const void *a, const void *b) {
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

/* Match modes for cmd_list_files. PREFIX preserves pre-2026.05 semantics
   (the legacy `prefix` request field maps to this). */
typedef enum {
    LF_MATCH_PREFIX = 0,
    LF_MATCH_SUFFIX,
    LF_MATCH_CONTAINS,
    LF_MATCH_GLOB
} ListFilesMatch;

static ListFilesMatch parse_list_files_match(const char *s) {
    if (!s || !*s) return LF_MATCH_PREFIX;
    if (strcmp(s, "prefix")   == 0) return LF_MATCH_PREFIX;
    if (strcmp(s, "suffix")   == 0) return LF_MATCH_SUFFIX;
    if (strcmp(s, "contains") == 0) return LF_MATCH_CONTAINS;
    if (strcmp(s, "glob")     == 0) return LF_MATCH_GLOB;
    return LF_MATCH_PREFIX;
}

static int filename_matches(const char *name, const char *pattern,
                            size_t pattern_len, ListFilesMatch mode) {
    if (pattern_len == 0) return 1;
    switch (mode) {
        case LF_MATCH_PREFIX:
            return strncmp(name, pattern, pattern_len) == 0;
        case LF_MATCH_SUFFIX: {
            size_t name_len = strlen(name);
            if (name_len < pattern_len) return 0;
            return memcmp(name + name_len - pattern_len, pattern, pattern_len) == 0;
        }
        case LF_MATCH_CONTAINS:
            return strstr(name, pattern) != NULL;
        case LF_MATCH_GLOB:
            return fnmatch(pattern, name, 0) == 0;
    }
    return 0;
}

int cmd_list_files(const char *db_root, const char *object,
                   const char *pattern, const char *match,
                   int offset, int limit) {
    if (limit <= 0) limit = g_global_limit;
    if (offset < 0) offset = 0;

    ListFilesMatch mmode = parse_list_files_match(match);
    size_t pattern_len = pattern ? strlen(pattern) : 0;

    char files_dir[PATH_MAX];
    snprintf(files_dir, sizeof(files_dir), "%s/%s/files", db_root, object);

    DIR *d1 = opendir(files_dir);
    if (!d1) {
        /* No files dir = no files. Not an error. */
        OUT("{\"files\":[],\"total\":0,\"offset\":%d,\"limit\":%d}\n", offset, limit);
        return 0;
    }

    /* Collect all matching filenames into a heap-grown array, then sort. */
    char **names = NULL;
    size_t cap = 256, count = 0, buffer_bytes = 0;
    int budget_exceeded = 0;
    names = malloc(cap * sizeof(char *));
    if (!names) { closedir(d1); OUT("{\"error\":\"oom\"}\n"); return 1; }

    struct dirent *e;
    while ((e = readdir(d1))) {
        if (e->d_name[0] == '.') continue;
        /* d_type == DT_DIR skips any leftover XX bucket dirs from a
           pre-migration filestore so list-files behaves sanely even if the
           startup sweep hasn't run (e.g. filesystem was switched mid-life). */
        if (e->d_type == DT_DIR) continue;
        if (!filename_matches(e->d_name, pattern, pattern_len, mmode)) continue;

        /* Per-query memory cap. Models pointer + strdup'd string; ignores
           glibc malloc bookkeeping (a small constant per chunk that's not
           worth tracking precisely). At 500 MB default this triggers around
           7-13M matches depending on filename length. */
        size_t entry_bytes = sizeof(char *) + strlen(e->d_name) + 1;
        if (buffer_bytes + entry_bytes > g_query_buffer_max_bytes) {
            budget_exceeded = 1;
            break;
        }
        buffer_bytes += entry_bytes;

        if (count >= cap) {
            cap *= 2;
            /* Plain realloc + walk: names[] holds strdup'd entries. */
            char **t = realloc(names, cap * sizeof(char *));
            if (!t) {
                for (size_t k = 0; k < count; k++) free(names[k]);
                free(names);
                names = NULL;
                count = 0;
                break;
            }
            names = t;
        }
        names[count++] = strdup(e->d_name);
    }
    closedir(d1);

    if (budget_exceeded) {
        for (size_t i = 0; i < count; i++) free(names[i]);
        free(names);
        OUT(QUERY_BUFFER_ERR);
        return 1;
    }

    /* qsort(NULL, 0, ...) is well-defined as a no-op per POSIX, but Coverity
       can't see that — explicit count guard keeps the static analyzer happy
       and avoids dragging the names arg through a NULL-pointer code path
       even when there are no entries to sort. */
    if (count > 0) qsort(names, count, sizeof(char *), cmp_str);

    /* Emit page [offset, offset+limit) plus the unfiltered total. */
    OUT("{\"files\":[");
    int emitted = 0;
    for (size_t i = (size_t)offset; i < count && emitted < limit; i++) {
        if (emitted) OUT(",");
        OUT("\"%s\"", names[i]);
        emitted++;
    }
    OUT("],\"total\":%zu,\"offset\":%d,\"limit\":%d}\n", count, offset, limit);

    for (size_t i = 0; i < count; i++) free(names[i]);
    free(names);
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
                      int splits, int max_key, int if_not_exists) {
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

    /* Existence check: fields.conf presence is authoritative. If present, the
       object was previously created — bail out before any destructive write
       so we can't silently clobber fields.conf / index.conf / existing data. */
    {
        char probe[PATH_MAX];
        snprintf(probe, sizeof(probe), "%s/%s/%s/fields.conf", db_root, dir, object);
        struct stat st;
        if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) {
            if (if_not_exists) {
                OUT("{\"status\":\"exists\",\"object\":\"%s\",\"dir\":\"%s\"}\n",
                    object, dir);
                return 0;
            }
            OUT("{\"error\":\"object already exists\",\"dir\":\"%s\",\"object\":\"%s\",\"hint\":\"pass \\\"if_not_exists\\\":true for idempotent create, or drop the object first\"}\n",
                dir, object);
            return 1;
        }
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

    /* Defaults + strict validation. As of 2026.05.1, splits must be a
       power of 2 in [16, 4096]. The per-shard index layout (index_splits
       = splits / 4) relies on this regularity. */
    if (splits <= 0) splits = DEFAULT_SPLITS;
    if (!is_valid_splits(splits)) {
        OUT("{\"error\":\"splits=%d invalid; must be a power of 2 in {16, 32, 64, 128, 256, 512, 1024, 2048, 4096}\"}\n",
            splits);
        return 1;
    }
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

/* ========== DROP OBJECT ==========
   Removes everything for an object: data/ metadata/ indexes/ files/ dirs,
   fields.conf, indexes/index.conf, the schema.conf entry, and invalidates
   every in-memory cache that might hold state for it. Idempotent with
   if_exists=1 — returns {"status":"not_found"} instead of an error. */

int cmd_drop_object(const char *db_root, const char *dir, const char *object,
                    int if_exists) {
    if (!dir || !dir[0]) {
        OUT("{\"error\":\"dir is required\"}\n");
        return 1;
    }
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        return 1;
    }

    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s/%s", db_root, dir, object);
    struct stat st;
    if (stat(obj_dir, &st) != 0 || !S_ISDIR(st.st_mode)) {
        if (if_exists) {
            OUT("{\"status\":\"not_found\",\"dir\":\"%s\",\"object\":\"%s\"}\n",
                dir, object);
            return 0;
        }
        OUT("{\"error\":\"object not found\",\"dir\":\"%s\",\"object\":\"%s\"}\n",
            dir, object);
        return 1;
    }

    /* Invalidate caches BEFORE removing files so no concurrent reader picks
       up a stale mmap after we unlink the backing files. */
    char eff_obj[PATH_MAX];
    snprintf(eff_obj, sizeof(eff_obj), "%s/%s", dir, object);
    fcache_invalidate(obj_dir);
    invalidate_idx_cache(object);
    invalidate_schema_caches(db_root, object);

    /* Nuke the on-disk object tree entirely. */
    rmrf(obj_dir);

    /* Strip the "dir:object:..." line from schema.conf (atomic rewrite). */
    char schema_path[PATH_MAX], tmp_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    snprintf(tmp_path, sizeof(tmp_path), "%s.new", schema_path);
    FILE *in = fopen(schema_path, "r");
    if (in) {
        FILE *out = fopen(tmp_path, "w");
        if (out) {
            char line[1024];
            char prefix[512];
            int plen = snprintf(prefix, sizeof(prefix), "%s:%s:", dir, object);
            while (fgets(line, sizeof(line), in)) {
                if (strncmp(line, prefix, (size_t)plen) == 0) continue;
                fputs(line, out);
            }
            fclose(out);
            fclose(in);
            if (rename(tmp_path, schema_path) != 0) {
                log_msg(1, "drop_object: rename(%s → %s): %s",
                        tmp_path, schema_path, strerror(errno));
                unlink(tmp_path);
            }
        } else {
            fclose(in);
        }
    }

    log_msg(3, "DROP-OBJECT %s/%s", dir, object);
    OUT("{\"status\":\"dropped\",\"dir\":\"%s\",\"object\":\"%s\"}\n", dir, object);
    return 0;
}

/* List the objects under a tenant. Reads schema.conf for entries that begin
   with "<dir>:". Used by shard-cli's tenant browser. */
int cmd_list_objects(const char *db_root, const char *dir) {
    if (!dir || !dir[0]) { OUT("{\"error\":\"dir is required\"}\n"); return 1; }
    if (!is_valid_dir(dir)) {
        OUT("{\"error\":\"unknown dir\",\"dir\":\"%s\"}\n", dir);
        return 1;
    }

    char schema_path[PATH_MAX];
    snprintf(schema_path, sizeof(schema_path), "%s/schema.conf", db_root);
    FILE *f = fopen(schema_path, "r");
    OUT("{\"dir\":\"%s\",\"objects\":[", dir);
    if (f) {
        char prefix[512];
        int plen = snprintf(prefix, sizeof(prefix), "%s:", dir);
        char line[1024];
        int printed = 0;
        while (fgets(line, sizeof(line), f)) {
            if (line[0] == '#' || line[0] == '\n') continue;
            if (strncmp(line, prefix, (size_t)plen) != 0) continue;
            const char *name_start = line + plen;
            const char *name_end = strchr(name_start, ':');
            if (!name_end || name_end == name_start) continue;
            OUT("%s\"%.*s\"", printed ? "," : "",
                (int)(name_end - name_start), name_start);
            printed++;
        }
        fclose(f);
    }
    OUT("]}\n");
    return 0;
}

static const char *field_type_str(enum FieldType t) {
    switch (t) {
        case FT_VARCHAR:  return "varchar";
        case FT_LONG:     return "long";
        case FT_INT:      return "int";
        case FT_SHORT:    return "short";
        case FT_DOUBLE:   return "double";
        case FT_BOOL:     return "bool";
        case FT_BYTE:     return "byte";
        case FT_NUMERIC:  return "numeric";
        case FT_DATE:     return "date";
        case FT_DATETIME: return "datetime";
        default:          return "unknown";
    }
}

/* Describe an object: schema (typed fields), indexes, splits, max_key, max_value,
   live record_count. Used by shard-cli to populate criteria builders + table
   headers without forcing the caller to read the on-disk fields.conf. */
int cmd_describe_object(const char *db_root, const char *dir, const char *object) {
    if (!dir || !dir[0])    { OUT("{\"error\":\"dir is required\"}\n"); return 1; }
    if (!object || !object[0]) { OUT("{\"error\":\"object is required\"}\n"); return 1; }

    char obj_root[PATH_MAX];
    snprintf(obj_root, sizeof(obj_root), "%s/%s/%s", db_root, dir, object);
    struct stat st;
    if (stat(obj_root, &st) != 0 || !S_ISDIR(st.st_mode)) {
        OUT("{\"error\":\"object not found\",\"dir\":\"%s\",\"object\":\"%s\"}\n",
            dir, object);
        return 1;
    }

    /* load_schema / load_typed_schema take an `effective_root` (db_root/dir)
       and the bare object name as separate args. eff_obj kept around for
       get_live_count which uses the joined form. */
    char effective_root[PATH_MAX];
    snprintf(effective_root, sizeof(effective_root), "%s/%s", db_root, dir);
    char eff_obj[PATH_MAX];
    snprintf(eff_obj, sizeof(eff_obj), "%s/%s", dir, object);

    Schema sch = load_schema(effective_root, object);
    TypedSchema *ts = load_typed_schema(effective_root, object);

    OUT("{\"dir\":\"%s\",\"object\":\"%s\",\"splits\":%d,\"max_key\":%d,\"max_value\":%d,\"slot_size\":%d",
        dir, object, sch.splits, sch.max_key, sch.max_value, sch.slot_size);

    OUT(",\"fields\":[");
    if (ts && ts->nfields > 0) {
        int printed = 0;
        for (int i = 0; i < ts->nfields; i++) {
            const TypedField *f = &ts->fields[i];
            if (f->removed) continue;
            OUT("%s{\"name\":\"%s\",\"type\":\"%s\",\"size\":%d",
                printed ? "," : "", f->name, field_type_str(f->type), f->size);
            if (f->type == FT_NUMERIC)
                OUT(",\"scale\":%d", f->numeric_scale);
            switch (f->default_kind) {
                case DK_LITERAL:     OUT(",\"default\":\"%s\"", f->default_val); break;
                case DK_AUTO_CREATE: OUT(",\"default\":\"auto_create\""); break;
                case DK_AUTO_UPDATE: OUT(",\"default\":\"auto_update\""); break;
                case DK_SEQ:         OUT(",\"default\":\"seq(%s)\"", f->default_val); break;
                case DK_UUID:        OUT(",\"default\":\"uuid()\""); break;
                case DK_RANDOM:      OUT(",\"default\":\"random(%s)\"", f->default_val); break;
                case DK_NONE:        break;
            }
            OUT("}");
            printed++;
        }
    }
    OUT("]");

    /* Index list: read $obj_root/indexes/index.conf — one declared index
       per line. The .idx files only materialize after the first insert, so
       scanning them would miss empty objects. */
    OUT(",\"indexes\":[");
    char idx_conf[PATH_MAX];
    snprintf(idx_conf, sizeof(idx_conf), "%s/indexes/index.conf", obj_root);
    FILE *iconf = fopen(idx_conf, "r");
    if (iconf) {
        char line[256];
        int printed = 0;
        while (fgets(line, sizeof(line), iconf)) {
            char *end = line + strlen(line);
            while (end > line && (end[-1] == '\n' || end[-1] == '\r' || end[-1] == ' ')) *--end = '\0';
            if (line[0] == '\0' || line[0] == '#') continue;
            OUT("%s\"%s\"", printed ? "," : "", line);
            printed++;
        }
        fclose(iconf);
    }
    OUT("]");

    int rc_count = get_live_count(db_root, eff_obj);
    OUT(",\"record_count\":%d}\n", rc_count);
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
    /* Coarse mutex for hashtable + bucket accumulator updates when the scan
       runs callbacks concurrently (scan_shards path). parallel_indexed_agg
       pre-existed this and uses per-worker ctxs merged later, so the lock is
       harmless there (no contention). */
    pthread_mutex_t lock;
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

    /* Find or create bucket + accumulate under internal mutex.
       criteria_match_tree + group extraction above are thread-local /
       read-only; only hashtable insert and accumulator updates need
       serialization. Mutex stays cheap because the sections are tiny. */
    pthread_mutex_lock(&ctx->lock);
    AggBucket *bkt;
    if (ctx->ngroups > 0) {
        bkt = agg_find_or_create(ctx, gvals, ctx->ngroups);
    } else {
        char *empty = "";
        bkt = agg_find_or_create(ctx, &empty, 1);
    }
    if (!bkt) {
        pthread_mutex_unlock(&ctx->lock);
        return 1;  /* budget exceeded — stop scan */
    }

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
    pthread_mutex_unlock(&ctx->lock);
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

        if (n >= cap) {
            cap *= 2;
            AggSpec *t = xrealloc_or_free(specs, cap * sizeof(*t));
            if (!t) { specs = NULL; break; }
            specs = t;
        }
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

    /* Use the persistent shard mmap cache. Per-call mmap+munmap was paying
       page-fault + TLB-flush per shard per query — visible as a 4-5x agg
       slowdown vs README baseline before this fix. */
    FcacheRead fc = fcache_get_read(shard);
    if (!fc.map) return NULL;
    uint32_t slots = fc.slots_per_shard;
    uint32_t mask = slots - 1;

    for (int ei = 0; ei < sa->entry_count; ei++) {
        if (query_deadline_tick(sa->deadline, &sa->dl_counter)) break;
        CollectedHash *e = &sa->entries[ei];
        for (uint32_t p = 0; p < slots; p++) {
            uint32_t s = ((uint32_t)e->start_slot + p) & mask;
            SlotHeader *h = (SlotHeader *)(fc.map + zoneA_off(s));
            if (h->flag == 0 && h->key_len == 0) break;
            if (h->flag != 1) continue;
            if (memcmp(h->hash, e->hash, 16) != 0) continue;
            const uint8_t *block = fc.map + zoneB_off(s, slots, sa->sch->slot_size);
            agg_scan_cb(h, block, &sa->local);
            break;
        }
    }
    fcache_release(fc);
    return NULL;
}

/* Orchestrate parallel indexed aggregate: fan out per-shard workers with
   local AggCtx each, then merge into main_ctx. */
static void parallel_indexed_agg(AggCtx *main_ctx, const char *db_root,
                                 const char *object, const Schema *sch,
                                 CollectedHash *batch, int batch_count) {
    int group_starts[1024], group_sizes[1024];
    int nshard_groups = shard_group_batch(batch, batch_count, group_starts, group_sizes, 1024);

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
    pthread_mutex_destroy(&ctx->lock);
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

static int idx_agg_cb(const char *val, size_t vlen, const uint8_t *hash16, void *raw) __attribute__((unused));
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

/* Run the plan dispatcher into `ctx` for the given criteria tree.
   ctx must be already initialized (specs, group setup, deadline,
   shared_buffer_bytes). Mutates ctx.tree. Returns 0 on success,
   -1 if the deadline tripped or the buffer budget was exceeded.
   Extracted so the NEQ algebraic shortcut can call it twice with
   different trees (eq-set, full-set) and subtract scalars. */
static int agg_run_plan(AggCtx *ctx, CriteriaNode *tree,
                        const char *db_root, const char *object,
                        const Schema *sch) {
    ctx->tree = tree;

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);

    QueryPlan plan = choose_primary_source(tree, db_root, object);

    if (plan.kind == PRIMARY_LEAF) {
        SearchCriterion *pc = plan.primary_leaf;
        enum SearchOp op = pc->op;
        int check_primary = op_needs_check_primary(op);
        CollectCtx cc;
        collect_ctx_init(&cc);
        cc.splits = sch->splits;
        cc.primary_crit = pc;
        cc.check_primary = check_primary;
        cc.deadline = ctx->deadline;
        btree_dispatch(db_root, object, pc->field, sch->splits,
                       pc,
                       resolve_idx_field(ctx->fs->ts, pc->field),
                       collect_hash_cb, &cc);
        if (cc.budget_exceeded) ctx->budget_exceeded = 1;
        else parallel_indexed_agg(ctx, db_root, object, sch, cc.entries, (int)cc.count);
        collect_ctx_destroy(&cc);
    } else if (plan.kind == PRIMARY_INTERSECT) {
        CriteriaNode *saved = ctx->tree;
        ctx->tree = NULL;
        keyset_agg_from_intersect(db_root, object, sch, ctx, &plan,
                                  saved, &ctx->tree, ctx->deadline);
        ctx->tree = saved;
    } else if (plan.kind == PRIMARY_KEYSET) {
        int budget_exceeded = 0;
        keyset_agg_from_or(db_root, object, sch, ctx, plan.or_node,
                           ctx->deadline, &budget_exceeded);
        if (budget_exceeded) ctx->budget_exceeded = 1;
    } else {
        scan_shards(data_dir, sch->slot_size, agg_scan_cb, ctx);
    }

    if (ctx->deadline->timed_out) return -1;
    if (ctx->budget_exceeded) return -1;
    return 0;
}

/* Free buckets owned by a cloned/local AggCtx. Counterpart to agg_free
   that omits `free(ctx->specs)` and `pthread_mutex_destroy()` because
   those resources are owned by the cloning origin. */
static void agg_ctx_free_local(AggCtx *ctx) {
    for (int i = 0; i < AGG_HT_SIZE; i++) {
        AggBucket *b = ctx->ht[i];
        while (b) {
            AggBucket *next = b->next;
            free(b->group_key);
            for (int j = 0; j < ctx->ngroups; j++) free(b->group_vals[j]);
            if (ctx->ngroups == 0 && b->group_vals) free(b->group_vals[0]);
            free(b->group_vals);
            free(b->accums);
            free(b);
            b = next;
        }
        ctx->ht[i] = NULL;
    }
    ctx->total_buckets = 0;
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
    pthread_mutex_init(&ctx.lock, NULL);
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

    /* ===== NEQ algebraic shortcut =====
       Narrow eligibility: criteria is exactly one NEQ leaf on an indexed
       field, no group_by, no having, every spec is COUNT/SUM/AVG (algebraic
       under subtraction; MIN/MAX excluded — no closed form for the
       complement). Substitution: agg(neq X) = agg(*) − agg(eq X). Wins
       because the existing NEQ-on-indexed path collects ~all hashes and
       fetches every record, while this path runs one full scan plus a
       (typically tiny) indexed eq scan. */
    int neq_eligible = 0;
    if (no_group && (!having_json || having_json[0] == '\0') &&
        tree && tree->kind == CNODE_LEAF &&
        tree->leaf.op == OP_NOT_EQUAL) {
        int algebraic = 1;
        for (int i = 0; i < nspecs; i++) {
            if (specs[i].fn != AGG_COUNT && specs[i].fn != AGG_SUM && specs[i].fn != AGG_AVG) {
                algebraic = 0; break;
            }
        }
        if (algebraic) {
            /* Per-shard layout — check the new <field>/<NNN>.idx layout. */
            Schema sch_neq = load_schema(db_root, object);
            if (btree_idx_exists(db_root, object, tree->leaf.field, sch_neq.splits))
                neq_eligible = 1;
        }
    }

    if (neq_eligible) {
        /* Two side-aggregations: eq(X) on the original ctx (its setup is
           already correct), and full(*) on a clone that shares specs/fs/
           buffer-budget read-only. */
        SearchCriterion *leaf = &tree->leaf;
        enum SearchOp saved_op = leaf->op;
        leaf->op = OP_EQUAL;
        compile_criteria_tree(tree, fs.ts);
        int rc_eq = agg_run_plan(&ctx, tree, db_root, object, &sch);
        leaf->op = saved_op;
        compile_criteria_tree(tree, fs.ts);

        AggCtx ctx_full;
        agg_ctx_clone_shared(&ctx_full, &ctx);
        if (rc_eq == 0) (void)agg_run_plan(&ctx_full, NULL, db_root, object, &sch);

        if (dl.timed_out) {
            OUT("{\"error\":\"query_timeout\"}\n");
            agg_ctx_free_local(&ctx_full);
            free_criteria_tree(tree);
            agg_free(&ctx);
            return -1;
        }
        if (ctx.budget_exceeded || ctx_full.budget_exceeded) {
            OUT(QUERY_BUFFER_ERR);
            agg_ctx_free_local(&ctx_full);
            free_criteria_tree(tree);
            agg_free(&ctx);
            return -1;
        }

        /* Pull the single bucket from each side. Either may be empty:
           agg_eq is empty when no record matches (NEQ matches everything),
           agg_full is empty only on an empty table. */
        int n_eq = 0, n_full = 0;
        AggBucket **bs_eq = agg_collect(&ctx, &n_eq);
        AggBucket **bs_full = agg_collect(&ctx_full, &n_full);
        AggAccum *acc_eq = (n_eq > 0) ? bs_eq[0]->accums : NULL;
        AggAccum *acc_full = (n_full > 0) ? bs_full[0]->accums : NULL;

        /* Emit single-bucket no-group output, subtracting per spec. */
        if (csv_delim) {
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                csv_emit_cell(specs[i].alias, csv_delim);
            }
            OUT("\n");
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) { char d[2] = { csv_delim, '\0' }; OUT("%s", d); }
                int64_t cnt = (acc_full ? acc_full[i].count : 0) - (acc_eq ? acc_eq[i].count : 0);
                double sum = (acc_full ? acc_full[i].sum : 0.0) - (acc_eq ? acc_eq[i].sum : 0.0);
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: snprintf(vbuf, sizeof(vbuf), "%ld", (long)cnt); break;
                    case AGG_SUM:   fmt_double(vbuf, sizeof(vbuf), sum); break;
                    case AGG_AVG:   fmt_double(vbuf, sizeof(vbuf), cnt > 0 ? sum / (double)cnt : 0.0); break;
                    default:        vbuf[0] = '\0'; break;
                }
                csv_emit_cell(vbuf, csv_delim);
            }
            OUT("\n");
        } else {
            OUT("{");
            for (int i = 0; i < nspecs; i++) {
                if (i > 0) OUT(",");
                int64_t cnt = (acc_full ? acc_full[i].count : 0) - (acc_eq ? acc_eq[i].count : 0);
                double sum = (acc_full ? acc_full[i].sum : 0.0) - (acc_eq ? acc_eq[i].sum : 0.0);
                char vbuf[64];
                switch (specs[i].fn) {
                    case AGG_COUNT: OUT("\"%s\":%ld", specs[i].alias, (long)cnt); break;
                    case AGG_SUM:
                        fmt_double(vbuf, sizeof(vbuf), sum);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    case AGG_AVG:
                        fmt_double(vbuf, sizeof(vbuf), cnt > 0 ? sum / (double)cnt : 0.0);
                        OUT("\"%s\":%s", specs[i].alias, vbuf); break;
                    default: break;
                }
            }
            OUT("}\n");
        }

        free(bs_eq); free(bs_full);
        agg_ctx_free_local(&ctx_full);
        free_criteria_tree(tree);
        agg_free(&ctx);
        return 0;
    }

    if (agg_run_plan(&ctx, tree, db_root, object, &sch) != 0) {
        if (dl.timed_out) OUT("{\"error\":\"query_timeout\"}\n");
        else if (ctx.budget_exceeded) OUT(QUERY_BUFFER_ERR);
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

