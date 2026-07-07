#include "types.h"
#include "slotcask.h"
#include "io_direct.h"
#include "query_internal.h"
#include <dirent.h>
#include <fnmatch.h>

/* ========== Probing helpers ========== */

/* Check if slot at given offset matches our key. Returns:
   1 = exact match (same key, active)
   0 = empty slot (can write here)
  -1 = tombstone (deleted, can write here on insert, skip on get)
  -2 = occupied by different key (continue probing) */
/* g_scan_stop moved to ShardDb struct */

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
    count_scan_cb_flush_thread();  /* flush TLS pending count before thread exits */
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
    parallel_for_io(scan_worker, args, path_count, sizeof(ScanWorkerArg));
    free(args);

    for (int i = 0; i < path_count; i++) free(paths[i]);
    free(paths);
}

/* ========== v2 (slotcask) scan bridge ========== */

/* The engine's scan_callback signature predates slotcask. To keep all
   existing find/count/aggregate/keys/fetch callbacks working unchanged
   on v2 objects, we adapt slotcask_walk_live to that signature here.
   Synthesizes a SlotHeader (hash, flag=1, klen, vlen) and passes the
   key-bytes pointer as `block` — slotcask stores key + value contiguously
   in the segment slot, which is exactly the v1 Zone B layout the cb
   expects (block[0..klen]=key, block[klen..klen+vlen]=value). */

typedef struct {
    SlotcaskDb  *db;
    int          kf_shard_id;
    SlotcaskScanCb scb;
    void        *sctx;
    int         *stop_flag;
    FILE        *parent_out;
} V2ShardArg;

static int v2_scan_wrap_cb(const uint8_t hash[16],
                            const void *key, size_t klen,
                            const void *value, size_t vlen,
                            void *wrap_ctx) {
    (void)value;  /* contiguous with key; cb derives via klen */
    V2ScanWrap *w = (V2ScanWrap *)wrap_ctx;
    SlotHeader hdr;
    memcpy(hdr.hash, hash, 16);
    hdr.flag      = 1;
    hdr.key_len   = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;
    return w->cb(&hdr, (const uint8_t *)key, w->ctx);
}

/* Per-shard worker: sets thread-local g_out (otherwise OUT() in the
   callback writes to stdout instead of the connection's stream), then
    calls the storage primitive for one kf shard. The shared stop_flag
    lets one shard's "abort" return halt the rest. */

/* Forward decl — definition lives alongside count_scan_cb in cmd_count.
   Drains the per-thread TLS count accumulator into the bound CountCtx;
   no-op for scan workers whose callback doesn't use TLS counting. */

/* Forward declarations — defined below after the O_DIRECT helper block. */
void scan_shards_v2_o_direct(SlotcaskDb *db, scan_callback cb, void *ctx);
void scan_shards_v2_o_direct_match(SlotcaskDb *db,
                                    FieldSchema *fs,
                                    const CompiledCriterion *single_cc,
                                    const CriteriaNode *tree,
                                    QueryDeadline *dl,
                                    int64_t *out_count);

/* Streaming variant of scan_shards_v2 — routes through the O_DIRECT
   seg-file path for cache pollution avoidance.  Early-stop semantics
   are preserved: seg_scan_o_direct returns non-zero when the cb adapter
   returns non-zero, and the shared stop_flag propagates across parallel
   file workers so limit-bound scans bail quickly.
   The old slotcask_walk_one_shard_streaming per-kf-shard fan-out is
   replaced by the seg-file fan-out in scan_shards_v2_o_direct. */
void scan_shards_v2_streaming(SlotcaskDb *db, scan_callback cb, void *ctx) {
    scan_shards_v2_o_direct(db, cb, ctx);
}

/* ========== O_DIRECT full-scan path (FP_FULL_SCAN / Phase 1e.4) ==========
 *
 * scan_shards_v2_o_direct: cache-bypassing replacement for the mmap-based
 * scan_shards_v2 / slotcask_walk_one_shard inner loop.  Enumerates every
 * .dat segment file under <data_dir>/data/streams/<s>/ for each stream
 * s in [0, num_streams) and calls seg_scan_o_direct on each file.  The
 * caller's scan_callback is invoked via a thin adapter that reconstructs
 * the (SlotHeader *, block) shape the callback expects.
 *
 * Parallelism: one parallel_for entry per segment file across all streams.
 * This matches the throughput of scan_shards_v2's per-kf-shard fan-out
 * (typically the same or better because .dat files are the actual data).
 *
 * Fallback: if O_DIRECT open fails silently (EINVAL / unsupported FS),
 * seg_scan_o_direct reverts to buffered + POSIX_FADV_DONTNEED internally —
 * the caller is unaffected.
 */

/* Adapter: od_record_cb → v2_scan_wrap_cb (SlotcaskScanCb) → scan_callback.
 * rec layout: [0..16) hash16  [16..18) klen LE  [19] flag  [19] rsv  [20..24) vlen LE
 *             [24..24+klen) key  [24+klen..) value
 * od_record_cb is called only for flag==1 records, so we skip the flag check. */

int od_seg_record_cb(const uint8_t *rec, size_t vlen,
                             const uint8_t hash16[16], void *raw_ctx)
{
    OdSegAdapterCtx *actx = (OdSegAdapterCtx *)raw_ctx;
    if (actx->stop_flag &&
        __atomic_load_n(actx->stop_flag, __ATOMIC_RELAXED)) return 1;

    uint16_t klen_le;
    memcpy(&klen_le, rec + 16, 2);
    uint16_t klen = klen_le;   /* already native-endian LE on x86/ARM LE */
    const uint8_t *key = rec + 24;
    const uint8_t *val = rec + 24 + klen;
    /* Delegate to the existing v2_scan_wrap_cb which synthesises a
       SlotHeader and fires the real scan_callback. */
    int rc = v2_scan_wrap_cb(hash16, key, klen, val, (size_t)vlen,
                             actx->wrap);
    if (rc != 0 && actx->stop_flag)
        __atomic_store_n(actx->stop_flag, 1, __ATOMIC_RELEASE);
    return rc;
}

/* One entry in the per-file parallel_for array. */
typedef struct {
    char           seg_path[PATH_MAX];
    int            slot_size;
    int            format;     /* SLOTCASK_FORMAT_FIXED or SLOTCASK_FORMAT_VARIABLE */
    V2ScanWrap    *wrap;
    int           *stop_flag;
    FILE          *parent_out;
} OdSegFileArg;

static void *od_seg_file_worker(void *raw) {
    OdSegFileArg *arg = (OdSegFileArg *)raw;
    g_out = arg->parent_out ? arg->parent_out : stdout;
    if (__atomic_load_n(arg->stop_flag, __ATOMIC_RELAXED)) return NULL;
    OdSegAdapterCtx actx = { .wrap = arg->wrap, .stop_flag = arg->stop_flag };
    if (arg->format == SLOTCASK_FORMAT_VARIABLE)
        seg_scan_o_direct_varlen(arg->seg_path, od_seg_record_cb, &actx);
    else
        seg_scan_o_direct(arg->seg_path, arg->slot_size, od_seg_record_cb, &actx);
    /* Drain per-thread count accumulator (count_scan_cb) so the
       orchestrator sees this worker's contribution after parallel_for
       joins.  No-op for callbacks that don't use the TLS counter. */
    count_scan_cb_flush_thread();
    return NULL;
}

/* Enumerate all .dat files under every stream directory and fan out. */
void scan_shards_v2_o_direct(SlotcaskDb *db, scan_callback cb, void *ctx) {
    if (!db || db->num_streams <= 0) return;

    /* Collect all .dat paths into a dynamic array. */
    OdSegFileArg *args = NULL;
    size_t nargs = 0, cap = 0;

    V2ScanWrap wrap = { cb, ctx };
    int stop_flag = 0;
    FILE *parent_out = g_out;

    for (int s = 0; s < db->num_streams; s++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir),
                 "%s/data/streams/%03d", db->data_dir, s);
        DIR *dh = opendir(stream_dir);
        if (!dh) continue;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            size_t nlen = strlen(de->d_name);
            if (nlen < 4 || strcmp(de->d_name + nlen - 4, ".dat") != 0)
                continue;
            /* Grow array if needed. */
            if (nargs >= cap) {
                size_t newcap = cap ? cap * 2 : 64;
                OdSegFileArg *t = realloc(args, newcap * sizeof(OdSegFileArg));
                if (!t) { closedir(dh); goto run; }
                args = t;
                cap = newcap;
            }
            snprintf(args[nargs].seg_path, PATH_MAX,
                     "%s/%s", stream_dir, de->d_name);
            args[nargs].slot_size  = db->slot_size;
            args[nargs].format     = db->format;
            args[nargs].wrap       = &wrap;
            args[nargs].stop_flag  = &stop_flag;
            args[nargs].parent_out = parent_out;
            nargs++;
        }
        closedir(dh);
    }

run:
    if (nargs == 0) { free(args); return; }
    g_scan_stop = 0;
    parallel_for_io(od_seg_file_worker, args, (int)nargs, sizeof(OdSegFileArg));
    free(args);
}

/* ── inline-match scan dispatcher (zero-callback, direct match_typed) ── */

/* One entry in the per-file parallel_for array for the match path. */
/* Match callback context for varlen inline-match scans. */
typedef struct {
    int64_t            *count;
    FieldSchema        *fs;
    const CompiledCriterion *single_cc;
    const CriteriaNode  *tree;
} VarlenMatchCtx;

/* od_record_cb wrapper for varlen match scanning.  Extracts the value
   from the record and runs match_typed / criteria_match_tree directly. */
static int varlen_match_cb(const uint8_t *rec, size_t vlen,
                            const uint8_t hash16[16], void *raw) {
    VarlenMatchCtx *mc = (VarlenMatchCtx *)raw;
    (void)hash16;
    uint16_t klen;
    memcpy(&klen, rec + 16, 2);
    const uint8_t *value = rec + 24 + (size_t)klen;

    int matched = 0;
    if (mc->single_cc) {
        if (match_typed(value, mc->single_cc, mc->fs) > 0)
            matched = 1;
    } else if (mc->tree) {
        if (criteria_match_tree(value, mc->tree, mc->fs))
            matched = 1;
    }
    if (matched && mc->count) (*mc->count)++;
    return 0;
}

typedef struct {
    char                seg_path[PATH_MAX];
    int                 slot_size;
    int                 format;     /* SLOTCASK_FORMAT_FIXED or SLOTCASK_FORMAT_VARIABLE */
    FieldSchema        *fs;
    const CompiledCriterion *single_cc;
    const CriteriaNode  *tree;
    QueryDeadline      *dl;
    int64_t            *out_count;
} OdMatchFileArg;

static void *od_match_file_worker(void *raw) {
    OdMatchFileArg *arg = (OdMatchFileArg *)raw;
    int64_t local_count = 0;

    if (arg->format == SLOTCASK_FORMAT_VARIABLE) {
        VarlenMatchCtx mc = {
            .count = &local_count, .fs = arg->fs,
            .single_cc = arg->single_cc, .tree = arg->tree,
        };
        seg_scan_o_direct_varlen(arg->seg_path, varlen_match_cb, &mc);
    } else {
        int rc = seg_scan_o_direct_match(arg->seg_path, arg->slot_size,
                                          arg->fs, arg->single_cc, arg->tree,
                                          arg->dl, &local_count);
        (void)rc;
    }

    if (local_count > 0)
        __atomic_add_fetch(arg->out_count,
                           local_count, __ATOMIC_RELAXED);
    return NULL;
}

/* Enumerate all .dat files under every stream directory and fan out,
   counting matches via the inline match path (no callback indirection). */
void scan_shards_v2_o_direct_match(SlotcaskDb *db,
                                    FieldSchema *fs,
                                    const CompiledCriterion *single_cc,
                                    const CriteriaNode *tree,
                                    QueryDeadline *dl,
                                    int64_t *out_count)
{
    if (!db || db->num_streams <= 0) return;
    *out_count = 0;

    OdMatchFileArg *args = NULL;
    size_t nargs = 0, cap = 0;

    for (int s = 0; s < db->num_streams; s++) {
        char stream_dir[PATH_MAX];
        snprintf(stream_dir, sizeof(stream_dir),
                 "%s/data/streams/%03d", db->data_dir, s);
        DIR *dh = opendir(stream_dir);
        if (!dh) continue;
        struct dirent *de;
        while ((de = readdir(dh)) != NULL) {
            size_t nlen = strlen(de->d_name);
            if (nlen < 4 || strcmp(de->d_name + nlen - 4, ".dat") != 0)
                continue;
            if (nargs >= cap) {
                size_t newcap = cap ? cap * 2 : 64;
                OdMatchFileArg *t = realloc(args, newcap * sizeof(OdMatchFileArg));
                if (!t) { closedir(dh); goto run_match; }
                args = t;
                cap = newcap;
            }
            snprintf(args[nargs].seg_path, PATH_MAX,
                     "%s/%s", stream_dir, de->d_name);
            args[nargs].slot_size  = db->slot_size;
            args[nargs].format     = db->format;
            args[nargs].fs         = fs;
            args[nargs].single_cc  = single_cc;
            args[nargs].tree       = tree;
            args[nargs].dl         = dl;
            args[nargs].out_count  = out_count;
            nargs++;
        }
        closedir(dh);
    }

run_match:
    if (nargs == 0) { free(args); return; }
    parallel_for_io(od_match_file_worker, args, (int)nargs, sizeof(OdMatchFileArg));
    free(args);
}

/* Dispatch helper: callers that have a Schema in scope use this to pick
   the right scan path. Returns 0 on success, -1 if v2 dispatch failed
   (caller can fall back to the legacy path or report no rows). */
int scan_dispatch(const char *db_root, const char *object,
                  const Schema *sc, const char *data_dir,
                  scan_callback cb, void *ctx) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;
    scan_shards_v2_o_direct(sdb, cb, ctx);
    return 0;
}

/* ========== Index-driven record lookup dispatch ==========
 *
 * Indexed query paths (PRIMARY_LEAF / INTERSECT / KEYSET) get a list of
 * 16-byte hashes from the btree and need to fetch the underlying record.
 * slotcask_lookup_by_hash gives transient pointers under the segcache
 * rdlock — we copy key+value into a malloc'd buffer that outlives the
 * cb so callers can hold the reference past release_record_ref.
 *
 * The (key, val) pointers are laid out contiguously so callers using
 * the historical (key followed by val) layout still work unchanged.
 */

static int v2_record_capture_cb(const uint8_t hash[16],
                                 const void *key, size_t klen,
                                 const void *value, size_t vlen,
                                 void *ctx) {
    (void)hash;
    RecordRef *r = (RecordRef *)ctx;
    size_t total = klen + vlen + 1;
    r->v2_buf = (total <= sizeof(r->inline_buf)) ? r->inline_buf : malloc(total);
    if (!r->v2_buf) return 1;
    memcpy(r->v2_buf, key, klen);
    if (vlen) memcpy(r->v2_buf + klen, value, vlen);
    r->v2_buf[klen + vlen] = 0;
    r->key = r->v2_buf;
    r->klen = klen;
    r->val = r->v2_buf + klen;
    r->vlen = vlen;
    return 1;  /* found; stop */
}

int read_record_ref(const char *db_root, const char *object,
                    const Schema *sch, const uint8_t hash[16],
                    RecordRef *out) {
    memset(out, 0, sizeof(*out));
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;
    slotcask_lookup_by_hash(sdb, hash, v2_record_capture_cb, out);
    return out->v2_buf ? 0 : -1;
}

void release_record_ref(RecordRef *r) {
    if (!r) return;
    if (r->fc.map) { fcache_release(r->fc); r->fc.map = NULL; }
    /* Only free the malloc'd fallback — inline_buf is part of the
       caller's own RecordRef and needs no explicit release. */
    if (r->v2_buf && r->v2_buf != r->inline_buf) free(r->v2_buf);
    r->v2_buf = NULL;
    r->key = r->val = NULL;
    r->klen = r->vlen = 0;
}

/* ========== FIELD DECODE ========== */

void init_field_schema(FieldSchema *fs, const char *db_root, const char *object) {
    fs->ts = load_typed_schema(db_root, object);
    fs->nfields = 0;
    Schema sc = load_schema(db_root, object);
    fs->auto_key = sc.auto_key;
    fs->auto_key_schema_snapshot = sc;
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
                    char *v = typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            return cp > 0 ? strdup(cat) : NULL;
        }
        int idx = typed_field_index(fs->ts, field);
        return typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)raw_len, idx);
    }
    return NULL;
}

char *json_escape_field(char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) { free(v); return NULL; }
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    free(v);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}

char *json_escape_const(const char *v) {
    if (!v) return NULL;
    size_t len = strlen(v);
    char *esc = malloc(len * 6 + 1);
    if (!esc) return NULL;
    int n = json_escape_into(esc, len * 6 + 1, v, len);
    if (n < 0) { free(esc); return NULL; }
    esc[n] = '\0';
    return esc;
}

/* CompiledCriterion is now defined in types.h — visible to io_direct.c's
   batch matchers.  Keep only the LikeKind forward decl for code that still
   references the name directly. */

/* ========== Parallel fetch: collect hashes → group by shard → parallel fetch ========== */

/* Shared hash entry for collected B+ tree results (used by find) */
int cmp_by_shard(const void *a, const void *b) {
    return ((const CollectedHash *)a)->shard_id - ((const CollectedHash *)b)->shard_id;
}

/* Sort `batch` in place by shard_id and split into per-shard groups. The
   group_starts[i]/group_sizes[i] pair describes a contiguous run of entries
   with the same shard_id, suitable for fan-out to per-shard workers.
   Returns the number of groups emitted (≤ max_groups). Excess shards beyond
   max_groups fold into the final group — same behavior the inline copies
   shipped before this was extracted. */
int shard_group_batch(CollectedHash *batch, int batch_count,
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

int update_schema_conf_splits_streams(const char *db_root, const char *object,
                                             int new_splits, int new_streams) {
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
            /* Format: dir:object:splits:max_key[:storage_version[:streams[:auto_key=...]]].
               Preserve every trailing field so v2 objects don't silently
               downgrade to v1 on a splits change, and so auto_key=... is
               kept across vacuum-reshards. Also walk the line tail for
               any unknown :tok= extension and re-emit it verbatim. */
            line[strcspn(line, "\r\n")] = '\0';
            int cur_splits = 0, max_key = 0;
            int sv = 0, streams = 0;
            int n = sscanf(line + pfxlen, "%d:%d:%d:%d",
                            &cur_splits, &max_key, &sv, &streams);
            int out_splits  = (new_splits  > 0) ? new_splits  : cur_splits;
            int out_streams = (new_streams > 0) ? new_streams : streams;

            /* Walk past splits[:max_key[:sv[:streams]]] to find any trailing
               tokens (auto_key=... and future extensions) — re-emit them. */
            char *scan = line + pfxlen;
            int skips = n > 4 ? 4 : n;
            for (int i = 0; i < skips; i++) {
                char *next = strchr(scan, ':');
                if (!next) { scan = NULL; break; }
                scan = next + 1;
            }
            /* `scan` now points at the byte after the last consumed numeric
               field. If there's more text, it begins with ':' separating
               the next token from the streams field (which we just
               consumed); skip that ':' before re-emitting. Actually no —
               sscanf+strchr leaves scan PAST the colon, pointing at the
               next field's first byte (or NUL if none). So `*scan` is
               the next token, no leading colon. */
            const char *tail = (scan && *scan) ? scan : NULL;

            if (n >= 4) {
                if (tail) fprintf(fout, "%s%d:%d:%d:%d:%s\n", prefix, out_splits, max_key, sv, out_streams, tail);
                else      fprintf(fout, "%s%d:%d:%d:%d\n",    prefix, out_splits, max_key, sv, out_streams);
            } else if (n >= 3) {
                if (tail) fprintf(fout, "%s%d:%d:%d:%s\n", prefix, out_splits, max_key, sv, tail);
                else      fprintf(fout, "%s%d:%d:%d\n",    prefix, out_splits, max_key, sv);
            } else {
                if (tail) fprintf(fout, "%s%d:%d:%s\n", prefix, out_splits, max_key, tail);
                else      fprintf(fout, "%s%d:%d\n",    prefix, out_splits, max_key);
            }
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

/* Defined in config.c — see header doc-comment there. Declared locally
   because config.c's helpers live across the layering boundary and the
   project's convention is to forward-declare at the point of use rather
   than introduce a cross-cutting header for two functions. */
extern int parse_default_modifier(char *type_spec_inout, TypedField *tf);
extern int gen_uuid4_raw(uint8_t out[16]);
extern long long seq_next_val_batch(const char *db_root, const char *object,
                                     const char *seq_name, int n);
extern void encode_field(const TypedField *f, const char *val, uint8_t *out);

/* Parse a single fields.conf-style line "name:type[:param][:default=...]"
   into a TypedField. Returns 1 on success, 0 on failure (bad line). Does
   NOT set offset. Default-modifier suffix is captured on out->default_kind
   / default_val so both add-field's rebuild append and edit-field carry
   the new default through end-to-end. */
int parse_field_line(const char *line, TypedField *out) {
    const char *colon = strchr(line, ':');
    if (!colon || colon == line) return 0;
    size_t nlen = colon - line;
    if (nlen >= 256) nlen = 255;
    memcpy(out->name, line, nlen);
    out->name[nlen] = '\0';
    /* Strip default-modifier suffix into out->default_kind/default_val so
       both add-field's rebuild append AND edit-field carry computed
       defaults through end-to-end. Without this, parse_field_type would
       see "int:default=99" and return FT_NONE (no exact-match), making
       cmd_edit_fields reject every :default= edit. */
    char clean_spec[256];
    strncpy(clean_spec, colon + 1, sizeof(clean_spec) - 1);
    clean_spec[sizeof(clean_spec) - 1] = '\0';
    parse_default_modifier(clean_spec, out);
    parse_field_type(clean_spec, out);
    return out->type != FT_NONE && out->size > 0;
}

/* === v2 (slotcask) rebuild path ===
 *
 * Used by add-fields, vacuum --compact, and vacuum --splits when the object
 * is storage_version=2. Caller holds objlock_wrlock — no concurrent ops can
 * race with the rebuild.
 *
 * Strategy: stage the object's current slotcask data files into a
 * `.legacy/` subdir, open a read-only slotcask handle against that dir,
 * open a fresh write handle in the object root with the new schema, walk
 * legacy via slotcask_walk_live, transform each record (memcpy by
 * new_to_old field map; new fields stay zero from calloc), insert into
 * new. fields.conf and schema.conf rewrites mirror v1. .legacy/ is
 * removed on success; on crash mid-rebuild the operator restores
 * the .legacy contents back into the object root. */
/* Per-new-field backfill state for the rebuild walk. Only populated for
   fields with new_to_old==-1 AND default_kind not in {DK_NONE, DK_AUTO_*}.
   DK_AUTO_CREATE / DK_AUTO_UPDATE stay inert during backfill — they're
   insert/update-time generators, not "stamp on every existing record"
   semantics. (Existing records' creation time is unknown; setting it to
   "now" would lie about history.) */
typedef struct {
    int kind;              /* DK_LITERAL / DK_SEQ / DK_UUID / DK_RANDOM */
    /* DK_SEQ pre-allocated range [seq_start, seq_start+seq_count-1].
       Walk worker atomically claims via fetch_add on seq_next. */
    _Atomic int64_t seq_next;
    int64_t         seq_start;
    int64_t         seq_count;
    /* DK_RANDOM byte count (parsed from default_val once up-front). */
    int random_bytes;
} BackfillSpec;

typedef struct {
    SlotcaskDb        *new_db;
    const TypedSchema *old_ts;
    TypedSchema       *new_ts;
    int               *new_to_old;
    int                slot_changed;
    int                live_count;
    int                skipped;
    int                error;
    /* Per-new-field backfill state; indexed by new_ts field index.
       Entries for fields that don't need backfill have kind=DK_NONE. */
    BackfillSpec      *backfill;     /* malloc'd in rebuild_object_v2; size = new_ts->nfields */
    const char        *db_root;       /* for seq_next_val_batch (DK_SEQ pre-reserve) */
    const char        *object;
} V2RebuildCtx;

/* Re-encode one field's bytes from old size/scale to new. Same-type only;
   caller (cmd_edit_fields) refuses cross-type edits up front.

     FT_VARCHAR  — copy [uint16 BE length][content], clamp to new capacity.
                   varchar shrink that would clip content is rejected
                   pre-flight; here we trust caller.
     FT_INT/LONG/SHORT — decode BE signed, re-encode at new size with sign
                   extension on widen, truncation on narrow. Narrow
                   overflow caught pre-flight.
     FT_NUMERIC  — int64 BE scaled by 10^S. Rescale on S change.
     FT_FLOAT/DOUBLE — natural widen by IEEE 754 cast (caller validates).
     other types — same size required; defensive memcpy. */
void transform_field_value(const TypedField *old_f,
                                   const TypedField *new_f,
                                   const uint8_t *src,
                                   uint8_t *dst) {
    /* No-op if size + scale identical (transform shouldn't have been
       called, but be defensive). */
    if (old_f->size == new_f->size &&
        old_f->numeric_scale == new_f->numeric_scale) {
        memcpy(dst, src, old_f->size);
        return;
    }

    switch (old_f->type) {
    case FT_VARCHAR: {
        uint16_t old_clen = ((uint16_t)src[0] << 8) | (uint16_t)src[1];
        uint16_t new_cap  = (uint16_t)(new_f->size >= 2 ? new_f->size - 2 : 0);
        uint16_t copy_len = old_clen <= new_cap ? old_clen : new_cap;
        dst[0] = (uint8_t)(copy_len >> 8);
        dst[1] = (uint8_t)(copy_len & 0xFF);
        if (copy_len > 0) memcpy(dst + 2, src + 2, copy_len);
        /* trailing slack in dst remains zero from caller's calloc */
        return;
    }
    case FT_INT:
    case FT_LONG:
    case FT_SHORT: {
        int64_t v = (src[0] & 0x80) ? -1 : 0;  /* sign extend */
        for (int i = 0; i < old_f->size; i++) v = (v << 8) | src[i];
        for (int i = new_f->size; i-- > 0; ) {
            dst[i] = (uint8_t)(v & 0xFF);
            v >>= 8;
        }
        return;
    }
    case FT_NUMERIC: {
        int64_t v = (src[0] & 0x80) ? -1 : 0;
        for (int i = 0; i < old_f->size; i++) v = (v << 8) | src[i];
        int delta = new_f->numeric_scale - old_f->numeric_scale;
        if (delta > 0) {
            int64_t mult = 1;
            for (int i = 0; i < delta; i++) mult *= 10;
            v *= mult;
        } else if (delta < 0) {
            int64_t div = 1;
            for (int i = 0; i < -delta; i++) div *= 10;
            v /= div;  /* truncates toward zero, matches Postgres */
        }
        for (size_t i = new_f->size; i-- > 0; ) {
            dst[i] = (uint8_t)(v & 0xFF);
            v >>= 8;
        }
        return;
    }
    case FT_FLOAT: {
        /* float → double widen. Other float edits not supported. */
        if (new_f->type == FT_DOUBLE) {
            uint32_t u = 0;
            for (size_t i = 0; i < 4; i++) u = (u << 8) | src[i];
            float fv;
            memcpy(&fv, &u, 4);
            double dv = (double)fv;
            uint64_t u64;
            memcpy(&u64, &dv, 8);
            for (size_t i = 8; i-- > 0; ) { dst[i] = (uint8_t)(u64 & 0xFF); u64 >>= 8; }
            return;
        }
        memcpy(dst, src, old_f->size < new_f->size ? old_f->size : new_f->size);
        return;
    }
    case FT_ENUM:
        /* 1B → 2B widen: zero-extend the byte index. The existing index
           value stays unchanged (record at byte 0x05 in 1B becomes
           0x00 0x05 in 2B). 2B → 1B narrow is rejected by the enum diff
           validator above; same-width edits short-circuit via
           field_needs_transform=0 before we get here. */
        if (new_f->size == 2 && old_f->size == 1) {
            dst[0] = 0x00;
            dst[1] = src[0];
            return;
        }
        memcpy(dst, src, old_f->size < new_f->size ? old_f->size : new_f->size);
        return;
    default:
        /* Same-size paths fell through above. Anything else: best-effort
           prefix copy. Caller should have refused. */
        memcpy(dst, src, old_f->size < new_f->size ? old_f->size : new_f->size);
        return;
    }
}

/* True if old_f → new_f requires byte-level re-encoding (not just memcpy). */
inline int field_needs_transform(const TypedField *old_f,
                                         const TypedField *new_f) {
    if (old_f->size != new_f->size) return 1;
    if (old_f->type == FT_NUMERIC &&
        old_f->numeric_scale != new_f->numeric_scale) return 1;
    return 0;
}

int v2_rebuild_walk_cb(const uint8_t hash16[16],
                                const void *key, size_t klen,
                                const void *value, size_t vlen,
                                void *ctxp) {
    (void)hash16;
    V2RebuildCtx *ctx = (V2RebuildCtx *)ctxp;
    if (ctx->error) return 1;

    if (!ctx->slot_changed) {
        /* Layout unchanged (e.g., splits-only resplit). Re-insert the
           record bytes verbatim. */
        if (slotcask_insert(ctx->new_db, -1, key, klen, value, vlen) != 0) {
            LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: insert failed for record %d (klen=%zu vlen=%zu), skipping",
                     ctx->live_count + ctx->skipped + 1, klen, vlen);
            ctx->skipped++;
            return 0;
        }
        ctx->live_count++;
        return 0;
    }

    /* Slot layout changed (compact, add-field, edit-field, or any combo):
       recompose typed payload by mapping new field offsets ← old field
       offsets. New fields (new_to_old == -1) get backfilled from
       ctx->backfill[k] when default_kind is set; otherwise they stay
       zero (matches the historical behaviour). Edited fields (size or
       numeric scale changed) go through transform_field_value;
       same-shape fields take the fast memcpy path. */
    uint8_t *buf = calloc(1, ctx->new_ts->total_size);
    if (!buf) { ctx->error = 1; return 1; }
    for (int k = 0; k < ctx->new_ts->nfields; k++) {
        int oi = ctx->new_to_old[k];
        const TypedField *nf = &ctx->new_ts->fields[k];
        if (oi < 0) {
            /* Newly-added field — apply computed default if any.
               DK_AUTO_CREATE / DK_AUTO_UPDATE stay zero by design: they're
               insert/update-time generators, not backfill stamps. The
               original record's creation time is unknown so backfilling
               "now" would lie. Same reasoning for explicit DK_NONE. */
            BackfillSpec *bf = ctx->backfill ? &ctx->backfill[k] : NULL;
            if (!bf || bf->kind == DK_NONE) continue;
            switch (bf->kind) {
            case DK_LITERAL:
                /* Re-use the insert-time encoder so the literal goes
                   through the same type-aware path (varchar length
                   prefix, int BE, numeric scaling, etc.). */
                encode_field(nf, nf->default_val, buf + nf->offset);
                break;
            case DK_SEQ: {
                int64_t v = atomic_fetch_add_explicit(&bf->seq_next, 1,
                                                       memory_order_relaxed);
                /* Render to decimal then run through encode_field so the
                   bytes match what an insert-time DK_SEQ generates
                   (consistency between backfilled and freshly-inserted
                   rows). */
                char numbuf[24];
                snprintf(numbuf, sizeof(numbuf), "%lld", (long long)v);
                encode_field(nf, numbuf, buf + nf->offset);
                break;
            }
            case DK_UUID: {
                uint8_t raw[16];
                if (gen_uuid4_raw(raw) != 0) { free(buf); ctx->error = 1; return 1; }
                if (nf->type == FT_UUID && nf->size == 16) {
                    /* Native uuid type: store raw 16 bytes. */
                    memcpy(buf + nf->offset, raw, 16);
                } else {
                    /* varchar:36 (or wider) — render as dashed 36-char
                       string and run through encode_field so the
                       uint16 length prefix is set. */
                    char uuidstr[37];
                    snprintf(uuidstr, sizeof(uuidstr),
                             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                             raw[0], raw[1], raw[2], raw[3], raw[4], raw[5],
                             raw[6], raw[7], raw[8], raw[9], raw[10], raw[11],
                             raw[12], raw[13], raw[14], raw[15]);
                    encode_field(nf, uuidstr, buf + nf->offset);
                }
                break;
            }
            case DK_RANDOM: {
                /* N raw bytes → 2N hex chars. The pre-flight check in
                   cmd_add_fields already refused a config that would
                   overflow the field's storage. */
                int nbytes = bf->random_bytes;
                if (nbytes <= 0 || nbytes > 256) break; /* defensive */
                uint8_t raw[256];
                if (fill_random(raw, (size_t)nbytes) != 0) {
                    free(buf); ctx->error = 1; return 1;
                }
                char hexbuf[513];
                for (int b = 0; b < nbytes; b++)
                    snprintf(hexbuf + b * 2, 3, "%02x", raw[b]);
                hexbuf[nbytes * 2] = '\0';
                encode_field(nf, hexbuf, buf + nf->offset);
                break;
            }
            default:
                /* DK_AUTO_CREATE / DK_AUTO_UPDATE / DK_NONE — leave zero. */
                break;
            }
            continue;
        }
        size_t off = ctx->old_ts->fields[oi].offset;
        size_t sz  = ctx->old_ts->fields[oi].size;
        if (off + sz > vlen) continue;  /* defensive */
        const TypedField *of = &ctx->old_ts->fields[oi];
        if (field_needs_transform(of, nf)) {
            transform_field_value(of, nf,
                                   (const uint8_t *)value + off,
                                   buf + nf->offset);
        } else {
            memcpy(buf + nf->offset, (const uint8_t *)value + off, sz);
        }
    }
    int rc = slotcask_insert(ctx->new_db, -1, key, klen,
                              buf, ctx->new_ts->total_size);
    free(buf);
    if (rc != 0) {
        LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: insert failed for record %d (klen=%zu), skipping",
                 ctx->live_count + ctx->skipped + 1, klen);
        ctx->skipped++;
        return 0;
    }
    ctx->live_count++;
    return 0;
}

int rebuild_object_v2(const char *db_root, const char *object,
                              const Schema *old_sch, const TypedSchema *old_ts,
                              const Schema *new_sch, TypedSchema *new_ts,
                              int *new_to_old, int slot_changed,
                              int splits_changed, int drop_tombstoned,
                              char added_lines[][256], int n_added) {
    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/data", obj_dir);
    char legacy_dir[PATH_MAX];
    snprintf(legacy_dir, sizeof(legacy_dir), "%s/data.legacy", obj_dir);

    /* Clean any stale data.legacy from a prior crashed rebuild. */
    rmrf(legacy_dir);

    /* Drop the cached slotcask handle so the rename below doesn't tug
       on live mmap regions. The next slotcask_registry_get will
       re-open fresh against the new data/ that we'll create. */
    slotcask_registry_invalidate(db_root, object);

    /* Atomic rename: data/ → data.legacy/. The new slotcask_open below
       will re-create data/ from scratch with the new schema. */
    if (rename(data_dir, legacy_dir) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed: %s",
                data_dir, legacy_dir, strerror(errno));
        OUT("{\"error\":\"Failed to stage legacy data\"}\n");
        return 1;
    }

    /* Open standalone handles — bypass the registry (it would re-open
       the live obj_dir under the new schema and we want both handles
       coexisting for the walk). slotcask_open's data_dir parameter is
       <obj>/, not <obj>/data/, so legacy_db needs the parent of
       data.legacy/ — but the layout helpers prepend /data/ to the
       passed dir, so we point legacy_db at a dir whose /data/ resolves
       to data.legacy/. Symlink would be cleaner; instead we move
       data.legacy → ./data inside a throwaway dir. Simpler: pass the
       parent of data.legacy and have the helpers find data/ — but
       the parent IS obj_dir. Solution: temporarily rename
       data.legacy → data inside a sibling dir. */
    char legacy_root[PATH_MAX];
    snprintf(legacy_root, sizeof(legacy_root), "%s/.rebuild_legacy_root", obj_dir);
    rmrf(legacy_root);
    mkdirp(legacy_root);
    char legacy_data_under_root[PATH_MAX];
    snprintf(legacy_data_under_root, sizeof(legacy_data_under_root),
             "%s/data", legacy_root);
    if (rename(legacy_dir, legacy_data_under_root) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s): %s",
                legacy_dir, legacy_data_under_root, strerror(errno));
        rmrf(legacy_root);
        OUT("{\"error\":\"Failed to stage legacy data root\"}\n");
        return 1;
    }

    SlotcaskDb legacy_db, new_db;
    int legacy_open = (slotcask_open(&legacy_db, legacy_root,
                                       old_sch->splits, old_sch->streams,
                                       old_sch->slot_size) == 0);
    int new_open = (slotcask_open(&new_db, obj_dir,
                                    new_sch->splits, new_sch->streams,
                                    new_sch->slot_size) == 0);
    if (!legacy_open || !new_open) {
        if (legacy_open) slotcask_close(&legacy_db);
        if (new_open)    slotcask_close(&new_db);
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: open failed (legacy=%d new=%d)",
                legacy_open, new_open);
        OUT("{\"error\":\"Failed to open slotcask handles for rebuild\"}\n");
        return 1;
    }

    V2RebuildCtx walk_ctx = {0};
    walk_ctx.new_db        = &new_db;
    walk_ctx.old_ts        = old_ts;
    walk_ctx.new_ts        = new_ts;
    walk_ctx.new_to_old    = new_to_old;
    walk_ctx.slot_changed  = slot_changed;
    walk_ctx.db_root       = db_root;
    walk_ctx.object        = object;

    /* Pre-flight an existing live count so DK_SEQ ranges can be reserved
       up-front (one flock per field rather than per record). The walk
       below is sequential so this count is exact at walk time. */
    long long pf_live = 0;
    if (slot_changed) {
        uint64_t pf_total = 0, pf_deleted = 0;
        if (slotcask_sum_kf_totals(&legacy_db, &pf_total, &pf_deleted) == 0) {
            pf_live = (long long)(pf_total - pf_deleted);
        }
    }

    /* Allocate per-new-field backfill specs. Only fields with new_to_old==-1
       AND a computed default kind need anything; the rest stay DK_NONE. */
    walk_ctx.backfill = NULL;
    if (slot_changed && new_ts->nfields > 0) {
        walk_ctx.backfill = (BackfillSpec *)calloc((size_t)new_ts->nfields,
                                                    sizeof(BackfillSpec));
        if (!walk_ctx.backfill) {
            slotcask_close(&legacy_db);
            slotcask_close(&new_db);
            OUT("{\"error\":\"Failed to allocate backfill state\"}\n");
            return 1;
        }
        for (int k = 0; k < new_ts->nfields; k++) {
            if (new_to_old[k] != -1) continue;  /* not a new field */
            const TypedField *nf = &new_ts->fields[k];
            walk_ctx.backfill[k].kind = nf->default_kind;
            if (nf->default_kind == DK_SEQ && pf_live > 0) {
                long long start = seq_next_val_batch(db_root, object,
                                                      nf->default_val,
                                                      (int)pf_live);
                if (start < 0) {
                    free(walk_ctx.backfill);
                    slotcask_close(&legacy_db);
                    slotcask_close(&new_db);
                    OUT("{\"error\":\"sequence unavailable for backfill of field [%s]\"}\n",
                        nf->name);
                    return 1;
                }
                walk_ctx.backfill[k].seq_start = start;
                walk_ctx.backfill[k].seq_count = pf_live;
                atomic_store_explicit(&walk_ctx.backfill[k].seq_next,
                                       start, memory_order_relaxed);
            } else if (nf->default_kind == DK_RANDOM) {
                walk_ctx.backfill[k].random_bytes = atoi(nf->default_val);
            }
        }
    }

    slotcask_walk_live(&legacy_db, v2_rebuild_walk_cb, &walk_ctx);
    int live_count = walk_ctx.live_count;
    int skipped    = walk_ctx.skipped;
    int walk_err   = walk_ctx.error;
    if (skipped > 0)
        LOG_WARN(LOG_SUB_CONFIG, "rebuild_v2: skipped %d records due to insert failure", skipped);
    free(walk_ctx.backfill);
    walk_ctx.backfill = NULL;

    slotcask_close(&legacy_db);
    slotcask_close(&new_db);

    if (walk_err) {
        LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: walk error after %d records; restoring original data",
                  live_count);
        rmrf(data_dir);
        if (rename(legacy_data_under_root, data_dir) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: restore failed: %s — original at %s",
                      strerror(errno), legacy_root);
            OUT("{\"error\":\"Rebuild walk failed; restore also failed — original at .rebuild_legacy_root\"}\n");
        } else {
            rmrf(legacy_root);
            OUT("{\"error\":\"Rebuild walk failed; original data restored\"}\n");
        }
        return 1;
    }

    /* Stage fields.conf rewrite (drop :removed lines if compact, append
       n_added at the end) — mirrors the v1 fields_changed branch. */
    int fields_changed = drop_tombstoned || n_added > 0;
    if (fields_changed) {
        char fpath[PATH_MAX], fpath_new[PATH_MAX], fpath_old[PATH_MAX];
        snprintf(fpath,     sizeof(fpath),     "%s/fields.conf", obj_dir);
        snprintf(fpath_new, sizeof(fpath_new), "%s/fields.conf.new", obj_dir);
        snprintf(fpath_old, sizeof(fpath_old), "%s/fields.conf.old", obj_dir);

        FILE *fin = fopen(fpath, "r");
        FILE *fout = fopen(fpath_new, "w");
        if (!fin || !fout) {
            if (fin) fclose(fin);
            if (fout) fclose(fout);
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
        for (int a = 0; a < n_added; a++) fprintf(fout, "%s\n", added_lines[a]);
        fclose(fin);
        fclose(fout);
        if (rename(fpath, fpath_old) != 0)
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed", fpath, fpath_old);
        if (rename(fpath_new, fpath) != 0) {
            LOG_ERROR(LOG_SUB_CONFIG, "rebuild_v2: rename(%s → %s) failed — restoring", fpath_new, fpath);
            (void)rename(fpath_old, fpath);
            OUT("{\"error\":\"Failed to swap fields.conf\"}\n");
            return 1;
        }
        unlink(fpath_old);
    }

    int streams_changed = (new_sch->streams != old_sch->streams);
    if (splits_changed || streams_changed)
        update_schema_conf_splits_streams(db_root, object,
                                           splits_changed  ? new_sch->splits  : 0,
                                           streams_changed ? new_sch->streams : 0);

    invalidate_schema_caches(db_root, object);
    invalidate_idx_cache(db_root, object);
    reset_deleted_count(db_root, object);
    set_count(db_root, object, live_count);

    /* Drop the legacy data root + force the registry to re-open against
       the new on-disk state on the next request. */
    rmrf(legacy_root);
    slotcask_registry_invalidate(db_root, object);

    int idx_rebuilt = 0;
    if (splits_changed) idx_rebuilt = reindex_object(db_root, object, 0);

    LOG_AUDIT(LOG_SUB_CONFIG, "REBUILD-V2 %s/%s: live=%d, splits=%d→%d, streams=%d→%d, slot_size=%d→%d, compact=%d, idx_rebuilt=%d",
            db_root, object, live_count, old_sch->splits, new_sch->splits,
            old_sch->streams, new_sch->streams,
            old_sch->slot_size, new_sch->slot_size, drop_tombstoned, idx_rebuilt);
    OUT("{\"status\":\"rebuilt\",\"live\":%d,\"splits\":%d,\"streams\":%d,\"slot_size\":%d,\"compact\":%s,\"indexes_rebuilt\":%d}\n",
        live_count, new_sch->splits, new_sch->streams, new_sch->slot_size,
        drop_tombstoned ? "true" : "false", idx_rebuilt);
    return 0;
}

int rebuild_object(const char *db_root, const char *object,
                   int new_splits_arg, int drop_tombstoned,
                   char added_lines[][256], int n_added,
                   int new_streams_arg) {
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

    /* streams change is a no-op for v1 (no streams concept). For v2, the
       caller (typically vacuum) passes the desired stream count. 0 = keep. */
    int old_streams = old_sch.streams;
    int new_streams = (new_streams_arg > 0 )
                      ? new_streams_arg : old_streams;
    int streams_changed = (new_streams != old_streams);

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
    new_sch.streams = new_streams;
    new_sch.max_value = new_ts.total_size;
    /* slot_size = 24B inline header + max_key + max_value, rounded to 8, floor 32. */
    new_sch.slot_size = (24 + new_sch.max_key + new_sch.max_value + 7) & ~7;
    if (new_sch.slot_size < 32) new_sch.slot_size = 32;

    /* slot_changed drives v2_rebuild_walk_cb's "recompose typed payload"
       branch vs. the fast "verbatim re-insert" branch. Any add-field call
       must force the recompose path even when the new fields happen to
       fit within the existing 8-byte-aligned slot_size — without that,
       the new fields are never written (verbatim re-insert truncates at
       the old vlen) AND computed defaults are never stamped. */
    int slot_changed = (new_sch.slot_size != old_sch.slot_size) || (n_added > 0);

    /* Nothing to do — caller probably called rebuild without flags */
    if (!splits_changed && !slot_changed && !streams_changed && n_added == 0) {
        OUT("{\"status\":\"noop\",\"reason\":\"no change requested\"}\n");
        return 0;
    }

    /* v2 path runs an entirely separate rebuild over slotcask files. */
    return rebuild_object_v2(db_root, object, &old_sch, old_ts,
                              &new_sch, &new_ts, new_to_old,
                              slot_changed, splits_changed,
                              drop_tombstoned, added_lines, n_added);
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

int cmd_exists(const char *db_root, const char *object,
               const char *key, size_t klen) {
    Schema sc = load_schema(db_root, object);

    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("false\n"); return 1; }
    int rc = slotcask_exists(sdb, key, klen);
    OUT("%s\n", rc == 1 ? "true" : "false");
    return rc == 1 ? 0 : 1;
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
    /* For v2 with a small caller-set limit, route through the streaming
       per-shard walker — Pass-1 ref-buffering wastes time collecting refs
       a limit-bound caller never reads. The threshold (1000) is well below
       typical full-scan workloads but covers the common KEYS first N case
       and admin previews. */
    int use_streaming = (limit > 0 && limit <= 1000);
    if (use_streaming) {
        SlotcaskSchemaInfo info = {
            .splits = sch.splits, .slot_size = sch.slot_size,
            .streams = sch.streams,
        };
        SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
        if (sdb) scan_shards_v2_streaming(sdb, keys_cb, &ctx);
        else     scan_dispatch(db_root, object, &sch, data_dir, keys_cb, &ctx);
    } else {
        scan_dispatch(db_root, object, &sch, data_dir, keys_cb, &ctx);
    }
    pthread_mutex_destroy(&ctx.lock);
    if (!csv_delim) OUT("]\n");
    return 0;
}

/* ========== FETCH (paginated scan with optional field projection) ========== */

/* Render block's key bytes as the wire-form string, honouring the
   object's auto_key mode (carried on FieldSchema). For AK_NONE the
   output is a verbatim NUL-terminated copy; for AK_UUID the 16 bytes
   render as 36-char dashed; for AK_SEQ the 8 bytes render as decimal. */
static void render_wire_key(const FieldSchema *fs, const uint8_t *block,
                             uint16_t klen, char out[1100]) {
    const Schema *sc = (fs && fs->auto_key != AK_NONE)
                        ? &fs->auto_key_schema_snapshot : NULL;
    format_wire_key(sc, (const char *)block, klen, out, 1100);
}

/* Print a record as JSON with optional projection */
void print_record_json(const SlotHeader *hdr, const uint8_t *block,
                              const char **proj_fields, int proj_count,
                              int *printed, FieldSchema *fs) {
    char key[1100];
    render_wire_key(fs, block, hdr->key_len, key);

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s{\"key\":\"%s\",\"value\":", *printed ? "," : "", key);
    if (proj_count > 0) {
        OUT("{");
        int first = 1;
        for (int i = 0; i < proj_count; i++) {
            char *pv = json_escape_field(decode_field(raw, hdr->value_len, proj_fields[i], fs));
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
    (*printed)++;
}

/* Emit a record as a dict entry: "key":<value-json> (with leading comma when needed) */
void print_record_dict(const SlotHeader *hdr, const uint8_t *block,
                       const char **proj_fields, int proj_count,
                       int *printed, FieldSchema *fs) {
    char key[1100];
    render_wire_key(fs, block, hdr->key_len, key);

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s\"%s\":", *printed ? "," : "", key);
    if (proj_count > 0) {
        OUT("{");
        int first = 1;
        for (int i = 0; i < proj_count; i++) {
            char *pv = json_escape_field(decode_field(raw, hdr->value_len, proj_fields[i], fs));
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
    (*printed)++;
}

/* Emit a record as a JSON array row: ["key","v1","v2",...] */
void print_record_row(const SlotHeader *hdr, const uint8_t *block,
                      const char **proj_fields, int proj_count,
                      int *printed, FieldSchema *fs) {
    char key[1100];
    render_wire_key(fs, block, hdr->key_len, key);

    const char *raw = (const char *)block + hdr->key_len;

    OUT("%s[\"%s\"", *printed ? "," : "", key);
    if (proj_count > 0) {
        for (int i = 0; i < proj_count; i++) {
            char *pv = json_escape_field(decode_field(raw, hdr->value_len, proj_fields[i], fs));
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    } else if (fs && fs->ts) {
        for (int i = 0; i < fs->ts->nfields; i++) {
            if (fs->ts->fields[i].removed) continue;
            char *pv = json_escape_field(typed_get_field_str(fs->ts, (const uint8_t *)raw, (int)hdr->value_len, i));
            OUT(",\"%s\"", pv ? pv : "");
            free(pv);
        }
    }
    OUT("]");
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
void csv_emit_header(const char **proj_fields, int proj_count,
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
void csv_emit_row(const char *key, const uint8_t *raw, size_t val_len,
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
            char *v = typed_get_field_str(fs->ts, raw, (int)val_len, i);
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

/* === v2 cmd_fetch worker ===
 *
 * v1's cursor encodes `(shard_path_idx, slot_idx)` and resumes mid-walk; v2
 * has no analogous addressable position (slotcask_walk_live walks all kf
 * shards). The v2 cursor is a simple `"N"` integer = how many live records
 * were already returned by prior pages. Resume = skip N entries before
 * emitting. Stable across calls because slotcask_walk_live's order is
 * deterministic given an unchanged dataset. Concurrent inserts can shift
 * the slot a record lives in but the per-kf-shard scan itself is
 * deterministic for read-only iteration. */
typedef struct {
    int     csv_delim;
    int     rows_fmt;
    int     dict_fmt;
    int     to_skip;            /* records still to skip (cursor + offset combined) */
    int     limit;
    int     printed;            /* emitted so far (this call) */
    int     scanned_so_far;     /* live records observed (skipped + emitted) */
    const char **proj_fields;
    int     proj_count;
    FieldSchema *fs;
} V2FetchCtx;

static int v2_fetch_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *ctxp) {
    (void)hash16;
    V2FetchCtx *ctx = (V2FetchCtx *)ctxp;
    ctx->scanned_so_far++;
    if (ctx->to_skip > 0) { ctx->to_skip--; return 0; }
    if (ctx->printed >= ctx->limit) return 1;

    /* Reuse the existing v1 emit helpers — they take a SlotHeader + block
       pointer where block = key bytes followed by value bytes. Synthesize a
       SlotHeader for v2 (flag=1, klen, vlen) and compose the block in a
       small heap buffer. */
    SlotHeader hdr = {0};
    memcpy(hdr.hash, hash16, 16);
    hdr.flag = 1;
    hdr.key_len = (uint16_t)klen;
    hdr.value_len = (uint32_t)vlen;
    uint8_t stk[2048];
    uint8_t *block = (klen + vlen + 1 < sizeof(stk)) ? stk : malloc(klen + vlen);
    if (!block) return 1;
    memcpy(block, key, klen);
    memcpy(block + klen, value, vlen);

    if (ctx->csv_delim) {
        char kbuf[1024];
        size_t kl = klen < sizeof(kbuf) - 1 ? klen : sizeof(kbuf) - 1;
        memcpy(kbuf, key, kl); kbuf[kl] = '\0';
        csv_emit_row(kbuf, block + klen, (uint32_t)vlen,
                      ctx->proj_count > 0 ? ctx->proj_fields : NULL,
                      ctx->proj_count, ctx->fs, (char)ctx->csv_delim);
        ctx->printed++;
    } else if (ctx->rows_fmt) {
        print_record_row(&hdr, block, ctx->proj_fields, ctx->proj_count,
                          &ctx->printed, ctx->fs);
    } else if (ctx->dict_fmt) {
        OUT("%s\"%.*s\":", ctx->printed ? "," : "", (int)klen, (const char *)key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen) : NULL;
        OUT("%s", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    } else {
        OUT("%s{\"key\":\"%.*s\",\"value\":", ctx->printed ? "," : "",
            (int)klen, (const char *)key);
        char *decoded = ctx->fs ? typed_decode(ctx->fs->ts,
                                                (const uint8_t *)value, vlen) : NULL;
        OUT("%s}", decoded ? decoded : "null");
        free(decoded);
        ctx->printed++;
    }

    if (block != stk) free(block);
    return 0;
}

static int cmd_fetch_v2(const char *db_root, const char *object,
                         int offset, int limit, const char *proj_str,
                         const char *cursor, const char *format,
                         const char *delimiter, const Schema *sch,
                         int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    if (limit <= 0) limit = g_global_limit;

    FieldSchema fs_fetch;
    init_field_schema(&fs_fetch, db_root, object);
    FieldSchema *fs_ptr = (fs_fetch.ts || fs_fetch.nfields > 0) ? &fs_fetch : NULL;

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

    int cursor_n = 0;
    if (cursor && cursor[0]) cursor_n = atoi(cursor);

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);

    if (csv_delim) {
        if (want_total) {
            OUT("{\"error\":\"\\\"total\\\" with format=csv is not supported\"}\n");
            return -1;
        }
        csv_emit_header(proj_count > 0 ? proj_fields : NULL, proj_count, fs_ptr, csv_delim);
    } else if (rows_fmt)
        emit_rows_columns(proj_fields, proj_count, fs_ptr);
    else if (dict_fmt)
        OUT(want_total ? "{\"rows\":{" : "{\"results\":{");
    else
        OUT(want_total ? "{\"rows\":[" : "{\"results\":[");

    V2FetchCtx fctx = {0};
    fctx.csv_delim = csv_delim;
    fctx.rows_fmt = rows_fmt;
    fctx.dict_fmt = dict_fmt;
    fctx.to_skip = 0;        /* skip handled by walk_live_skip below */
    fctx.limit = limit;
    fctx.proj_fields = proj_fields;
    fctx.proj_count = proj_count;
    fctx.fs = fs_ptr;
    int64_t skip_n = (int64_t)cursor_n + (offset > 0 ? offset : 0);
    /* slotcask_walk_live_skip cheaply skips past the first N live
       records (no segcache touch) before invoking the callback —
       offset=5000 went from ~5ms (per-record segcache_acquire even on
       skip) to <1ms. */
    if (sdb) slotcask_walk_live_skip(sdb, skip_n, v2_fetch_cb, &fctx);

    if (csv_delim || rows_fmt) {
        /* No JSON wrapper. CSV / rows formats stream as-is. */
    } else if (want_total) {
        /* total mode: emit {"rows":[...],"total":null} — cursor is omitted */
        const char *close = dict_fmt ? "}" : "]";
        OUT("%s,\"total\":null}\n", close);
    } else {
        const char *close = dict_fmt ? "}" : "]";
        if (fctx.printed >= limit) {
            int next_cur = cursor_n + fctx.printed;
            OUT("%s,\"cursor\":\"%d\"}\n", close, next_cur);
        } else {
            OUT("%s,\"cursor\":null}\n", close);
        }
    }
    return 0;
}

/* Cursor-based fetch — scans from cursor position, returns next cursor.
   Cursor format: "shard_path_idx:slot_idx" or empty for start.
   Response: {"results":[...],"cursor":"..."} */
int cmd_fetch(const char *db_root, const char *object,
                     int offset, int limit, const char *proj_str,
                     const char *cursor, const char *format,
                     const char *delimiter, int want_total) {
    int rows_fmt = (format && strcmp(format, "rows") == 0);
    int dict_fmt = (format && strcmp(format, "dict") == 0);
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    (void)rows_fmt; (void)dict_fmt; (void)csv_delim;
    if (limit <= 0) limit = g_global_limit;
    Schema sch = load_schema(db_root, object);
    return cmd_fetch_v2(db_root, object, offset, limit, proj_str, cursor,
                         format, delimiter, &sch, want_total);
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
    if (!valid_filename(filename)) {
        OUT("{\"error\":\"invalid filename\"}\n");
        return 1;
    }
    OUT("{\"path\":\"%s/%s/files/%s\"}\n", db_root, object, filename);
    return 0;
}

/* Compute the on-disk destination path for a filename. Caller supplies buffers.
   Files live flat under <object>/files/ — basename is the lookup key, no
   hashing or sub-bucketing. (Pre-2026.05.2 used XX/XX hash buckets; the
   ./migrate binary's migrate_files() pass lifts those into place.) */
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


/* ========== LIST FILES ==========
   Walks <db_root>/<object>/files/, optionally filters by pattern + match
   mode, and returns a sorted page. Files live flat — basename is the
   lookup key; no bucketing. Pre-2026.05.2 used a 2-level XX/XX hash
   bucket tree, lifted in place by the ./migrate binary's migrate_files()
   pass. Pagination is offset/limit on a stable alphabetical sort. */

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
        char *esc = json_escape_const(names[i]);
        OUT("\"%s\"", esc ? esc : names[i]);
        free(esc);
        emitted++;
    }
    OUT("],\"total\":%zu,\"offset\":%d,\"limit\":%d}\n", count, offset, limit);

    for (size_t i = 0; i < count; i++) free(names[i]);
    free(names);
    return 0;
}

/* ========== CREATE OBJECT ========== */

