#include "types.h"
#include "slotcask.h"

/* Hashing & shard derivation live in util.c (compute_hash_raw) and
   slotcask.c (compute_record_shard) — single source of truth across the
   engine and storage layer. */

/* Canonical layout for per-shard indexes:
       <db_root>/<object>/indexes/<field>/<NNN>.idx
   where NNN is 3 hex digits matching the data shard filename pattern.
   Composite indexes (field name contains '+') get the literal name as
   the directory; the path-encoded form is fine on POSIX filesystems. */
void build_idx_path(char *buf, size_t buflen,
                           const char *db_root, const char *object,
                           const char *field, int idx_shard_id) {
    snprintf(buf, buflen, "%s/%s/indexes/%s/%03x.idx",
             db_root, object, field, idx_shard_id & 0xFFF);
}

/* ========== Pre-allocation ========== */

/* Schema format: dir:object:splits:max_key (max_value derived from
   fields.conf, slot_size = max_key + max_value rounded to 8). */

/* ========== Record Count ==========
   Both live and deleted counts share one file ($obj/metadata/counts)
   formatted as "<live> <deleted>\n", protected by a single counts.lock.
   Collapses two flock cycles per delete into one. */

static void counts_paths(char *cpath, char *lpath, const char *db_root, const char *object) {
    char mdir[PATH_MAX];
    snprintf(mdir, sizeof(mdir), "%s/%s/metadata", db_root, object);
    mkdirp(mdir);
    snprintf(cpath, PATH_MAX, "%s/counts", mdir);
    snprintf(lpath, PATH_MAX, "%s/counts.lock", mdir);
}

static void counts_read_locked(const char *cpath, int *live, int *del) {
    *live = 0; *del = 0;
    FILE *f = fopen(cpath, "r");
    if (!f) return;
    /* Short read or parse failure → leave both as 0 (the function's
       expected default for missing/empty/corrupt counts files). */
    if (fscanf(f, "%d %d", live, del) != 2) { *live = 0; *del = 0; }
    fclose(f);
}

static void counts_write_locked(const char *cpath, int live, int del) {
    FILE *f = fopen(cpath, "w");
    if (!f) return;
    fprintf(f, "%d %d\n", live, del);
    fclose(f);
}

/* ========== In-memory counts cache ==========
 *
 * The on-disk counts file (text "<live> <deleted>\n") was being
 * read+written under flock on every insert/delete — ~9 syscalls per call,
 * ~24 µs single-thread. For single-conn DELETE x10K that was the
 * dominant cost (DELETE 17k op/s vs UPDATE 29k op/s in bench-kv).
 *
 * Now: per-object atomic int64s in a process-wide hash table. update at
 * insert/delete is a single atomic_fetch_add. Reads (cmd_size, etc.) hit
 * the atomic load. Disk file is the persistence layer — flushed on
 * demand via counts_flush() (called from shutdown / vacuum / recount /
 * server stop, plus opportunistically every FLUSH_INTERVAL atomic
 * updates). Counts may be slightly stale on crash; recount rebuilds.
 */
/* CountsCacheEntry, COUNTS_CACHE_BUCKETS moved to shard_db_internal.h;
   g_counts_cache, g_counts_lock moved to ShardDb struct */
#define COUNTS_FLUSH_INTERVAL 10000   /* atomic updates between auto-flushes */

static unsigned counts_hash_path(const char *p) {
    unsigned h = 5381;
    while (*p) h = ((h << 5) + h) + (unsigned char)(*p++);
    return h;
}

/* Lock-free lookup — entries are write-once after install (we never
   evict). Read used with acquire ordering to pair with the
   release-store at install time, then strcmp the path. Hot path on
   bench-kv DELETE x10000: was 10K mutex_lock/unlock pairs, now zero. */
static CountsCacheEntry *counts_cache_lookup_lockfree(const char *path) {
    unsigned h = counts_hash_path(path);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int idx = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        int u = atomic_load_explicit(&g_counts_cache[idx].used,
                                       memory_order_acquire);
        if (!u) return NULL;
        if (strcmp(g_counts_cache[idx].path, path) == 0)
            return &g_counts_cache[idx];
    }
    return NULL;
}

/* Slow path — used at install time only. Caller holds g_counts_lock. */
static CountsCacheEntry *counts_cache_lookup_locked(const char *path) {
    unsigned h = counts_hash_path(path);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int idx = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        if (!g_counts_cache[idx].used) return NULL;
        if (strcmp(g_counts_cache[idx].path, path) == 0)
            return &g_counts_cache[idx];
    }
    return NULL;
}

static CountsCacheEntry *counts_cache_get(const char *path) {
    /* Hot path: try lock-free lookup first. */
    CountsCacheEntry *e = counts_cache_lookup_lockfree(path);
    if (e) return e;

    /* Slow path: take the mutex, install. */
    pthread_mutex_lock(&g_counts_lock);
    e = counts_cache_lookup_locked(path);
    if (e) { pthread_mutex_unlock(&g_counts_lock); return e; }

    /* Install — find empty slot, init from disk. */
    unsigned h = counts_hash_path(path);
    int idx = -1;
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        int probe = (int)((h + (unsigned)i) % COUNTS_CACHE_BUCKETS);
        if (!g_counts_cache[probe].used) { idx = probe; break; }
    }
    if (idx < 0) {
        /* Cache full — fall back to direct file I/O via a NULL return.
           Callers handle this gracefully. */
        LOG_WARN(LOG_SUB_SLOTCASK, "counts_cache_get %s: cache full (COUNTS_CACHE_BUCKETS exhausted), falling back to direct file I/O", path);
        pthread_mutex_unlock(&g_counts_lock);
        return NULL;
    }

    int live = 0, del = 0;
    counts_read_locked(path, &live, &del);
    strncpy(g_counts_cache[idx].path, path, PATH_MAX - 1);
    g_counts_cache[idx].path[PATH_MAX - 1] = '\0';
    atomic_init(&g_counts_cache[idx].live, (int64_t)live);
    atomic_init(&g_counts_cache[idx].deleted, (int64_t)del);
    atomic_init(&g_counts_cache[idx].pending_writes, 0);
    /* Release-store on used so the lock-free reader's acquire-load sees
       the path + atomics fully initialised before observing used=1. */
    atomic_store_explicit(&g_counts_cache[idx].used, 1, memory_order_release);
    pthread_mutex_unlock(&g_counts_lock);
    return &g_counts_cache[idx];
}

/* Persist current cached counts back to disk. Called on shutdown and
   periodically from update_counts after FLUSH_INTERVAL ops. */
static void counts_flush_entry(const char *cpath, const char *lpath,
                                CountsCacheEntry *e) {
    int live = (int)atomic_load_explicit(&e->live, memory_order_relaxed);
    int del  = (int)atomic_load_explicit(&e->deleted, memory_order_relaxed);
    int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
    if (lockfd < 0) return;
    flock(lockfd, LOCK_EX);
    counts_write_locked(cpath, live, del);
    flock(lockfd, LOCK_UN);
    close(lockfd);
    atomic_store_explicit(&e->pending_writes, 0, memory_order_relaxed);
}

void counts_flush(const char *db_root, const char *object) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    pthread_mutex_lock(&g_counts_lock);
    CountsCacheEntry *e = counts_cache_lookup_locked(cpath);
    if (e) counts_flush_entry(cpath, lpath, e);
    pthread_mutex_unlock(&g_counts_lock);
}

/* Drop the in-memory entry for an object — used after drop-object /
   create-object so a recreated object starts from on-disk state instead
   of inheriting the stale cached counters. Does NOT touch the disk file
   (drop-object's rmrf removes that). */
void counts_invalidate(const char *db_root, const char *object) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    pthread_mutex_lock(&g_counts_lock);
    CountsCacheEntry *e = counts_cache_lookup_locked(cpath);
    if (e) {
        /* Atomic store so concurrent lock-free readers see used=0
           promptly (and skip this entry on subsequent lookups). */
        atomic_store_explicit(&e->used, 0, memory_order_release);
        e->path[0] = '\0';
        atomic_store_explicit(&e->live, 0, memory_order_relaxed);
        atomic_store_explicit(&e->deleted, 0, memory_order_relaxed);
        atomic_store_explicit(&e->pending_writes, 0, memory_order_relaxed);
    }
    pthread_mutex_unlock(&g_counts_lock);
}

/* Flush every cached entry. Called from server-shutdown paths so the
   on-disk counts file is up-to-date when the daemon stops cleanly. */
void counts_flush_all(void) {
    pthread_mutex_lock(&g_counts_lock);
    for (int i = 0; i < COUNTS_CACHE_BUCKETS; i++) {
        if (!atomic_load_explicit(&g_counts_cache[i].used,
                                    memory_order_relaxed)) continue;
        char lpath[PATH_MAX];
        snprintf(lpath, PATH_MAX, "%s.lock", g_counts_cache[i].path);
        counts_flush_entry(g_counts_cache[i].path, lpath, &g_counts_cache[i]);
    }
    pthread_mutex_unlock(&g_counts_lock);
}

/* Apply deltas to both counts. Atomic in-memory; opportunistic flush to
   disk every COUNTS_FLUSH_INTERVAL updates. Drops the per-call flock +
   open + read + write + close + funlock cycle the original did. */
static void update_counts(const char *db_root, const char *object, int live_delta, int del_delta) {
    char cpath[PATH_MAX], lpath[PATH_MAX];
    counts_paths(cpath, lpath, db_root, object);
    CountsCacheEntry *e = counts_cache_get(cpath);
    if (!e) {
        /* Cache full — fall back to direct file I/O so the counts still
           progress (slow path; bumping COUNTS_CACHE_BUCKETS is the fix). */
        int lockfd = open(lpath, O_RDWR | O_CREAT, 0644);
        if (lockfd < 0) return;
        flock(lockfd, LOCK_EX);
        int live, del;
        counts_read_locked(cpath, &live, &del);
        live += live_delta; if (live < 0) live = 0;
        del  += del_delta;  if (del  < 0) del  = 0;
        counts_write_locked(cpath, live, del);
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return;
    }

    if (live_delta) {
        int64_t after = atomic_fetch_add_explicit(&e->live, (int64_t)live_delta,
                                                    memory_order_relaxed)
                        + (int64_t)live_delta;
        if (after < 0) atomic_store_explicit(&e->live, 0, memory_order_relaxed);
    }
    if (del_delta) {
        int64_t after = atomic_fetch_add_explicit(&e->deleted, (int64_t)del_delta,
                                                    memory_order_relaxed)
                        + (int64_t)del_delta;
        if (after < 0) atomic_store_explicit(&e->deleted, 0, memory_order_relaxed);
    }

    /* Opportunistic flush. */
    uint64_t p = atomic_fetch_add_explicit(&e->pending_writes, 1,
                                             memory_order_relaxed) + 1;
    if (p % COUNTS_FLUSH_INTERVAL == 0) {
        counts_flush_entry(cpath, lpath, e);
    }
}

/* The slotcask kf header is the source of truth for record counts —
   slotcask_put / slotcask_delete update it atomically under the kf-shard
   wrlock. The four mutators below remain as no-ops so existing callers
   (bulk-insert / vacuum / truncate / etc.) keep their bookkeeping shape
   without forcing every site to dispatch on storage layout. */
void update_count(const char *db_root, const char *object, int delta) {
    (void)db_root; (void)object; (void)delta;
}

void update_deleted_count(const char *db_root, const char *object, int delta) {
    (void)db_root; (void)object; (void)delta;
}

void set_count(const char *db_root, const char *object, int count) {
    (void)db_root; (void)object; (void)count;
}

void reset_deleted_count(const char *db_root, const char *object) {
    (void)db_root; (void)object;
}

/* Resolve (live, deleted) for an object given an already-loaded Schema --
   skips the load_schema() lookup for callers that already have one (e.g.
   auto_reshard_thread, which needs sch.splits for its own comparison
   regardless). Sums kf headers — each is updated atomically inside
   slotcask_put / slotcask_delete and is the single source of truth for
   record counts (cannot go stale across daemon crashes the way a
   separate counts file would).

   Callers pass `object` in two forms historically:
     1. (db_root, "object")          — most call sites
     2. (db_root, "dir/object")      — describe-object, list-objects, list-dirs
   slotcask_registry_get expects (effective_root, bare_object) so we split
   joined form here. */
static int resolve_counts_with_schema(const char *db_root, const char *object,
                                       const Schema *sc,
                                       uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(eff_root, bare_obj, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "resolve_counts %s/%s: slotcask_registry_get failed", eff_root, bare_obj);
        *out_live = 0; *out_deleted = 0; return -1;
    }
    uint64_t total = 0, deleted = 0;
    if (slotcask_sum_kf_totals(sdb, &total, &deleted) != 0) {
        LOG_ERROR(LOG_SUB_SLOTCASK,
            "resolve_counts %s/%s: failed to read kf totals",
            eff_root, bare_obj);
        *out_live = 0;
        *out_deleted = 0;
        return -1;
    }
    *out_live    = total > deleted ? total - deleted : 0;
    *out_deleted = deleted;
    return 0;
}

static int resolve_counts(const char *db_root, const char *object,
                          uint64_t *out_live, uint64_t *out_deleted) {
    char eff_root[PATH_MAX];
    const char *bare_obj;
    const char *slash = strchr(object, '/');
    if (slash) {
        size_t dir_len = (size_t)(slash - object);
        snprintf(eff_root, sizeof(eff_root), "%s/%.*s",
                 db_root, (int)dir_len, object);
        bare_obj = slash + 1;
    } else {
        snprintf(eff_root, sizeof(eff_root), "%s", db_root);
        bare_obj = object;
    }
    Schema sc = load_schema(eff_root, bare_obj);
    return resolve_counts_with_schema(db_root, object, &sc, out_live, out_deleted);
}

int get_deleted_count(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (int)del;
}

int get_live_count(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (int)live;
}

/* Full-width counterpart to get_live_count() (above) — no (int)
   narrowing. get_live_count() itself is left untouched, since its
   existing call sites operate on in-memory result sets already bounded
   by QUERY_BUFFER_MB / int-sized offsets, where the narrowing is
   harmless. Callers that compare against multi-billion-record
   thresholds (auto-reshard) must use this one instead. */
long long get_live_count_ll(const char *db_root, const char *object) {
    uint64_t live = 0, del = 0;
    resolve_counts(db_root, object, &live, &del);
    return (long long)live;
}

/* Schema-aware sibling of get_live_count_ll() — for callers that already
   have a freshly-loaded Schema (avoids a redundant load_schema() call on
   hot per-object sweep paths, e.g. auto_reshard_thread). */
long long get_live_count_ll_for_schema(const char *db_root, const char *object,
                                        const Schema *sc) {
    uint64_t live = 0, del = 0;
    resolve_counts_with_schema(db_root, object, sc, &live, &del);
    return (long long)live;
}

/* Forward declaration */
int is_number(const char *s);

/* ========== GET ========== */

int cmd_get(const char *db_root, const char *object,
            const char *key, size_t klen) {
    Schema sc = load_schema(db_root, object);

    /* Route through slotcask. Wire response shape (bare value dict for
       single-key get, per 2026.05.1) is preserved. */
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }
    LOG_DEBUG(LOG_SUB_SLOTCASK, "GET %s (klen=%zu, %zu bytes)", object, klen, vlen);
    TypedSchema *ts = load_typed_schema(db_root, object);
    typed_decode_stream(ts, (const uint8_t *)val, (uint32_t)vlen,
                         g_out ? g_out : stdout);
    fputc('\n', g_out ? g_out : stdout);
    free(val);
    return 0;
}

/* Single-key get with a field projection. Same slotcask read path as
   cmd_get; response is a bare dict of the requested fields (matching
   cmd_get's bare-value contract — no {"key":...,"value":{...}} wrapper).
   fields_csv is a comma-separated field list (composite fields use '+',
   same as decode_field elsewhere). */
int cmd_get_fields(const char *db_root, const char *object,
                    const char *key, size_t klen, const char *fields_csv) {
    Schema sc = load_schema(db_root, object);
    SlotcaskSchemaInfo info = {
        .splits = sc.splits, .slot_size = sc.slot_size,
        .streams = sc.streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }
    void *val = NULL; size_t vlen = 0;
    if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
        OUT("{\"error\":\"Not found\"}\n");
        return 1;
    }

    FieldSchema pfs;
    init_field_schema(&pfs, db_root, object);
    char proj_buf[MAX_LINE];
    strncpy(proj_buf, fields_csv, MAX_LINE - 1);
    proj_buf[MAX_LINE - 1] = '\0';
    const char *flds[MAX_FIELDS];
    int nf = 0;
    char *_tok_save = NULL;
    char *tok = strtok_r(proj_buf, ",", &_tok_save);
    while (tok && nf < MAX_FIELDS) { flds[nf++] = tok; tok = strtok_r(NULL, ",", &_tok_save); }

    OUT("{");
    int first = 1;
    for (int fi = 0; fi < nf; fi++) {
        char *pv = json_projected_field((const char *)val, vlen, flds[fi],
            (pfs.ts || pfs.nfields > 0) ? &pfs : NULL);
        if (!pv) continue;
        OUT("%s\"%s\":%s", first ? "" : ",", flds[fi], pv);
        first = 0; free(pv);
    }
    OUT("}\n");
    free(val);
    return 0;
}

/* ========== CAS (Compare-and-Swap) helper ========== */

/* Check all criteria against the current record value (typed binary).
   value_len is the number of valid bytes in value_ptr (may be < ts->total_size
   for trim-encoded records). Returns 1 if ALL criteria match, 0 on first failure. */
int cas_check(TypedSchema *ts, const uint8_t *value_ptr, int value_len,
              SearchCriterion *crit, int ncrit) {
    for (int i = 0; i < ncrit; i++) {
        char *val_str = NULL;
        if (strchr(crit[i].field, '+')) {
            /* Composite field: concatenate sub-fields */
            char fb[256]; strncpy(fb, crit[i].field, 255); fb[255] = '\0';
            char cat[4096]; int cp = 0; int ok = 1;
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok) {
                int fi = typed_field_index(ts, tok);
                if (fi >= 0) {
                    char *v = typed_get_field_str(ts, value_ptr, value_len, fi);
                    if (v) { int sl = strlen(v); memcpy(cat + cp, v, sl); cp += sl; free(v); }
                    else { ok = 0; break; }
                } else { ok = 0; break; }
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            cat[cp] = '\0';
            val_str = (ok && cp > 0) ? strdup(cat) : NULL;
        } else {
            int fi = typed_field_index(ts, crit[i].field);
            if (fi >= 0) val_str = typed_get_field_str(ts, value_ptr, value_len, fi);
        }
        int matched = match_criterion(val_str, &crit[i]);
        free(val_str);
        if (!matched) return 0;
    }
    return 1;
}

/* ========== INSERT — v2 (slotcask) helper ==========
 *
 * Closure carries everything the upsert callbacks need. check_fn validates
 * if_json criteria; pre_commit_fn diffs and updates indexes between data
 * write and kf commit (Option B). The ts pointer is borrowed from
 * load_typed_schema's cache and stays valid for the request's lifetime.
 */
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*fields)[256];
    int               nfields;
    enum IndexType   *idx_types;       /* parallel to fields[], loaded once */
    const char       *value_json;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
    /* Populated by slotcask BEFORE pre_commit so bitmap update_idx_fn can
       address the just-written record by (shard, slot). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit when a bitmap-index field's per-file cap
       is exceeded. cmd_insert_v2 surfaces this as the wire-level error
       so the operator gets an actionable message + the field name. */
    char              err_buf[256];
    /* Fresh-insert path only: staged bitmap writer handles from
       v2_insert_prepare_commit (pre-marker, cap-checked, no durable
       mutation), applied by v2_insert_apply_commit (post-marker). The
       BitmapPrepareSet entries borrow new_key pointers rather than
       copying, so the underlying allocations are tracked separately
       here and freed once apply/abort has read them. */
    BitmapPrepareSet  bm_prep;
    uint8_t          *bm_owned_keys[MAX_FIELDS];
    int               n_bm_owned;
} V2InsertCtx;

static void v2_insert_bm_owned_free(V2InsertCtx *c) {
    for (int i = 0; i < c->n_bm_owned; i++) free(c->bm_owned_keys[i]);
    c->n_bm_owned = 0;
}

static int v2_insert_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        /* if_json criteria require an existing record; reject if missing. */
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

/* UpdateIdxArg / update_idx_fn now live in index.c (declared in types.h)
   so query.c can dispatch the same per-field worker for its bulk
   update/delete pre_commits. */

static int capture_index_update_error(char *err_buf, size_t err_cap,
                                      const UpdateIdxArg *arg,
                                      const char *operation) {
    if (!arg || arg->out_error == 0) return 0;
    if (err_buf[0]) return 1;
    if (arg->out_error == -1 && arg->type == IT_BITMAP) {
        snprintf(err_buf, err_cap,
                 "bitmap index on field '%s' exceeded its distinct-value cap "
                 "during %s; raise field:bitmap(N) or switch to btree",
                 arg->field, operation);
    } else {
        int err = arg->out_errno ? arg->out_errno : EIO;
        snprintf(err_buf, err_cap,
                 "%s index update failed on field '%s': %s",
                 operation, arg->field, strerror(err));
    }
    return 1;
}

/* Update-resolved runtime path only — see slotcask.h's
   slotcask_prepare_commit_fn / slotcask_apply_commit_fn doc comment. A
   fresh-key insert never reaches this hook; it goes through
   v2_insert_prepare_commit / v2_insert_apply_commit instead, since only
   a fresh insert has a not-yet-durable kf slot that a legitimate
   rejection (e.g. bitmap cap) can still safely fall back on. */
static int v2_insert_pre_commit(const SlotcaskOldRecord *old,
                                const uint8_t *new_value, size_t new_vlen,
                                int is_update, void *ctx_ptr) {
    (void)new_value; (void)new_vlen;
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    if (c->nfields == 0) return 0;

    if (is_update && old) {
        /* Per-field diff: write/delete only entries that changed.
           Phase 1 (serial): build all (new_key, old_key) pairs, decide
           which fields changed. Phase 2 (parallel): apply the changes
           via parallel_for. For 12-index workloads with N changed fields,
           this drops the index-update wall time from N×~1µs sequential
           to ~1µs parallel (limited by core count). */
        char *old_json = typed_decode(c->idx_ts, old->value, (uint32_t)old->vlen);
        UpdateIdxArg args[MAX_FIELDS];
        const char *ch_fields[MAX_FIELDS];
        enum IndexType ch_types[MAX_FIELDS];
        int n_args = 0;
        for (int i = 0; i < c->nfields; i++) {
            uint8_t *new_key = NULL, *old_key = NULL;
            size_t new_len = 0, old_len = 0;
            int have_new = build_index_key_from_json(c->idx_ts, c->value_json,
                                                     c->fields[i], &new_key, &new_len);
            int have_old = old_json
                ? build_index_key_from_json(c->idx_ts, old_json,
                                            c->fields[i], &old_key, &old_len)
                : 0;
            if (have_new < 0 || have_old < 0) {
                free(new_key); free(old_key);
                for (int j = 0; j < n_args; j++) {
                    free(args[j].new_key);
                    free(args[j].old_key);
                }
                free(old_json);
                snprintf(c->err_buf, sizeof(c->err_buf),
                         "index-key decode failed during insert/update: %s",
                         strerror(errno ? errno : EIO));
                return -1;
            }
            int changed = 0;
            if (have_new && !have_old) changed = 1;
            else if (have_new && have_old) {
                if (new_len != old_len ||
                    memcmp(new_key, old_key, new_len) != 0) changed = 1;
            }
            else if (!have_new && have_old) {
                /* Field cleared on update — the old btree entry must be
                   removed or it leaks a stale (key → record_hash) edge.
                   update_idx_fn already handles new_key=NULL as a
                   delete-only operation. */
                changed = 1;
            }
            if (changed) {
                args[n_args].db_root = c->db_root;
                args[n_args].object  = c->object;
                args[n_args].field   = c->fields[i];
                args[n_args].splits  = c->splits;
                /* update_idx_fn treats NULL keys as "skip" — so a
                   delete-only op (have_old, !have_new) gets new_key=NULL
                   and old_key=old_key, an insert-only op (have_new,
                   !have_old) gets new_key=new_key and old_key=NULL,
                   and a real update gets both. */
                args[n_args].new_key = have_new ? new_key : NULL;
                args[n_args].new_len = new_len;
                args[n_args].old_key = have_old ? old_key : NULL;
                args[n_args].old_len = old_len;
                args[n_args].hash    = c->hash;
                args[n_args].type    = c->idx_types ? c->idx_types[i] : IT_BTREE;
                args[n_args].kf_shard = c->kf_shard;
                args[n_args].kf_slot  = c->kf_slot;
                args[n_args].bm_max_values = 0;  /* default cap — header wins on existing */
                ch_fields[n_args] = c->fields[i];
                ch_types[n_args] = c->idx_types ? c->idx_types[i] : IT_BTREE;
                /* Bitmap keeps sync_after=1 so bm_sync fires inside
                   bitmap_update under its open writer handle (I3);
                   btree/trigram durability moves to the collector below. */
                args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
                n_args++;
            } else {
                /* Unchanged — free immediately, nothing to dispatch. */
                free(new_key); free(old_key);
            }
        }
        int idx_failed = 0;
        if (n_args > 0) {
            parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
            for (int i = 0; i < n_args; i++) {
                if (capture_index_update_error(c->err_buf,
                                               sizeof(c->err_buf), &args[i],
                                               "insert/update"))
                    idx_failed = 1;
                free(args[i].new_key);
                free(args[i].old_key);
            }
        }
        if (n_args > 0 &&
            index_sync_record_fields(c->db_root, c->object, c->splits,
                                     c->hash, ch_fields, ch_types,
                                     n_args) != 0)
            idx_failed = 1;
        free(old_json);
        bm_flush_thread_bitmap_cache();
        if (idx_failed) return -1;
    } else {
        /* Fresh insert: entirely handled by v2_insert_prepare_commit /
           v2_insert_apply_commit instead — see the doc comment above. */
    }
    return 0;
}

/* Fresh-insert prepare phase — fires after the segment write, before the
   commit-intent marker exists. Only bitmap fields need a phase here: they
   are the only index type with a legitimate, deterministic rejection
   (the per-file distinct-value cap in bm_dict_add). Btree/trigram writes
   have no such rejection path, so they're deferred entirely to
   v2_insert_apply_commit — doing them here would just be durable I/O with
   no durability marker protecting it yet, for no benefit.

   Opens one BitmapShard writer handle per changed bitmap field on THIS
   thread and keeps it open (rwlock held) through to apply/abort — see
   BitmapPrepareSet's doc comment in types.h for why that ownership must
   stay on one thread. */
static int v2_insert_prepare_commit(const uint8_t *new_value, size_t new_vlen,
                                    uint32_t planned_kf_slot, void *ctx_ptr) {
    (void)new_value; (void)new_vlen;
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    c->kf_slot = planned_kf_slot;
    if (c->nfields == 0 || !c->idx_types) return 0;

    if (bitmap_prepare_set_init(&c->bm_prep, MAX_FIELDS) != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf),
                 "out of memory preparing bitmap index update during insert");
        return -1;
    }

    for (int i = 0; i < c->nfields; i++) {
        if (c->idx_types[i] != IT_BITMAP) continue;
        /* Composite + bitmap is rejected at create-object; defensive. */
        if (strchr(c->fields[i], '+')) continue;
        uint8_t *nk = NULL;
        size_t nl = 0;
        int key_rc = build_index_key_from_json(c->idx_ts, c->value_json,
                                               c->fields[i], &nk, &nl);
        if (key_rc < 0) {
            bitmap_prepare_set_free(&c->bm_prep);
            v2_insert_bm_owned_free(c);
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "bitmap index-key decode failed during insert: %s",
                     strerror(errno ? errno : EIO));
            return -1;
        }
        if (key_rc == 0) continue;

        UpdateIdxArg arg = {0};
        arg.db_root = c->db_root;
        arg.object  = c->object;
        arg.field   = c->fields[i];
        arg.splits  = c->splits;
        arg.new_key = nk;
        arg.new_len = nl;
        arg.hash    = c->hash;
        arg.type    = IT_BITMAP;
        arg.kf_shard = c->kf_shard;
        arg.kf_slot  = c->kf_slot;
        arg.bm_max_values = 0;
        arg.sync_after = 1;

        char err_field[128];
        int prc = bitmap_prepare_set_add(&c->bm_prep, &arg, err_field, sizeof(err_field));
        if (prc == -1) {
            free(nk);
            bitmap_prepare_set_free(&c->bm_prep);
            v2_insert_bm_owned_free(c);
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "bitmap index on field '%s' exceeded its distinct-value cap; "
                     "raise field:bitmap(N) or switch to btree", err_field);
            return -1;
        }
        if (prc != 0) {
            free(nk);
            bitmap_prepare_set_free(&c->bm_prep);
            v2_insert_bm_owned_free(c);
            snprintf(c->err_buf, sizeof(c->err_buf),
                     "bitmap index update failed on field '%s' during insert",
                     c->fields[i]);
            return -1;
        }
        /* Staged: bitmap_prepare_set_add borrowed `nk` into the entry's
           new_val — it does not free it. Track it so apply/abort frees it
           exactly once, after the last read. */
        c->bm_owned_keys[c->n_bm_owned++] = nk;
    }
    return 0;
}

/* Rare path: prepare_commit staged bitmap writer handles but the marker
   write itself then failed, so apply_commit never ran. Release what was
   staged without applying it. */
static void v2_insert_abort_commit(void *ctx_ptr) {
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    bitmap_prepare_set_free(&c->bm_prep);
    v2_insert_bm_owned_free(c);
}

/* Fresh-insert apply phase — fires after the commit-intent marker is
   durable, before the kf slot itself is committed. Btree/trigram writes
   go first (unconditional — no cap, so no rejection is legal here; a
   failure is genuine I/O/OOM and falls through to the existing
   replay/fail-closed handling, unchanged), then the bitmap writer
   handles staged by v2_insert_prepare_commit are applied and released. */
static int v2_insert_apply_commit(const uint8_t *new_value, size_t new_vlen,
                                  uint32_t planned_kf_slot, void *ctx_ptr) {
    (void)new_value; (void)new_vlen; (void)planned_kf_slot;
    V2InsertCtx *c = (V2InsertCtx *)ctx_ptr;
    UpdateIdxArg tg_args[MAX_FIELDS];
    const char *tg_fields[MAX_FIELDS];
    int n_tg = 0;
    if (c->nfields == 0) return 0;

    if (index_parallel(c->db_root, c->object, c->splits,
                       c->value_json, c->hash, c->fields, c->nfields,
                       c->idx_types) != 0) {
        snprintf(c->err_buf, sizeof(c->err_buf),
                 "btree index update failed during insert: %s",
                 strerror(errno));
        bitmap_prepare_set_free(&c->bm_prep);
        v2_insert_bm_owned_free(c);
        return -1;
    }

    int idx_failed = 0;
    if (c->idx_types) {
        /* Trigram indexes — same dispatch shape as before, no overflow
           path (no per-file cap). update_idx_fn's IT_TRIGRAM branch
           extracts distinct trigrams from new_key and writes one .tg
           leaf entry per (trigram, record hash). */
        for (int i = 0; i < c->nfields; i++) {
            if (c->idx_types[i] != IT_TRIGRAM) continue;
            if (strchr(c->fields[i], '+')) continue;  /* composite + trigram = rejected upstream */
            uint8_t *nk = NULL;
            size_t nl = 0;
            int key_rc = build_index_key_from_json(c->idx_ts, c->value_json,
                                                   c->fields[i], &nk, &nl);
            if (key_rc < 0) {
                for (int j = 0; j < n_tg; j++) free(tg_args[j].new_key);
                snprintf(c->err_buf, sizeof(c->err_buf),
                         "trigram index-key decode failed during insert: %s",
                         strerror(errno ? errno : EIO));
                bitmap_prepare_set_free(&c->bm_prep);
                v2_insert_bm_owned_free(c);
                return -1;
            }
            if (key_rc == 0) continue;
            tg_args[n_tg].db_root  = c->db_root;
            tg_args[n_tg].object   = c->object;
            tg_args[n_tg].field    = c->fields[i];
            tg_args[n_tg].splits   = c->splits;
            tg_args[n_tg].new_key  = nk;
            tg_args[n_tg].new_len  = nl;
            tg_args[n_tg].old_key  = NULL;
            tg_args[n_tg].old_len  = 0;
            tg_args[n_tg].hash     = c->hash;
            tg_args[n_tg].type     = IT_TRIGRAM;
            tg_args[n_tg].kf_shard = c->kf_shard;
            tg_args[n_tg].kf_slot  = c->kf_slot;
            tg_args[n_tg].bm_max_values = 0;
            tg_args[n_tg].sync_after = 0;  /* was uninitialized stack garbage;
                                              durability moves to index_sync_record_fields */
            tg_fields[n_tg] = c->fields[i];
            n_tg++;
        }
        if (n_tg > 0) {
            parallel_for(update_idx_fn, tg_args, n_tg, sizeof(UpdateIdxArg));
            for (int i = 0; i < n_tg; i++) {
                if (capture_index_update_error(c->err_buf,
                                               sizeof(c->err_buf),
                                               &tg_args[i], "insert"))
                    idx_failed = 1;
                free(tg_args[i].new_key);
            }
        }

        if (bitmap_prepare_set_apply(&c->bm_prep) != 0) {
            if (!c->err_buf[0])
                snprintf(c->err_buf, sizeof(c->err_buf),
                         "bitmap index update failed during insert apply");
            idx_failed = 1;
        }
        bitmap_prepare_set_free(&c->bm_prep);
        v2_insert_bm_owned_free(c);
        bm_flush_thread_bitmap_cache();
    }
    if (n_tg > 0) {
        enum IndexType tg_types[MAX_FIELDS];
        for (int i = 0; i < n_tg; i++) tg_types[i] = IT_TRIGRAM;
        if (index_sync_record_fields(c->db_root, c->object, c->splits,
                                     c->hash, tg_fields, tg_types,
                                     n_tg) != 0)
            idx_failed = 1;
    }
    return idx_failed ? -1 : 0;
}

/* Produce the "now" string for an auto_create / auto_update timestamp field in
   the form its type expects. buf must be >= 24 bytes.
     FT_TIMESTAMP  — Unix epoch ms (decimal)
     FT_DATETIMEMS — yyyyMMddHHmmssSSS
     FT_DATE       — yyyyMMdd
     other (FT_DATETIME / fallback) — yyyyMMddHHmmss */
void auto_now_str(const TypedField *f, char *buf, size_t bufsz) {
    if (f->type == FT_TIMESTAMP) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
        snprintf(buf, bufsz, "%lld", ms);
    } else if (f->type == FT_DATETIMEMS) {
        struct timespec tsn; clock_gettime(CLOCK_REALTIME, &tsn);
        time_t nowsec = tsn.tv_sec; struct tm tm; localtime_r(&nowsec, &tm);
        int msec = (int)(tsn.tv_nsec / 1000000L);
        snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d%03d",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                 tm.tm_hour, tm.tm_min, tm.tm_sec, msec);
    } else {
        time_t now = time(NULL); struct tm tm; localtime_r(&now, &tm);
        if (f->type == FT_DATE)
            snprintf(buf, bufsz, "%04d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
        else
            snprintf(buf, bufsz, "%04d%02d%02d%02d%02d%02d",
                     tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
                     tm.tm_hour, tm.tm_min, tm.tm_sec);
    }
}

static int cmd_insert_v2(const char *db_root, const char *object,
                         const char *key, size_t klen, const char *value,
                         const char *if_json, int if_not_exists,
                         const Schema *sc) {
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"Object [%s] not found. Use create-object first.\"}\n", object);
        return 1;
    }

    if ((int)klen > sc->max_key) {
        fprintf(stderr, "Error: Key too large (%zu > %d)\n", klen, sc->max_key);
        return 1;
    }

    uint8_t *typed_buf = malloc(ts->total_size);
    if (!typed_buf) { OUT("{\"error\":\"oom\"}\n"); return 1; }
    char enc_err[512] = {0};
    int enc = typed_encode_defaults(ts, value, typed_buf, ts->total_size,
                                    db_root, object, enc_err, sizeof(enc_err));
    if (enc == -2) {
        /* Strict enum (or future typed) validation rejection — actionable
           error already in enc_err. Caller never sees a successfully-
           encoded but semantically-corrupt record. */
        free(typed_buf);
        OUT("{\"error\":\"%s\"}\n", enc_err);
        return 1;
    }
    if (enc < 0) {
        free(typed_buf);
        OUT("{\"error\":\"Typed encode failed\"}\n");
        return 1;
    }
    size_t vlen = ts->total_size;
    if ((int)vlen > sc->max_value) {
        free(typed_buf);
        fprintf(stderr, "Error: Value too large (%zu > %d)\n", vlen, sc->max_value);
        return 1;
    }

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        free(typed_buf);
        OUT("{\"error\":\"Cannot open shard\"}\n");
        return 1;
    }

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* auto_create: stamp now() on first insert only; preserve the stored value
       on any update / re-insert. This path is an upsert, so a re-insert of an
       existing key would otherwise zero the create-time (typed_encode_defaults
       leaves DK_AUTO_CREATE fields blank). We consult the prior record — but
       only when the schema actually declares an auto_create field, so ordinary
       objects pay nothing. */
    {
        int has_ac = 0;
        for (int i = 0; i < ts->nfields; i++)
            if (!ts->fields[i].removed &&
                ts->fields[i].default_kind == DK_AUTO_CREATE) { has_ac = 1; break; }
        if (has_ac) {
            void *ac_old = NULL; size_t ac_old_vlen = 0;
            int existed = (slotcask_get(sdb, key, klen, &ac_old, &ac_old_vlen) == 0);
            for (int i = 0; i < ts->nfields; i++) {
                if (ts->fields[i].removed ||
                    ts->fields[i].default_kind != DK_AUTO_CREATE) continue;
                size_t off = (size_t)ts->fields[i].offset;
                size_t w   = (size_t)ts->fields[i].size;
                if (existed && ac_old && ac_old_vlen >= off + w) {
                    memcpy(typed_buf + off, (uint8_t *)ac_old + off, w);
                } else if (!existed) {
                    /* Re-stamp unconditionally, even though typed_encode_defaults
                       already stamped now() for a client-omitted field. Do NOT
                       "optimize" this away: if the client explicitly supplied a
                       value for this field, typed_encode_defaults wrote THAT
                       value (seen[i]=1 skips generate_default), and this is the
                       only place that overwrites it — removing this branch lets
                       a client-supplied auto_create value survive a fresh
                       insert, which the field's contract forbids. The extra
                       clock_gettime on fresh inserts is negligible. */
                    char tbuf[24];
                    auto_now_str(&ts->fields[i], tbuf, sizeof(tbuf));
                    encode_field(&ts->fields[i], tbuf, typed_buf + off);
                }
                /* existed but old too short (field added post-hoc): leave blank. */
            }
            free(ac_old);
        }
    }

    /* Index fields + criteria (only parsed if if_json is present). */
    char fields[MAX_FIELDS][256];
    int nfields = load_index_fields(db_root, object, fields, MAX_FIELDS);
    for (int _i = 0; _i < nfields; _i++) fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        free(typed_buf);
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    V2InsertCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .fields = fields, .nfields = nfields,
        .idx_types = idx_types,
        .value_json = value,
        .idx_ts = ts,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);

    SlotcaskUpsertOpts opts = {
        .if_not_exists  = if_not_exists,
        .check          = v2_insert_check_fn,
        .check_ctx      = &ctx,
        .pre_commit     = v2_insert_pre_commit,
        .pre_commit_ctx = &ctx,
        /* Fresh-key inserts route through the two-phase hooks instead of
           pre_commit (v2_insert_pre_commit's fresh-insert branch is a
           no-op — see its doc comment); pre_commit stays wired for the
           update-resolved path only. */
        .prepare_commit = v2_insert_prepare_commit,
        .apply_commit   = v2_insert_apply_commit,
        .abort_commit   = v2_insert_abort_commit,
        /* Bitmap index needs (shard, slot) — slotcask writes them here
           before invoking pre_commit. update_idx_fn reads them via
           V2InsertCtx (same struct, no second indirection). */
        .out_kf_shard   = &ctx.kf_shard,
        .out_kf_slot    = &ctx.kf_slot,
        .has_indexed_fields = nfields > 0,
    };
    SlotcaskUpsertResult result = {0};
    int rc;
    /* Fast path: pure INSERT semantics (if_not_exists=true) without a CAS
       criterion lets us skip the kf_lookup-with-verify pass that the upsert
       path always pays. The duplicate detection still happens implicitly
       inside kf_put_new — caller sees -2 with was_update=1 if a duplicate
       is found. v2_insert_check_fn / v2_insert_pre_commit handle old=NULL
       correctly (they only diff against old when is_update=1). */
    uint64_t _commit_t0 = now_us();
    if (if_not_exists && !if_json) {
        rc = slotcask_insert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    } else {
        rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        typed_buf, vlen, &opts, &result);
    }
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        char *cur = result.current_value
            ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur);
        free(result.current_value);
        free_criteria(crit, ncrit);
        free(typed_buf);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        free(typed_buf);
        if (ctx.err_buf[0]) {
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        } else {
            OUT("{\"error\":\"upsert failed\"}\n");
        }
        return 1;
    }

    if (!result.was_update) update_count(db_root, object, 1);
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "%s %s.%s (slotcask)",
            result.was_update ? "UPDATE" : "INSERT", object, wire_key);

    free(result.current_value);
    free_criteria(crit, ncrit);
    free(typed_buf);
    OUT("{\"status\":\"%s\",\"key\":\"%s\"}\n",
        result.was_update ? "updated" : "inserted", wire_key);
    return 0;
}

/* ========== INSERT (mmap + atomic flag flip) ========== */

int cmd_insert(const char *db_root, const char *object,
               const char *key, size_t klen, const char *value,
               const char *if_json, int if_not_exists) {
    Schema sc = load_schema(db_root, object);
    return cmd_insert_v2(db_root, object, key, klen, value, if_json,
                         if_not_exists, &sc);
}

/* ========== PARTIAL UPDATE — v2 (slotcask) helper ==========
 *
 * Single-lock construction: upsert(require_existing=1) with a
 * new_from_old callback that rebuilds NEW from the OLD record already read
 * under the kf-shard wrlock, an inline check_fn that enforces the `if`
 * criteria, and a pre_commit hook that diffs old vs new typed records for
 * index updates. No outside-lock OLD snapshot exists, so a concurrent
 * partial update cannot resurrect stale fields (atomic single updates).
 * dry_run is the only path that still reads OLD up-front — race-tolerant
 * since it writes nothing.
 */
typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    enum IndexType   *idx_types;        /* parallel to idx_fields[] */
    TypedSchema      *idx_ts;
    /* Raw partial-update field JSON, parsed inside the lock-protected
       new_from_old callback (v2_update_new_from_old). */
    const char       *partial_json;
    /* CAS criteria — verified inside check_fn under the kf-shard wrlock
       so the check + commit are atomic against concurrent writers. NULL
       when the caller didn't pass `if`. */
    SearchCriterion  *crit;
    int               ncrit;
    /* Populated by slotcask BEFORE pre_commit (bitmap addresses records
       by physical slot, not by hash). */
    int               kf_shard;
    uint32_t          kf_slot;
    /* Populated by pre_commit on bitmap-index cap overflow, and by
       v2_update_new_from_old for malformed-escape / varchar-overflow
       rejections. */
    char              err_buf[256];
    /* Stashed during v2_update_new_from_old so apply_commit can
       compute the forward index diff (old→new) without re-reading. */
    const uint8_t    *saved_old_value;
    size_t            saved_old_vlen;
} V2UpdateCtx;

/* NEW-from-OLD constructor for single partial updates. Runs inside
   upsert_slow_path while the kf-shard write lock is held, after
   if_not_exists / require_existing / check have accepted the current OLD —
   so the replacement is built from the same OLD that the commit will
   overwrite, never from a stale caller-side snapshot. Copies every
   untouched field, applies only fields present in the request, preserves
   removed fields, and stamps auto_update fields exactly once. */
static int v2_update_new_from_old(const SlotcaskOldRecord *old,
                                  uint8_t *out_value,
                                  size_t out_capacity,
                                  size_t *out_vlen,
                                  void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old || !out_value || !out_vlen || old->vlen > out_capacity) return -1;
    /* Stash OLD so apply_commit can compute the index diff without
       re-reading (the two-phase hook signature doesn't carry OLD). */
    c->saved_old_value = old->value;
    c->saved_old_vlen  = old->vlen;
#ifdef TEST_BUILD
    /* Fixed-path seam: deterministic pause at the top of the under-lock
       callback, after OLD has been received. Reports under_kf_wrlock == 1. */
    slotcask_test_after_old(1);
#endif
    memcpy(out_value, old->value, old->vlen);
    *out_vlen = old->vlen;

    /* Build new typed buffer = copy of old, with partial fields applied. */
    const char *field_names[MAX_FIELDS];
    char *field_vals[MAX_FIELDS] = {0};
    enum FieldType field_types[MAX_FIELDS];
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        field_names[i] = c->idx_ts->fields[i].name;
        field_types[i] = c->idx_ts->fields[i].type;
    }
    if (json_get_fields_unescaped(c->partial_json, field_names,
                                  c->idx_ts->nfields, field_types,
                                  field_vals) != 0) {
        /* At least one field the client explicitly named had a malformed
           JSON escape. Reject the whole update rather than silently
           applying every other field and dropping this one — a partial
           write here would look like success to the caller. */
        for (int i = 0; i < c->idx_ts->nfields; i++) free(field_vals[i]);
        snprintf(c->err_buf, sizeof(c->err_buf),
                 "malformed JSON escape in one or more field values");
        return -1;
    }

    for (int i = 0; i < c->idx_ts->nfields; i++) {
        if (field_vals[i]) {
            if (!c->idx_ts->fields[i].removed) {
                if (c->idx_ts->fields[i].type == FT_VARCHAR) {
                    int content_max = c->idx_ts->fields[i].size - 2;
                    size_t vlen = strlen(field_vals[i]);
                    if ((int)vlen > content_max) {
                        snprintf(c->err_buf, sizeof(c->err_buf),
                            "value for field '%s' is %zu bytes; exceeds max %d for varchar",
                            c->idx_ts->fields[i].name, vlen, content_max);
                        free(field_vals[i]);
                        for (int j = i + 1; j < c->idx_ts->nfields; j++)
                            free(field_vals[j]);
                        return -1;
                    }
                }
                encode_field(&c->idx_ts->fields[i], field_vals[i],
                             out_value + c->idx_ts->fields[i].offset);
            }
            free(field_vals[i]);
        }
    }

    /* auto_update fields: stamp current value on every update.
       Each typed type gets its appropriate now-form:
         FT_DATE      — yyyyMMdd (8-char int32 packed)
         FT_TIMESTAMP — Unix epoch ms (int64 BE; 2026.05.6+)
         everything else — yyyyMMddHHmmss (FT_DATETIME / fallback) */
    for (int i = 0; i < c->idx_ts->nfields; i++) {
        if (c->idx_ts->fields[i].removed) continue;
        if (c->idx_ts->fields[i].default_kind != DK_AUTO_UPDATE) continue;

        char tbuf[24];
        if (c->idx_ts->fields[i].type == FT_TIMESTAMP) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            long long ms = (long long)tsn.tv_sec * 1000LL + tsn.tv_nsec / 1000000LL;
            snprintf(tbuf, sizeof(tbuf), "%lld", ms);
        } else if (c->idx_ts->fields[i].type == FT_DATETIMEMS) {
            struct timespec tsn;
            clock_gettime(CLOCK_REALTIME, &tsn);
            time_t nowsec = tsn.tv_sec;
            struct tm tmv;
            localtime_r(&nowsec, &tmv);
            int msec = (int)(tsn.tv_nsec / 1000000L);
            snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d%03d",
                     tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                     tmv.tm_hour, tmv.tm_min, tmv.tm_sec, msec);
        } else {
            time_t now = time(NULL);
            struct tm tmv;
            localtime_r(&now, &tmv);
            if (c->idx_ts->fields[i].type == FT_DATE)
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
            else
                snprintf(tbuf, sizeof(tbuf), "%04d%02d%02d%02d%02d%02d",
                         tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday,
                         tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
        }
        encode_field(&c->idx_ts->fields[i], tbuf,
                     out_value + c->idx_ts->fields[i].offset);
    }
    return 0;
}

static int v2_update_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old) return 0;  /* require_existing handles this, but defensive */
    if (c->crit && c->ncrit > 0 &&
        !cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    return 1;
}

typedef struct {
    const char *db_root, *object;
    int nidx;
    char (*idx_fields)[256];
    enum IndexType *idx_types;
    int splits;
    const uint8_t *hash;
    int kf_shard;
    uint32_t kf_slot;
    TypedSchema *idx_ts;
    const uint8_t *old_value;
    const uint8_t *new_value;
    char *err_buf;
    size_t err_buf_len;
} IndexDiffApplyArgs;

static int apply_index_diff(const IndexDiffApplyArgs *a) {
    if (a->nidx == 0) return 0;
    enum { INDEX_KEY_MAX = 4096 };
    size_t arena_bytes = (size_t)a->nidx * (size_t)(2 * INDEX_KEY_MAX);
    uint8_t *arena = malloc(arena_bytes);
    UpdateIdxArg args[MAX_FIELDS];
    const char *ch_fields[MAX_FIELDS];
    enum IndexType ch_types[MAX_FIELDS];
    uint8_t *fb_bufs[2 * MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int i = 0; i < a->nidx; i++) {
        uint8_t *old_slot = arena ? arena + (size_t)i * 2 * INDEX_KEY_MAX : NULL;
        uint8_t *new_slot = old_slot ? old_slot + INDEX_KEY_MAX : NULL;
        size_t old_len = 0, new_len = 0;
        int have_old = 0, have_new = 0;
        uint8_t *old_buf = NULL, *new_buf = NULL;

        if (arena) {
            int ro = a->old_value
                ? build_index_key_from_record_into(a->idx_ts, a->old_value,
                                                   a->idx_fields[i],
                                                   old_slot, INDEX_KEY_MAX, &old_len)
                : 0;
            int rn = a->new_value
                ? build_index_key_from_record_into(a->idx_ts, a->new_value,
                                                   a->idx_fields[i],
                                                   new_slot, INDEX_KEY_MAX, &new_len)
                : 0;
            have_old = (ro == 1);
            have_new = (rn == 1);
            old_buf = have_old ? old_slot : NULL;
            new_buf = have_new ? new_slot : NULL;
            if (ro == -1) {
                have_old = a->old_value
                    ? build_index_key_from_record(a->idx_ts, a->old_value,
                                                  a->idx_fields[i], &old_buf, &old_len)
                    : 0;
                if (have_old) fb_bufs[n_fb++] = old_buf;
            }
            if (rn == -1) {
                have_new = a->new_value
                    ? build_index_key_from_record(a->idx_ts, a->new_value,
                                                  a->idx_fields[i], &new_buf,
                                                  &new_len)
                    : 0;
                if (have_new) fb_bufs[n_fb++] = new_buf;
            }
        } else {
            if (a->old_value)
                have_old = build_index_key_from_record(a->idx_ts, a->old_value,
                                                       a->idx_fields[i], &old_buf, &old_len);
            if (a->new_value)
                have_new = build_index_key_from_record(a->idx_ts,
                                                       a->new_value,
                                                       a->idx_fields[i],
                                                       &new_buf, &new_len);
            if (have_old) fb_bufs[n_fb++] = old_buf;
            if (have_new) fb_bufs[n_fb++] = new_buf;
        }

        int changed = 0;
        if (have_new && !have_old) changed = 1;
        else if (!have_new && have_old) changed = 1;
        else if (have_new && have_old) {
            if (new_len != old_len || memcmp(new_buf, old_buf, new_len) != 0) changed = 1;
        }
        if (changed) {
            args[n_args].db_root = a->db_root;
            args[n_args].object  = a->object;
            args[n_args].field   = a->idx_fields[i];
            args[n_args].splits  = a->splits;
            args[n_args].new_key = have_new ? new_buf : NULL;
            args[n_args].new_len = new_len;
            args[n_args].old_key = have_old ? old_buf : NULL;
            args[n_args].old_len = old_len;
            args[n_args].hash    = a->hash;
            args[n_args].type    = a->idx_types ? a->idx_types[i] : IT_BTREE;
            args[n_args].kf_shard = a->kf_shard;
            args[n_args].kf_slot  = a->kf_slot;
            args[n_args].bm_max_values = 0;
            ch_fields[n_args] = a->idx_fields[i];
            ch_types[n_args] = a->idx_types ? a->idx_types[i] : IT_BTREE;
            /* Bitmap keeps sync_after=1 (I3); btree/trigram move to the
               collector below. */
            args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
            n_args++;
        }
    }

    int idx_failed = 0;
    if (n_args > 0) {
        parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
        for (int i = 0; i < n_args; i++) {
            if (capture_index_update_error(a->err_buf, a->err_buf_len,
                                           &args[i], "update"))
                idx_failed = 1;
        }
    }
    if (n_args > 0 &&
        index_sync_record_fields(a->db_root, a->object, a->splits,
                                 a->hash, ch_fields, ch_types,
                                 n_args) != 0)
        idx_failed = 1;
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    free(arena);
    bm_flush_thread_bitmap_cache();
    return idx_failed ? -1 : 0;
}

/* Recovery-time counterpart to v2_update_pre_commit — reconciles index
   state for a marker replayed at crash-recovery time (see
   kf_marker_replay_locked, slotcask.c). Registered onto
   g_recovery_index_diff_fn below so slotcask.c can reach apply_index_diff
   without taking a direct dependency on schema/index logic. */
static int storage_recovery_index_diff(const char *db_root, const char *object,
                                       int kf_shard, uint32_t kf_slot,
                                       const uint8_t *hash,
                                       const uint8_t *old_value, size_t old_vlen,
                                       const uint8_t *new_value, size_t new_vlen,
                                       char *err_buf, size_t err_buf_len) {
    (void)old_vlen; (void)new_vlen;
    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    if (nidx <= 0) return 0;
    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);
    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        snprintf(err_buf, err_buf_len, "recovery: no typed schema for %s/%s", db_root, object);
        return -1;
    }
    Schema sc = load_schema(db_root, object);

    IndexDiffApplyArgs args = {
        .db_root = db_root, .object = object,
        .nidx = nidx, .idx_fields = idx_fields, .idx_types = idx_types,
        .splits = sc.splits, .hash = hash,
        .kf_shard = kf_shard, .kf_slot = kf_slot, .idx_ts = ts,
        .old_value = old_value, .new_value = new_value,
        .err_buf = err_buf, .err_buf_len = err_buf_len,
    };
    return apply_index_diff(&args);
}

__attribute__((constructor))
static void storage_register_recovery_callback(void) {
    g_recovery_index_diff_fn = storage_recovery_index_diff;
}

static int v2_update_pre_commit(const SlotcaskOldRecord *old,
                                const uint8_t *new_value, size_t new_vlen,
                                int is_update, void *ctx_ptr) {
    (void)new_vlen; (void)is_update;
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;
    IndexDiffApplyArgs args = {
        .db_root = c->db_root, .object = c->object,
        .nidx = c->nidx, .idx_fields = c->idx_fields,
        .idx_types = c->idx_types, .splits = c->splits,
        .hash = c->hash, .kf_shard = c->kf_shard,
        .kf_slot = c->kf_slot, .idx_ts = c->idx_ts,
        .old_value = old->value, .new_value = new_value,
        .err_buf = c->err_buf, .err_buf_len = sizeof(c->err_buf),
    };
    return apply_index_diff(&args);
}

/* Two-phase update hooks: prepare_commit is a no-op (no bitmap staging
   needed for updates), apply_commit fires the index diff after the
   commit-intent marker is durable, abort_commit is a no-op. */
static int v2_update_prepare_commit(const uint8_t *new_value, size_t new_vlen,
                                    uint32_t kf_slot, void *ctx_ptr) {
    (void)new_value; (void)new_vlen; (void)kf_slot; (void)ctx_ptr;
    return 0;
}

static int v2_update_apply_commit(const uint8_t *new_value, size_t new_vlen,
                                  uint32_t kf_slot, void *ctx_ptr) {
    (void)new_vlen; (void)kf_slot;
    V2UpdateCtx *c = (V2UpdateCtx *)ctx_ptr;
    if (c->nidx == 0 || !c->saved_old_value) return 0;
    IndexDiffApplyArgs args = {
        .db_root = c->db_root, .object = c->object,
        .nidx = c->nidx, .idx_fields = c->idx_fields,
        .idx_types = c->idx_types, .splits = c->splits,
        .hash = c->hash, .kf_shard = c->kf_shard,
        .kf_slot = c->kf_slot, .idx_ts = c->idx_ts,
        .old_value = c->saved_old_value, .new_value = new_value,
        .err_buf = c->err_buf, .err_buf_len = sizeof(c->err_buf),
    };
    return apply_index_diff(&args);
}

static void v2_update_abort_commit(void *ctx_ptr) {
    (void)ctx_ptr;
}

static int cmd_update_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *partial_json,
                         const char *if_json, int dry_run, const Schema *sc) {
    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) { OUT("{\"error\":\"Not found\"}\n"); return 1; }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) { OUT("{\"error\":\"Object not found\"}\n"); return 1; }

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* dry_run validates criteria but doesn't write — race-tolerant. It is
       the only remaining user of the up-front slotcask_get snapshot: a
       normal update builds NEW from the lock-protected OLD inside
       v2_update_new_from_old instead, so no stale outside-lock snapshot
       exists for the race to be lost against. */
    if (dry_run) {
        void *old_val = NULL; size_t old_vlen = 0;
        if (slotcask_get(sdb, key, klen, &old_val, &old_vlen) != 0) {
            OUT("{\"error\":\"Not found\"}\n");
            return 1;
        }
#ifdef TEST_BUILD
        /* TEST_BUILD seam retained for dry_run only: fires on dry-run
           updates in the fixed code (normal updates pause at the top of
           v2_update_new_from_old, under_kf_wrlock == 1). */
        slotcask_test_after_old(0);
#endif
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            if (parse_criteria_json(if_json, &crit, &ncrit) != 0) {
                free(old_val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check(ts, old_val, (int)old_vlen, crit, ncrit);
            free_criteria(crit, ncrit);
            if (!pass) {
                char *cur = typed_decode(ts, old_val, (uint32_t)old_vlen);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(old_val);
                return 1;
            }
        }
        free(old_val);
        char wire_key[1100];
        format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
        OUT("{\"status\":\"would_update\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    /* Parse `if` once before the lock-protected callback; check_fn runs
       cas_check under the kf-shard wrlock so the verify + commit are atomic
       against concurrent writers. Note the intentional precedence shift:
       when both the partial field JSON and the `if` criteria are invalid,
       this now reports "invalid if condition" first — the field parsing
       happens inside the callback under the lock. */
    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    V2UpdateCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
        .idx_ts = ts,
        .partial_json = partial_json,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);

    SlotcaskUpsertOpts opts = {
        .require_existing = 1,
        .check            = v2_update_check_fn,
        .check_ctx        = &ctx,
        .new_from_old     = v2_update_new_from_old,
        .new_from_old_ctx = &ctx,
        .pre_commit       = v2_update_pre_commit,
        .pre_commit_ctx   = &ctx,
        .prepare_commit   = v2_update_prepare_commit,
        .apply_commit     = v2_update_apply_commit,
        .abort_commit     = v2_update_abort_commit,
        .out_kf_shard     = &ctx.kf_shard,
        .out_kf_slot      = &ctx.kf_slot,
        .has_indexed_fields = nidx > 0,
    };
    SlotcaskUpsertResult result = {0};
    uint64_t _commit_t0 = now_us();
    int rc = slotcask_upsert_with_hooks(sdb, -1, key, klen,
                                        NULL, 0, &opts, &result);
    commit_lock_hold_record(_commit_t0, db_root, object);

    if (rc == -2) {
        /* Either require_existing fired (record vanished) or check_fn
           rejected (criteria didn't match). Disambiguate by whether the
           result has an old value attached. */
        if (result.was_update && result.condition_not_met) {
            char *cur = result.current_value
                ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
                : NULL;
            OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                cur ? cur : "null");
            free(cur);
        } else {
            OUT("{\"error\":\"Not found\"}\n");
        }
        free(result.current_value);
        free_criteria(crit, ncrit);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        if (ctx.err_buf[0]) {
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        } else {
            OUT("{\"error\":\"update failed\"}\n");
        }
        return 1;
    }
    free_criteria(crit, ncrit);

    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));
    LOG_INFO(LOG_SUB_SLOTCASK, "UPDATE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    OUT("{\"status\":\"updated\",\"key\":\"%s\"}\n", wire_key);
    return 0;
}

/* ========== PARTIAL UPDATE ========== */

int cmd_update(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *partial_json,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);
    return cmd_update_v2(db_root, object, key, klen, partial_json,
                         if_json, dry_run, &sc);
}

/* ========== DELETE — v2 (slotcask) helper ========== */

typedef struct {
    const char       *db_root;
    const char       *object;
    int               splits;
    uint8_t           hash[16];
    char            (*idx_fields)[256];
    int               nidx;
    enum IndexType   *idx_types;
    TypedSchema      *idx_ts;
    SearchCriterion  *crit;
    int               ncrit;
    int               kf_shard;     /* populated by slotcask before pre_commit */
    uint32_t          kf_slot;
    char              err_buf[256];
} V2DeleteCtx;

static int v2_delete_check_fn(const SlotcaskOldRecord *old, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (c->ncrit > 0) {
        if (!old) return 0;
        if (!cas_check(c->idx_ts, old->value, (int)old->vlen, c->crit, c->ncrit)) return 0;
    }
    return 1;
}

/* Forward index diff for an indexed delete — (old=OLD, new=NULL). Runs
   after the delete marker is durable, before the kf tombstone. Same
   parallel-fanout + arena allocation pattern as v2_update_pre_commit;
   update_idx_fn with new_key=NULL is a pure delete. */
static int v2_delete_apply_commit(const SlotcaskOldRecord *old,
                                  uint32_t kf_slot, void *ctx_ptr) {
    V2DeleteCtx *c = (V2DeleteCtx *)ctx_ptr;
    if (!old || c->nidx == 0) return 0;

    enum { INDEX_KEY_MAX = 4096 };
    size_t arena_bytes = (size_t)c->nidx * (size_t)INDEX_KEY_MAX;
    uint8_t *arena = malloc(arena_bytes);
    UpdateIdxArg args[MAX_FIELDS];
    const char *ch_fields[MAX_FIELDS];
    enum IndexType ch_types[MAX_FIELDS];
    uint8_t *fb_bufs[MAX_FIELDS]; int n_fb = 0;
    int n_args = 0;

    for (int i = 0; i < c->nidx; i++) {
        uint8_t *ikey = NULL;
        size_t ilen = 0;
        int have = 0;
        if (arena) {
            uint8_t *slot = arena + (size_t)i * INDEX_KEY_MAX;
            int rc = build_index_key_from_record_into(c->idx_ts, old->value,
                                                       c->idx_fields[i],
                                                       slot, INDEX_KEY_MAX, &ilen);
            if (rc == 1) { ikey = slot; have = 1; }
            else if (rc == -1) {
                have = build_index_key_from_record(c->idx_ts, old->value,
                                                   c->idx_fields[i],
                                                   &ikey, &ilen);
                if (have) fb_bufs[n_fb++] = ikey;
            }
        } else {
            have = build_index_key_from_record(c->idx_ts, old->value,
                                               c->idx_fields[i],
                                               &ikey, &ilen);
            if (have) fb_bufs[n_fb++] = ikey;
        }
        if (!have) continue;
        args[n_args].db_root = c->db_root;
        args[n_args].object  = c->object;
        args[n_args].field   = c->idx_fields[i];
        args[n_args].splits  = c->splits;
        args[n_args].new_key = NULL;
        args[n_args].new_len = 0;
        args[n_args].old_key = ikey;
        args[n_args].old_len = ilen;
        args[n_args].hash    = c->hash;
        args[n_args].type    = c->idx_types ? c->idx_types[i] : IT_BTREE;
        args[n_args].kf_shard = c->kf_shard;
        args[n_args].kf_slot  = kf_slot;
        args[n_args].bm_max_values = 0;
        ch_fields[n_args] = c->idx_fields[i];
        ch_types[n_args] = c->idx_types ? c->idx_types[i] : IT_BTREE;
        /* sync_after was UNINITIALIZED stack garbage (bug: deletes on
           btree/trigram fields synced only if garbage said so). Bitmap
           keeps 1 so bm_sync fires inside bitmap_update (I3); the rest
           move to the collector below. */
        args[n_args].sync_after = (ch_types[n_args] == IT_BITMAP) ? 1 : 0;
        n_args++;
    }

    int idx_failed = 0;
    if (n_args > 0) {
        parallel_for(update_idx_fn, args, n_args, sizeof(UpdateIdxArg));
        for (int i = 0; i < n_args; i++) {
            if (capture_index_update_error(c->err_buf, sizeof(c->err_buf),
                                           &args[i], "delete"))
                idx_failed = 1;
        }
    }
    if (n_args > 0 &&
        index_sync_record_fields(c->db_root, c->object, c->splits,
                                 c->hash, ch_fields, ch_types,
                                 n_args) != 0)
        idx_failed = 1;
    for (int i = 0; i < n_fb; i++) free(fb_bufs[i]);
    free(arena);
    bm_flush_thread_bitmap_cache();
    return idx_failed ? -1 : 0;
}

static int cmd_delete_v2(const char *db_root, const char *object,
                         const char *key, size_t klen,
                         const char *if_json, int dry_run,
                         const Schema *sc) {
    char wire_key[1100];
    format_wire_key(sc, key, klen, wire_key, sizeof(wire_key));

    SlotcaskSchemaInfo info = {
        .splits = sc->splits, .slot_size = sc->slot_size,
        .streams = sc->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Wire up compact trim for VARIABLE-format typed objects. sdb is a
       registry-cached SlotcaskDb shared across all concurrent request
       threads for this object (only guarded by a shared objlock rdlock
       here), so first-publish must be serialized and trim_fn must be
       published via release so any thread that observes it non-NULL also
       sees trim_ctx. */
    if (!atomic_load_explicit(&sdb->trim_fn, memory_order_acquire)) {
        pthread_mutex_lock(&sdb->trim_init_lock);
        if (!sdb->trim_fn) {
            sdb->trim_ctx = (void *)ts;
            atomic_store_explicit(&sdb->trim_fn, schema_trim_fn,
                                  memory_order_release);
        }
        pthread_mutex_unlock(&sdb->trim_init_lock);
    }

    /* dry_run: read + validate, never tombstone. */
    if (dry_run) {
        void *val = NULL; size_t vlen = 0;
        if (slotcask_get(sdb, key, klen, &val, &vlen) != 0) {
            OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
            return 0;
        }
        if (if_json) {
            SearchCriterion *crit = NULL; int ncrit = 0;
            if (parse_criteria_json(if_json, &crit, &ncrit) != 0) {
                free(val);
                OUT("{\"error\":\"invalid if condition\"}\n");
                return 1;
            }
            int pass = cas_check(ts, val, (int)vlen, crit, ncrit);
            if (!pass) {
                char *cur = typed_decode(ts, val, (uint32_t)vlen);
                OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
                    cur ? cur : "null");
                free(cur); free(val); free_criteria(crit, ncrit);
                return 1;
            }
            free_criteria(crit, ncrit);
        }
        free(val);
        OUT("{\"status\":\"would_delete\",\"key\":\"%s\"}\n", wire_key);
        return 0;
    }

    char idx_fields[MAX_FIELDS][256];
    int nidx = load_index_fields(db_root, object, idx_fields, MAX_FIELDS);
    for (int _i = 0; _i < nidx; _i++) idx_fields[_i][255] = '\0';

    enum IndexType idx_types[MAX_FIELDS];
    load_index_types(db_root, object, idx_types, MAX_FIELDS);

    SearchCriterion *crit = NULL;
    int ncrit = 0;
    if (if_json && parse_criteria_json(if_json, &crit, &ncrit) != 0) {
        OUT("{\"error\":\"invalid if condition\"}\n");
        return 1;
    }

    V2DeleteCtx ctx = {
        .db_root = db_root, .object = object, .splits = sc->splits,
        .idx_fields = idx_fields, .nidx = nidx,
        .idx_types = idx_types,
        .idx_ts = ts,
        .crit = crit, .ncrit = ncrit,
    };
    compute_hash_raw(key, klen, ctx.hash);
    int durability_degraded = 0;

    /* Only set the check hook when there's actual CAS criteria — otherwise
       v2_delete_check_fn would just return 1 unconditionally but the
       primitive doesn't know that and reads OLD anyway. Combined with
       skip_old_read on non-indexed paths, a plain DELETE bypasses
       read_record_value entirely. */
    int has_cas = (crit && ncrit > 0);
    SlotcaskDeleteOpts opts = {
        .check              = has_cas ? v2_delete_check_fn : NULL,
        .check_ctx          = &ctx,
        .apply_commit       = (nidx > 0) ? v2_delete_apply_commit : NULL,
        .pre_commit_ctx     = &ctx,
        .out_kf_shard       = &ctx.kf_shard,
        .out_kf_slot        = &ctx.kf_slot,
        .has_indexed_fields = (nidx > 0),
        .out_durability_degraded = &durability_degraded,
        /* apply_commit only dereferences old when there are index entries
           to drop; check only when there is CAS criteria. On non-indexed +
           non-CAS delete, opt out of read_record_value — saves a
           segcache_acquire + 100B memcpy + malloc/free per call.
           v2_delete_apply_commit handles old=NULL. */
        .skip_old_read      = (nidx == 0),
    };
    SlotcaskDeleteResult result = {0};
    int rc = slotcask_delete_with_hooks(sdb, key, klen, &opts, &result);

    if (result.not_found) {
        OUT("{\"status\":\"not_found\",\"key\":\"%s\"}\n", wire_key);
        free(result.current_value);
        free_criteria(crit, ncrit);
        return 0;
    }
    if (rc == -2 && result.condition_not_met) {
        char *cur = result.current_value
            ? typed_decode(ts, result.current_value, (uint32_t)result.current_vlen)
            : NULL;
        OUT("{\"error\":\"condition_not_met\",\"current\":%s}\n",
            cur ? cur : "null");
        free(cur); free(result.current_value); free_criteria(crit, ncrit);
        return 1;
    }
    if (rc != 0) {
        free(result.current_value);
        free_criteria(crit, ncrit);
        if (ctx.err_buf[0])
            OUT("{\"error\":\"%s\"}\n", ctx.err_buf);
        else
            OUT("{\"error\":\"delete failed\"}\n");
        return 1;
    }

    update_counts(db_root, object, -1, 1);
    LOG_INFO(LOG_SUB_SLOTCASK, "DELETE %s.%s (slotcask)", object, wire_key);
    free(result.current_value);
    free_criteria(crit, ncrit);
    if (durability_degraded)
        OUT("{\"status\":\"deleted\",\"key\":\"%s\",\"durability_degraded\":true}\n",
            wire_key);
    else
        OUT("{\"status\":\"deleted\",\"key\":\"%s\"}\n", wire_key);
    return 0;
}

/* ========== DELETE (with probing) ========== */

int cmd_delete(const char *db_root, const char *object,
               const char *key, size_t klen,
               const char *if_json, int dry_run) {
    Schema sc = load_schema(db_root, object);
    return cmd_delete_v2(db_root, object, key, klen, if_json, dry_run, &sc);
}

/* ========== MULTI-KEY GET ========== */

/* ========== Parallel multi-key EXISTS ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output */
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    int orig_idx;
    int found;
} MultiExistsEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiExistsEntry *entries;
    int count;
} MultiExistsShardWork;

/* Parse one wire-form key from a JSON array element into its storage
   form, computing hash + kf-shard id along the way. Shared by every
   multi-key path (bulk_exists, bulk_get). Auto-key UUID/seq forms get
   parsed to binary; otherwise the storage form is verbatim bytes.
   On malformed auto-key forms returns 0 — caller treats the row as a
   miss (found=0 / result_json=NULL) so clients see the same "missing"
   shape as a non-existent key. Returns 1 on success.
   Heap allocations land in *out_wire_key and *out_storage_key — caller
   owns both. */
static int parse_multi_key(const char *src, size_t klen, const Schema *sc,
                            char  **out_wire_key,
                            uint8_t **out_storage_key, size_t *out_storage_klen,
                            uint8_t out_hash[16], int *out_shard_id) {
    char *wire = malloc(klen + 1);
    if (!wire) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "parse_multi_key: malloc(%zu) failed for wire key buffer", klen + 1);
        return -1;
    }
    memcpy(wire, src, klen);
    wire[klen] = '\0';
    *out_wire_key = wire;

    uint8_t *skey = NULL;
    size_t   slen = 0;
    if (sc->auto_key == AK_UUID) {
        uint8_t bin[16];
        if (parse_uuid_string(wire, bin) == 0) {
            skey = malloc(16);
            if (skey) { memcpy(skey, bin, 16); slen = 16; }
        }
    } else if (sc->auto_key == AK_SEQ) {
        int64_t v;
        if (parse_seq_key(wire, &v) == 0) {
            skey = malloc(8);
            if (skey) {
                for (int b = 7; b >= 0; b--) { skey[b] = (uint8_t)(v & 0xFF); v >>= 8; }
                slen = 8;
            }
        }
    } else {
        skey = malloc(klen + 1);
        if (skey) { memcpy(skey, src, klen); skey[klen] = '\0'; slen = klen; }
    }
    *out_storage_key = skey;
    *out_storage_klen = slen;
    if (skey) {
        compute_hash_raw((const char *)skey, slen, out_hash);
        *out_shard_id = compute_record_shard(out_hash, sc->splits);
    } else {
        /* Malformed auto-key wire form → never matches; keep shard
           deterministic so bucket-sort works. */
        memset(out_hash, 0, 16);
        *out_shard_id = 0;
    }
    return 1;
}

static void *multi_exists_shard_worker(void *arg) {
    MultiExistsShardWork *sw = (MultiExistsShardWork *)arg;
    if (sw->count == 0) return NULL;

    /* bulk_lookup_in_kfshard amortises kfcache_acquire + segcache_acquire
       across the worker's records. The dispatcher already aligned
       shard_id with compute_record_shard so all entries here hash to the
       same kf shard. */
    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    SlotcaskBulkRec *batch = calloc(sw->count, sizeof(SlotcaskBulkRec));
    if (!batch) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_exists_shard_worker %s/%s: calloc(%d) failed for batch, keys reported as not-found", sw->db_root, sw->object, sw->count);
        return NULL;
    }
    for (int ei = 0; ei < sw->count; ei++) {
        MultiExistsEntry *e = &sw->entries[ei];
        batch[ei].key       = e->key;
        batch[ei].klen      = e->klen;
        batch[ei].value     = NULL;
        batch[ei].vlen      = 0;
        batch[ei].user_ctx  = NULL;
        batch[ei].old_value = NULL;
        batch[ei].old_vlen  = 0;
        batch[ei].status    = 0;
        batch[ei].was_update = 0;
    }
    int kf_shard_id = sw->entries[0].shard_id;  /* aligned by dispatcher */
    slotcask_bulk_lookup_in_kfshard(sdb, kf_shard_id, batch, (size_t)sw->count);
    for (int ei = 0; ei < sw->count; ei++) {
        sw->entries[ei].found = (batch[ei].status == 0) ? 1 : 0;
    }
    free(batch);
    return NULL;
}

/* Bucket-sort entries by shard_id, dispatch multi_exists_shard_worker
   in parallel across shards, copy `.found` results back into `entries`
   in original order. Shared by cmd_exists_multi, cmd_not_exists, and
   cmd_get_multi — they each parse their key list into `entries[]` then
   call this; afterwards they format their type-specific response from
   the populated `entries[].found`.

   Replaces the O(n²) insertion sort that previously dominated BULK
   EXISTS / BULK GET: at 10K keys × ~128 shards the sort was ~50M
   swaps before parallel_for even started. The bucket-sort is a
   single pass over entries plus one pass to fan out into per-shard
   buckets. */
static void multi_bucket_dispatch(MultiExistsEntry *entries, int key_count,
                                  const Schema *sc,
                                  const char *db_root, const char *object) {
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts    = calloc(sc->splits, sizeof(int));
    int *shard_to_worker = malloc(sc->splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc->splits; s++) if (shard_counts[s] > 0) nshard++;

    MultiExistsShardWork *workers = calloc(nshard, sizeof(MultiExistsShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc->splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object  = object;
                workers[g].sch     = sc;
                workers[g].count   = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiExistsEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    parallel_for_io(multi_exists_shard_worker, workers, nshard, sizeof(MultiExistsShardWork));

    /* Copy results back via orig_idx (no sorted[] indirection). */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].found = workers[g].entries[i].found;

    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    free(shard_counts);
    free(shard_to_worker);
}

/* mode=exists with keys[], returns {"k1":true,"k2":false,...} */
int cmd_exists_multi(const char *db_root, const char *object, const char *keys_json,
                     const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* xrealloc_or_free freed the old buffer; per-key
                       strings inside it are leaked (no double-free,
                       since the array holding their pointers is gone)
                       — acceptable on OOM. Coverity CID 1693843: the
                       previous "entries=NULL; break" left key_count>0
                       and the loop below would deref NULL. */
                    OUT("{\"error\":\"oom: bulk_exists keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

    /* Output in original order — build the response in a single buffer
       and OUT it once. Was 10K fprintf() calls in a loop, each taking
       the per-FILE stdio lock + parsing the format string (~15+ ms for
       10K keys at 1-2 µs/call). One snprintf-into-buffer + one OUT is
       hundreds of µs. */
    if (csv_delim) {
        OUT("key%cexists\n", csv_delim);
        size_t cap = (size_t)key_count * 64 + 64;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) for (int i = 0; i < key_count; i++) {
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
            if (pos + klen + 16 > cap) {
                cap = (pos + klen + 16) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: re-assert the post-grow invariant in subtractive
               form (no addition-overflow path). The pre-grow above already
               guarantees this — the check is dead code on the happy path
               and the compiler DCEs it — but Coverity's flow analysis
               loses the size-aliasing through the realloc→t→buf chain
               and flags the memcpy below as OVERRUN. CID 1693857/1693869. */
            if (cap < pos || cap - pos < klen) { free(buf); buf = NULL; break; }
            /* csv_emit_cell quotes if needed; do it via the existing helper but
               into our buffer via snprintf — replicate the no-quote-needed shape
               here to avoid re-implementing the quote logic. */
            /* CID 1693857/1693869 - bounds checked above, triage */
            memcpy(buf + pos, out_key, klen); pos += klen;
            /* Use SB_APPEND for bounded write — pre-grow above guarantees
               room, but the macro silences the CodeQL "potentially
               overflowing snprintf" finding by clamping to cap-1. */
            SB_APPEND(buf, pos, cap, "%c%s\n",
                       csv_delim, entries[i].found ? "true" : "false");
        }
        if (buf) {
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    } else {
        size_t cap = (size_t)key_count * 32 + 32;
        char *buf = malloc(cap);
        size_t pos = 0;
        if (buf) {
            buf[pos++] = '{';
            for (int i = 0; i < key_count; i++) {
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
                if (pos + klen + 16 > cap) {
                    cap = (pos + klen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow
                   invariant — see the equivalent comment in the CSV branch
                   above for rationale. Tautological on the happy path. */
                if (cap < pos || cap - pos < klen + 16) { free(buf); buf = NULL; break; }
                if (i) buf[pos++] = ',';
                buf[pos++] = '"';
                memcpy(buf + pos, out_key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].found) {
                    memcpy(buf + pos, "true", 4); pos += 4;
                } else {
                    memcpy(buf + pos, "false", 5); pos += 5;
                }
            }
        }
        if (buf) {
            buf[pos++] = '}'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        } else {
            OUT("{}\n");
        }
    }

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

/* mode=not-exists with keys[], returns keys that don't exist */
int cmd_not_exists(const char *db_root, const char *object, const char *keys_json) {
    int key_count = 0, key_cap = 256;
    MultiExistsEntry *entries = malloc(key_cap * sizeof(MultiExistsEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                MultiExistsEntry *t = xrealloc_or_free(entries, key_cap * sizeof(*t));
                if (!t) {
                    /* See cmd_exists_multi above for rationale (Coverity
                       CID 1693844). */
                    OUT("{\"error\":\"oom: bulk_get keys\"}\n");
                    return 1;
                }
                entries = t;
            }
            MultiExistsEntry *e = &entries[key_count++];
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->found = 0;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("[]\n"); return 0; }

    multi_bucket_dispatch(entries, key_count, &sc, db_root, object);

    /* Build output in one buffer; one fwrite. */
    size_t cap = (size_t)key_count * 32 + 16;
    char *buf = malloc(cap);
    if (buf) {
        size_t pos = 0;
        buf[pos++] = '[';
        int first = 1;
        for (int i = 0; i < key_count; i++) {
            if (entries[i].found) continue;
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            size_t klen = strlen(out_key);
            if (pos + klen + 8 > cap) {
                cap = (pos + klen + 8) * 2;
                char *t = realloc(buf, cap);
                if (!t) { free(buf); buf = NULL; break; }
                buf = t;
            }
            /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693871 */
            if (cap < pos || cap - pos < klen + 8) { free(buf); buf = NULL; break; }
            if (!first) buf[pos++] = ',';
            buf[pos++] = '"';
            /* CID 1693871 - bounds checked above, triage */
            memcpy(buf + pos, out_key, klen); pos += klen;
            buf[pos++] = '"';
            first = 0;
        }
        if (buf) {
            buf[pos++] = ']'; buf[pos++] = '\n';
            fwrite(buf, 1, pos, g_out ? g_out : stdout);
            free(buf);
        }
    }
    if (!buf) OUT("[]\n");

    /* (workers + per-worker entries freed inside multi_bucket_dispatch.) */
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}

/* ========== Parallel multi-key GET ========== */

typedef struct {
    char *key;          /* storage form: binary for auto_key, string otherwise */
    size_t klen;        /* binary length (was strlen(key) before auto-key) */
    char *wire_key;     /* wire-form string for response output (NULL → use key) */
    uint8_t hash[16];
    int shard_id;
    int start_slot;
    int orig_idx;
    char *result_json; /* NULL if not found */
} MultiGetEntry;

typedef struct {
    const char *db_root;
    const char *object;
    const Schema *sch;
    MultiGetEntry *entries;
    int count;
    FieldSchema *fs;
} MultiGetShardWork;

/* Callback for multi-get: decodes value inline into entry's result_json. */
static int multi_get_fetch_cb(const uint8_t hash[16],
                               const void *key, size_t klen,
                               const void *value, size_t vlen,
                               void *ctx_ptr) {
    (void)key; (void)klen;
    MultiGetShardWork *sw = (MultiGetShardWork *)ctx_ptr;
    for (int ei = 0; ei < sw->count; ei++) {
        if (memcmp(sw->entries[ei].hash, hash, 16) == 0) {
            char *decoded = sw->fs ? typed_decode(sw->fs->ts,
                                                   (const uint8_t *)value,
                                                   (uint32_t)vlen) : NULL;
            sw->entries[ei].result_json = decoded ? decoded : strdup("null");
            break;
        }
    }
    return 0;
}

static void *multi_get_shard_worker(void *arg) {
    MultiGetShardWork *sw = (MultiGetShardWork *)arg;
    if (sw->count == 0) return NULL;

    SlotcaskSchemaInfo info = {
        .splits = sw->sch->splits, .slot_size = sw->sch->slot_size,
        .streams = sw->sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(sw->db_root, sw->object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: slotcask_registry_get failed, %d keys reported as missing", sw->db_root, sw->object, sw->count);
        return NULL;
    }

    /* Extract pre-computed hashes from entries */
    uint8_t (*hashes)[16] = malloc((size_t)sw->count * sizeof(*hashes));
    if (!hashes) {
        LOG_ERROR(LOG_SUB_SLOTCASK, "multi_get_shard_worker %s/%s: malloc(%d * 16) failed for hashes array", sw->db_root, sw->object, sw->count);
        return NULL;
    }
    for (int ei = 0; ei < sw->count; ei++)
        memcpy(hashes[ei], sw->entries[ei].hash, 16);

    /* Batch resolve+fetch — two-phase model resolves KF shards internally
       and parallelizes segment reads. Callback decodes each found record. */
    slotcask_bulk_resolve_and_fetch(sdb, hashes, (size_t)sw->count,
                                     sw, multi_get_fetch_cb);

    free(hashes);
    return NULL;
}

int cmd_get_multi(const char *db_root, const char *object, const char *keys_json,
                  const char *format, const char *delimiter) {
    char csv_delim = (format && strcmp(format, "csv") == 0) ? parse_csv_delim(delimiter) : 0;
    /* Parse keys */
    int key_count = 0, key_cap = 256;
    MultiGetEntry *entries = malloc(key_cap * sizeof(MultiGetEntry));
    Schema sc = load_schema(db_root, object);

    const char *p = json_skip(keys_json);
    if (*p == '[') p++;
    while (*p) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
            size_t klen = p - start;
            if (*p == '"') p++;
            if (key_count >= key_cap) {
                key_cap *= 2;
                /* Plain realloc (not xrealloc_or_free): on failure we still
                   need the old `entries` array intact to free each already-
                   parsed entry's key/wire_key before dropping the array
                   itself (CID 1696478). Resetting key_count to 0 here also
                   makes the `key_count == 0` early-return below fire
                   correctly instead of falling through to a NULL entries[]
                   dereference. */
                MultiGetEntry *t = realloc(entries, key_cap * sizeof(*t));
                if (!t) {
                    for (int j = 0; j < key_count; j++) {
                        free(entries[j].key);
                        free(entries[j].wire_key);
                    }
                    free(entries);
                    entries = NULL;
                    key_count = 0;
                    break;
                }
                entries = t;
            }
            MultiGetEntry *e = &entries[key_count++];
            /* Wire form for response output; storage form (binary for
               auto_key=uuid/seq, verbatim otherwise); hash + kf-shard
               bucket — see parse_multi_key. */
            parse_multi_key(start, klen, &sc,
                             &e->wire_key,
                             (uint8_t **)&e->key, &e->klen,
                             e->hash, &e->shard_id);
            e->start_slot = 0;
            e->result_json = NULL;
        } else p++;
    }

    if (key_count == 0) { free(entries); OUT("{}\n"); return 0; }

    /* Bucket-sort by shard_id — same fix as cmd_exists_multi above. */
    for (int i = 0; i < key_count; i++) entries[i].orig_idx = i;
    int *shard_counts = calloc(sc.splits, sizeof(int));
    int *shard_to_worker = malloc(sc.splits * sizeof(int));
    for (int i = 0; i < key_count; i++) shard_counts[entries[i].shard_id]++;
    int nshard = 0;
    for (int s = 0; s < sc.splits; s++) if (shard_counts[s] > 0) nshard++;

    FieldSchema fs; init_field_schema(&fs, db_root, object);
    MultiGetShardWork *workers = calloc(nshard, sizeof(MultiGetShardWork));
    {
        int g = 0;
        for (int s = 0; s < sc.splits; s++) {
            if (shard_counts[s] > 0) {
                workers[g].db_root = db_root;
                workers[g].object = object;
                workers[g].sch = &sc;
                workers[g].fs = (fs.ts || fs.nfields > 0) ? &fs : NULL;
                workers[g].count = 0;
                workers[g].entries = malloc(shard_counts[s] * sizeof(MultiGetEntry));
                shard_to_worker[s] = g;
                g++;
            } else {
                shard_to_worker[s] = -1;
            }
        }
    }
    for (int i = 0; i < key_count; i++) {
        int w = shard_to_worker[entries[i].shard_id];
        workers[w].entries[workers[w].count++] = entries[i];
    }

    /* Parallel fetch */
    parallel_for_io(multi_get_shard_worker, workers, nshard, sizeof(MultiGetShardWork));

    /* Copy results back to entries[] via orig_idx. */
    for (int g = 0; g < nshard; g++)
        for (int i = 0; i < workers[g].count; i++)
            entries[workers[g].entries[i].orig_idx].result_json = workers[g].entries[i].result_json;
    free(shard_counts); free(shard_to_worker);

    /* Output in original key order */
    if (csv_delim) {
        /* Header: key + schema fields (no projection on get-multi). */
        OUT("key");
        if (fs.ts) {
            for (int i = 0; i < fs.ts->nfields; i++) {
                if (fs.ts->fields[i].removed) continue;
                char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                csv_emit_cell(fs.ts->fields[i].name, csv_delim);
            }
        }
        OUT("\n");
        for (int i = 0; i < key_count; i++) {
            if (!entries[i].result_json) continue;
            const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
            csv_emit_cell(out_key, csv_delim);
            JsonObj value_obj;
            int have_value = json_parse_object(entries[i].result_json,
                                               strlen(entries[i].result_json),
                                               &value_obj) >= 0;
            if (fs.ts) {
                for (int fi = 0; fi < fs.ts->nfields; fi++) {
                    if (fs.ts->fields[fi].removed) continue;
                    char d[2] = { csv_delim, '\0' }; OUT("%s", d);
                    char *pv = !have_value ? NULL :
                        (fs.ts->fields[fi].type == FT_VARCHAR
                         ? json_obj_strdup_unescaped(&value_obj, fs.ts->fields[fi].name, NULL)
                         : json_obj_strdup(&value_obj, fs.ts->fields[fi].name));
                    csv_emit_cell(pv, csv_delim);
                    free(pv);
                }
            }
            OUT("\n");
            free(entries[i].result_json);
        }
    } else {
        /* Build the response in one buffer + one fwrite. Was 10K+
           fprintf calls in a loop (each takes the per-FILE stdio lock
           + parses the format string), the dominant cost in BULK GET
           on top of the bucket-sort fix. */
        size_t cap = (size_t)key_count * 256 + 64;
        char *buf = malloc(cap);
        if (buf) {
            size_t pos = 0;
            buf[pos++] = '{';
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                size_t klen = strlen(out_key);
                size_t vlen = entries[i].result_json ? strlen(entries[i].result_json) : 4;
                if (pos + klen + vlen + 16 > cap) {
                    cap = (pos + klen + vlen + 16) * 2;
                    char *t = realloc(buf, cap);
                    if (!t) { free(buf); buf = NULL; break; }
                    buf = t;
                }
                /* Coverity: subtractive re-assertion of the post-grow invariant. CID 1693870 */
                if (cap < pos || cap - pos < klen + vlen + 16) { free(buf); buf = NULL; break; }
                if (!first) buf[pos++] = ',';
                first = 0;
                buf[pos++] = '"';
                /* CID 1693870 - bounds checked above, triage */
                memcpy(buf + pos, out_key, klen); pos += klen;
                buf[pos++] = '"'; buf[pos++] = ':';
                if (entries[i].result_json) {
                    /* CID 1693872 - bounds checked above, triage */
                    memcpy(buf + pos, entries[i].result_json, vlen); pos += vlen;
                    free(entries[i].result_json);
                } else {
                    memcpy(buf + pos, "null", 4); pos += 4;
                }
            }
            if (buf) {
                buf[pos++] = '}'; buf[pos++] = '\n';
                fwrite(buf, 1, pos, g_out ? g_out : stdout);
                free(buf);
            }
        }
        if (!buf) {
            /* OOM fallback — old per-record path. */
            OUT("{");
            int first = 1;
            for (int i = 0; i < key_count; i++) {
                if (!first) OUT(",");
                first = 0;
                const char *out_key = entries[i].wire_key ? entries[i].wire_key : entries[i].key;
                if (entries[i].result_json) {
                    OUT("\"%s\":%s", out_key, entries[i].result_json);
                    free(entries[i].result_json);
                } else {
                    OUT("\"%s\":null", out_key);
                }
            }
            OUT("}\n");
        }
    }

    /* cmd_get_multi has its own MultiGetEntry/MultiGetShardWork types
       (extra `fs` FieldSchema field, different result type) so it can't
       share the multi_bucket_dispatch helper used by exists/not_exists;
       clean up workers explicitly. */
    for (int g = 0; g < nshard; g++) free(workers[g].entries);
    free(workers);
    for (int i = 0; i < key_count; i++) { free(entries[i].key); free(entries[i].wire_key); }
    free(entries);
    return 0;
}
