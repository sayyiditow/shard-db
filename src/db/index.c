#include "types.h"
#include "bitmap.h"
#include "trigram.h"
#include "slotcask.h"

/* Per-shard build worker shared by cmd_add_index and cmd_add_indexes; defined
   below alongside the partition_by_shard helper. */

/* ========== Binary Sorted Index (B-tree style) ========== */

/*
 * Single binary file per field: $DB_ROOT/<object>/indexes/<field>.idx
 *
 * Layout:
 *   Header (32 bytes):
 *     uint64_t count       - number of entries
 *     uint32_t val_size    - padded value size per entry (default 128)
 *     uint32_t is_numeric  - 1 if all values are numeric (numeric sort)
 *     uint8_t  reserved[16]
 *
 *   Entries (fixed-size, sorted by value):
 *     char value[val_size]    - null-padded value
 *     char hash[32]           - record hash hex
 *     = entry_size = val_size + 32
 *
 * Operations:
 *   Insert: binary search → memmove rest down → write entry → count++
 *   Delete: binary search → memmove rest up → count--
 *   Search: binary search → scan forward for all matches
 *   Range:  binary search to start → scan forward until past end
 *
 * All operations mmap the file with MAP_SHARED for in-place modification.
 */


/* ========== Per-shard btree index wrappers ==========
   See types.h for the contract. Layout: <db_root>/<obj>/indexes/<field>/<NNN>.idx
   with index_splits_for(splits) shards (non-linear curve, capped at 128
   for splits=4096). Writes route by hash16 to a single shard
   (idx_shard_for_hash); reads fan out across all shards. */

void btree_idx_insert(const char *db_root, const char *object,
                      const char *field, int splits,
                      const char *value, size_t vlen,
                      const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, idx_shard);
    btree_insert(idx_path, value, vlen, hash);
}

void btree_idx_delete(const char *db_root, const char *object,
                      const char *field, int splits,
                      const char *value, size_t vlen,
                      const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, idx_shard);
    btree_delete(idx_path, value, vlen, hash);
}

/* Trigram index entry insert/delete. Mirror of btree_idx_* but routes
   to .tg files instead of .idx — both extensions share the BTRH btree
   format, so the underlying primitives (btree_insert/btree_delete) are
   reused unchanged. A field may carry BOTH .idx and .tg simultaneously;
   bt_cache treats them as independent path-keyed cache entries. */
void tg_idx_insert(const char *db_root, const char *object,
                   const char *field, int splits,
                   const uint8_t trigram[3],
                   const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char tg_path[PATH_MAX];
    tg_build_path(tg_path, sizeof(tg_path), db_root, object, field, idx_shard);
    btree_insert(tg_path, (const char *)trigram, 3, hash);
}

void tg_idx_delete(const char *db_root, const char *object,
                   const char *field, int splits,
                   const uint8_t trigram[3],
                   const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char tg_path[PATH_MAX];
    tg_build_path(tg_path, sizeof(tg_path), db_root, object, field, idx_shard);
    btree_delete(tg_path, (const char *)trigram, 3, hash);
}

/* Per-shard parallel-walk machinery. parallel_for spawns one task per shard,
   each calls the appropriate single-file btree_search/range/range_ex on its
   shard. The shared `cb` and `ctx` must be thread-safe — callers that need
   per-worker state are responsible (see idx_count_cb's atomic counter and
   collect_hash_cb's mutex). KeySet-based callbacks (intersect_*, or_*) are
   already lock-free. Avoid nesting: parent callers must NOT themselves run
   under parallel_for or the pool can deadlock. */

typedef struct {
    char idx_path[PATH_MAX];
    int op;                  /* 0 = search, 1 = range, 2 = range_ex */
    const char *value;
    size_t vlen;
    const char *min_val;
    size_t      min_len;
    int         min_exclusive;
    const char *max_val;
    size_t      max_len;
    int         max_exclusive;
    bt_result_cb cb;
    void *ctx;
} ShardWalkArg;

static void *shard_walk_worker(void *arg) {
    ShardWalkArg *sw = (ShardWalkArg *)arg;
    switch (sw->op) {
        case 0: btree_search(sw->idx_path, sw->value, sw->vlen,
                             sw->cb, sw->ctx); break;
        case 1: btree_range(sw->idx_path,
                            sw->min_val, sw->min_len,
                            sw->max_val, sw->max_len,
                            sw->cb, sw->ctx); break;
        case 2: btree_range_ex(sw->idx_path,
                               sw->min_val, sw->min_len, sw->min_exclusive,
                               sw->max_val, sw->max_len, sw->max_exclusive,
                               sw->cb, sw->ctx); break;
    }
    /* Flush any thread-local accumulator the callback populated while
       walking this shard. Today only idx_count_cb batches (it amortises
       per-match atomic-adds into one per-shard-worker atomic-add); if
       more callbacks adopt the same pattern, hook them in this single
       cleanup point. No-op for callbacks that don't use TLS. */
    idx_count_cb_flush_thread();
    return NULL;
}

static void shard_walk_dispatch(const char *db_root, const char *object,
                                const char *field, int splits,
                                ShardWalkArg *tmpl,
                                bt_result_cb cb, void *ctx) {
    int n = index_splits_for(splits);
    ShardWalkArg *args = malloc((size_t)n * sizeof(ShardWalkArg));
    for (int s = 0; s < n; s++) {
        args[s] = *tmpl;
        build_idx_path(args[s].idx_path, sizeof(args[s].idx_path),
                       db_root, object, field, s);
        args[s].cb = cb;
        args[s].ctx = ctx;
    }
    parallel_for(shard_walk_worker, args, n, sizeof(ShardWalkArg));
    free(args);
}

void btree_idx_search(const char *db_root, const char *object,
                      const char *field, int splits,
                      const char *value, size_t vlen,
                      bt_result_cb cb, void *ctx) {
    ShardWalkArg t = {{0}, .op = 0, .value = value, .vlen = vlen};
    shard_walk_dispatch(db_root, object, field, splits, &t, cb, ctx);
}

void btree_idx_range(const char *db_root, const char *object,
                     const char *field, int splits,
                     const char *min_val, size_t min_len,
                     const char *max_val, size_t max_len,
                     bt_result_cb cb, void *ctx) {
    ShardWalkArg t = {{0}, .op = 1,
                      .min_val = min_val, .min_len = min_len,
                      .max_val = max_val, .max_len = max_len};
    shard_walk_dispatch(db_root, object, field, splits, &t, cb, ctx);
}

void btree_idx_range_ex(const char *db_root, const char *object,
                        const char *field, int splits,
                        const char *min_val, size_t min_len, int min_exclusive,
                        const char *max_val, size_t max_len, int max_exclusive,
                        bt_result_cb cb, void *ctx) {
    ShardWalkArg t = {{0}, .op = 2,
                      .min_val = min_val, .min_len = min_len, .min_exclusive = min_exclusive,
                      .max_val = max_val, .max_len = max_len, .max_exclusive = max_exclusive};
    shard_walk_dispatch(db_root, object, field, splits, &t, cb, ctx);
}

/* ----- Globally-ordered walk (for cursor pagination) =====
   K-way merge across all idx shards. Each shard runs a streaming
   BtRangeIter; a min-heap (ASC) or max-heap (DESC) of (current entry,
   shard_id) picks the next globally-ordered entry. O(splits/4) memory —
   one entry materialised per shard at a time, regardless of total range
   cardinality. */

typedef struct {
    BtRangeIter *iter;
    /* Currently-buffered head entry — copied out of the iterator since the
       iterator's internal buffer gets overwritten on next(). */
    char    value[BT_MAX_VAL_LEN];
    size_t  vlen;
    uint8_t hash[BT_HASH_SIZE];
    int     has_entry;       /* 1 if value/hash hold a valid head, 0 if drained */
    int     shard_id;        /* tie-break ordering when (value,hash) collide */
} ShardCursor;

static int sc_cmp_asc(const ShardCursor *a, const ShardCursor *b) {
    size_t m = a->vlen < b->vlen ? a->vlen : b->vlen;
    int r = memcmp(a->value, b->value, m);
    if (r != 0) return r;
    if (a->vlen != b->vlen) return a->vlen < b->vlen ? -1 : 1;
    r = memcmp(a->hash, b->hash, BT_HASH_SIZE);
    if (r != 0) return r;
    return a->shard_id - b->shard_id;
}

/* Refill the head entry of cursor c by pulling one from its iterator. */
static void sc_pull(ShardCursor *c) {
    const char *v;
    size_t vl;
    const uint8_t *h;
    if (btree_range_iter_next(c->iter, &v, &vl, &h)) {
        c->vlen = vl > BT_MAX_VAL_LEN ? BT_MAX_VAL_LEN : vl;
        memcpy(c->value, v, c->vlen);
        memcpy(c->hash, h, BT_HASH_SIZE);
        c->has_entry = 1;
    } else {
        c->has_entry = 0;
    }
}

void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_result_cb cb, void *ctx) {
    int n = index_splits_for(splits);
    ShardCursor *cursors = calloc((size_t)n, sizeof(ShardCursor));
    if (!cursors) return;

    /* Open one streaming iterator per shard and prime its head entry. Shards
       whose iterator fails to open (missing file, etc.) drop out — they
       contribute nothing and don't block the merge. */
    for (int s = 0; s < n; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
        cursors[s].shard_id = s;
        cursors[s].iter = btree_range_iter_open(idx_path,
                                                min_val, min_len, min_exclusive,
                                                max_val, max_len, max_exclusive,
                                                desc);
        if (cursors[s].iter) sc_pull(&cursors[s]);
    }

    /* Linear-scan-pick-best is fine at this scale — splits/4 ≤ 1024 and the
       per-iteration callback cost (record fetch + criteria_match_tree)
       dwarfs the O(N) selection. Heap would shave µs at high splits but
       complicates code without changing the dominant cost. */
    while (1) {
        int best = -1;
        for (int s = 0; s < n; s++) {
            if (!cursors[s].has_entry) continue;
            if (best < 0) { best = s; continue; }
            int cmp = sc_cmp_asc(&cursors[s], &cursors[best]);
            if (desc ? cmp > 0 : cmp < 0) best = s;
        }
        if (best < 0) break;  /* every iterator drained */

        ShardCursor *bc = &cursors[best];
        if (cb(bc->value, bc->vlen, bc->hash, ctx) < 0) break;
        sc_pull(bc);
    }

    for (int s = 0; s < n; s++) {
        if (cursors[s].iter) btree_range_iter_close(cursors[s].iter);
    }
    free(cursors);
}

void btree_idx_unlink_all(const char *db_root, const char *object,
                          const char *field, int splits) {
    int n = index_splits_for(splits);
    for (int s = 0; s < n; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
        btree_cache_invalidate(idx_path);
        unlink(idx_path);
    }
    /* Drop the (now-empty) field directory. */
    char dir_path[PATH_MAX];
    snprintf(dir_path, sizeof(dir_path), "%s/%s/indexes/%s",
             db_root, object, field);
    rmdir(dir_path);
}

int btree_idx_exists(const char *db_root, const char *object,
                     const char *field, int splits) {
    int n = index_splits_for(splits);
    for (int s = 0; s < n; s++) {
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, s);
        struct stat st;
        if (stat(idx_path, &st) == 0 && st.st_size > 0) return 1;
    }
    return 0;
}

/* Wrapper for insert-time indexing — uses B+ tree */
void write_index_entry(const char *db_root, const char *object,
                              const char *field, int splits,
                              const uint8_t *val, size_t vlen,
                              const uint8_t hash16[16]) {
    btree_idx_insert(db_root, object, field, splits,
                     (const char *)val, vlen, hash16);
}

/* Build the bitmap shard file path for a (object, field, NNN) tuple,
   mirroring build_idx_path / bm_build_path. */
static void bitmap_shard_path(char *out, size_t outlen,
                              const char *db_root, const char *object,
                              const char *field, int shard_idx) {
    bm_build_path(out, outlen, db_root, object, field, shard_idx);
}

/* The earlier thread-local bitmap cache was replaced by the global
   path-keyed bm_cache (bitmap.c). Callers now bm_open(writer=1) for
   each write and bm_close to release — the rwlock is held only across
   the per-write critical section, and the cache keeps the mmap alive
   so subsequent same-shard writes pay just the rwlock cost. The flush
   API is kept as a no-op shim so existing batch-boundary callers (CRUD
   pre_commits, bulk worker) don't break — they're effectively no-ops
   now that there's no TLS state to release. */
void bm_flush_thread_bitmap_cache(void) {
    /* no-op — see comment above. */
}

/* Bitmap insert/delete: open the per-data-shard `.bm` file, flip the
   bit for (value, slot). `bool_fastpath` is detected by checking field
   value bytes — a bool encoding is exactly 1 byte (0x00 or 0x01).
   For varchar enums the value is variable-length bytes.

   Returns 0 on success, -1 if the dict cap was exceeded (the caller
   surfaces an actionable error pointing the operator at btree). */
static int bitmap_update(const char *db_root, const char *object,
                         const char *field, int kf_shard, int splits,
                         uint32_t kf_slot, uint32_t max_values,
                         const uint8_t *new_val, size_t new_len,
                         const uint8_t *old_val, size_t old_len) {
    char path[1024];
    bitmap_shard_path(path, sizeof(path), db_root, object, field, kf_shard);

    int bool_fastpath = 0;
    if (new_val && new_len == 1) bool_fastpath = 1;
    else if (old_val && old_len == 1) bool_fastpath = 1;

    int slots = (int)slotcask_default_slots_for_splits(splits);

    /* The global bm_cache (bitmap.c) keeps the mmap alive across calls;
       bm_open / bm_close just acquire + release the per-entry rwlock. */
    BitmapShard *bm = bm_open(path, slots, 1, bool_fastpath, max_values,
                              1 /* writer */);
    if (!bm) return 0;

    /* Auto-grow on slot overflow. Slotcask doubles a kf shard's
       slots_per_shard on auto-resplit (80% load trigger); the bitmap
       file was sized from the default slot tier so subsequent inserts
       beyond that point would land at slots above bm->slots and silently
       no-op. Detect and grow inline — cheap rewrite that happens at
       most a few times over a shard's lifetime. */
    if (kf_slot >= bm_slots(bm)) {
        uint32_t want = kf_slot + 1;
        /* Round up to next power of 2 so subsequent inserts don't keep
           triggering grows. Cap at 2^31 — past that the index isn't
           the bottleneck. */
        uint32_t grown = 1;
        while (grown < want && grown < 0x80000000u) grown <<= 1;
        bm_grow(bm, grown);
    }

    int rc = 0;
    if (old_val) bm_clear(bm, old_val, old_len, kf_slot);
    if (new_val) {
        if (bm_set(bm, new_val, new_len, kf_slot) != 0) {
            rc = -1;
        }
    }
    /* Release the per-entry rwlock. The cache keeps fd + mmap alive
       so a same-path follow-up call pays just the rwlock acquire. */
    bm_close(bm);
    return rc;
}

/* Per-field index update worker — dispatched via parallel_for from
   every CRUD pre_commit (insert, update, delete, bulk variants).
   NULL keys = skip that side, so the same worker covers insert-only,
   delete-only, and changed-field update with NULL acting as "no-op".

   Dispatches on UpdateIdxArg::type. Composite indexes (field1+field2)
   are always btree per the 2026.05.7 contract. */
void *update_idx_fn(void *arg) {
    UpdateIdxArg *a = (UpdateIdxArg *)arg;

    switch (a->type) {
        case IT_BTREE:
            if (a->old_key)
                delete_index_entry(a->db_root, a->object, a->field, a->splits,
                                   a->old_key, a->old_len, a->hash);
            if (a->new_key)
                write_index_entry(a->db_root, a->object, a->field, a->splits,
                                  a->new_key, a->new_len, a->hash);
            break;

        case IT_TRIGRAM: {
            /* Diff the two distinct-trigram sets. For every trigram in
               old but not in new → delete its leaf entry. For every in
               new but not in old → insert. Trigrams in both stay put
               (record still contains them). Nested-loop comparison is
               O(n_old × n_new); fine at typical short-field sizes. For
               very long varchars the diff cost grows but stays bounded
               by TG_MAX_DISTINCT² ≈ 16M memcmps worst case ~30ms — the
               record's own write cost dominates anyway. */
            uint8_t old_tg[TG_MAX_DISTINCT][3];
            uint8_t new_tg[TG_MAX_DISTINCT][3];
            size_t n_old = a->old_key
                ? tg_extract_distinct(a->old_key, a->old_len, old_tg, TG_MAX_DISTINCT)
                : 0;
            size_t n_new = a->new_key
                ? tg_extract_distinct(a->new_key, a->new_len, new_tg, TG_MAX_DISTINCT)
                : 0;

            /* Delete old-only trigrams. */
            for (size_t i = 0; i < n_old; i++) {
                int in_new = 0;
                for (size_t j = 0; j < n_new; j++) {
                    if (memcmp(old_tg[i], new_tg[j], 3) == 0) { in_new = 1; break; }
                }
                if (!in_new) {
                    tg_idx_delete(a->db_root, a->object, a->field, a->splits,
                                  old_tg[i], a->hash);
                }
            }

            /* Insert new-only trigrams. */
            for (size_t i = 0; i < n_new; i++) {
                int in_old = 0;
                for (size_t j = 0; j < n_old; j++) {
                    if (memcmp(new_tg[i], old_tg[j], 3) == 0) { in_old = 1; break; }
                }
                if (!in_old) {
                    tg_idx_insert(a->db_root, a->object, a->field, a->splits,
                                  new_tg[i], a->hash);
                }
            }
            break;
        }

        case IT_BITMAP:
            /* The slow-path insert case can call us with kf_slot=0 and
               kf_shard=0 unset (slotcask determined the slot AFTER
               pre_commit). Detect by the absence of a publish: if the
               caller didn't write to the out-params, skip the bitmap
               update. Reindex will catch it. We can't reliably tell
               "unset" from "shard 0 + slot 0", so the field-level
               convention is: callers that don't have the slot leave
               type=IT_BTREE and rely on the btree path. Anyone setting
               type=IT_BITMAP guarantees they've populated shard+slot. */
            a->out_error = bitmap_update(a->db_root, a->object, a->field,
                                         a->kf_shard, a->splits,
                                         a->kf_slot, a->bm_max_values,
                                         a->new_key, a->new_len,
                                         a->old_key, a->old_len);
            break;
    }
    return NULL;
}

/* Delete from index — uses B+ tree */
void delete_index_entry(const char *db_root, const char *object,
                               const char *field, int splits,
                               const uint8_t *val, size_t vlen,
                               const uint8_t hash16[16]) {
    btree_idx_delete(db_root, object, field, splits,
                     (const char *)val, vlen, hash16);
}

/* ========== Parallel indexing ========== */

typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    int splits;
    uint8_t *val;               /* heap-owned bytes (index-key encoding); freed by caller */
    size_t vlen;
    const uint8_t *hash16;
} IndexThreadArg;

void *index_thread_fn(void *arg) {
    IndexThreadArg *a = (IndexThreadArg *)arg;
    write_index_entry(a->db_root, a->object, a->field, a->splits,
                      a->val, a->vlen, a->hash16);
    return NULL;
}

/* Extract a field value from JSON — handles composite fields (e.g. "city+age") */
char *extract_field_value(const char *json, const char *field_name) {
    if (strchr(field_name, '+')) {
        /* Composite: split, extract sub-fields, concatenate */
        char fbuf[256];
        strncpy(fbuf, field_name, 255); fbuf[255] = '\0';
        const char *subs[16]; int nsub = 0;
        char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
        while (tok && nsub < 16) { subs[nsub++] = tok; tok = strtok_r(NULL, "+", &_tok_save); }
        char *svals[16];
        json_get_fields(json, subs, nsub, svals);
        char cat[4096]; int pos = 0; int ok = 1;
        for (int i = 0; i < nsub; i++) {
            if (!svals[i] || !svals[i][0]) { ok = 0; break; }
            int len = strlen(svals[i]);
            if (pos + len < (int)sizeof(cat)) { memcpy(cat + pos, svals[i], len); pos += len; }
        }
        cat[pos] = '\0';
        for (int i = 0; i < nsub; i++) free(svals[i]);
        return (ok && pos > 0) ? strdup(cat) : NULL;
    }
    JsonObj jo;
    json_parse_object(json, strlen(json), &jo);
    return json_obj_strdup(&jo, field_name);
}

/* Build concatenated value for a composite index field like "status+invoiceDate"
   from pre-extracted sub-field values. Returns malloc'd string or NULL. */
char *build_composite_value(const char *field_name, const char *json_value) {
    /* Check if composite (contains +) */
    if (!strchr(field_name, '+')) return NULL;

    /* Split field names on + */
    char fbuf[256];
    strncpy(fbuf, field_name, 255); fbuf[255] = '\0';
    const char *sub_fields[16];
    int nsub = 0;
    char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
    while (tok && nsub < 16) { sub_fields[nsub++] = tok; tok = strtok_r(NULL, "+", &_tok_save); }

    /* Extract sub-field values from JSON */
    char *sub_vals[16];
    json_get_fields(json_value, sub_fields, nsub, sub_vals);

    /* Concatenate all values */
    char result[4096];
    int pos = 0;
    int all_present = 1;
    for (int i = 0; i < nsub; i++) {
        if (!sub_vals[i] || sub_vals[i][0] == '\0') { all_present = 0; break; }
        int len = strlen(sub_vals[i]);
        if (pos + len >= (int)sizeof(result)) { all_present = 0; break; }
        memcpy(result + pos, sub_vals[i], len);
        pos += len;
    }
    result[pos] = '\0';

    for (int i = 0; i < nsub; i++) free(sub_vals[i]);
    return all_present ? strdup(result) : NULL;
}

void index_parallel(const char *db_root, const char *object, int splits,
                           const char *value, const uint8_t hash16[16],
                           char fields[][256], int nfields,
                           const enum IndexType *types) {
    if (nfields <= 0) return;

    TypedSchema *ts = load_typed_schema(db_root, object);

    /* Collect all unique sub-field names from single + composite indexes */
    const char *unique_keys[MAX_FIELDS * 4];
    int unique_count = 0;
    for (int i = 0; i < nfields; i++) {
        if (strchr(fields[i], '+')) {
            char fbuf[256];
            strncpy(fbuf, fields[i], 255); fbuf[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
            while (tok) {
                int found = 0;
                for (int j = 0; j < unique_count; j++)
                    if (strcmp(unique_keys[j], tok) == 0) { found = 1; break; }
                if (!found && unique_count < MAX_FIELDS * 4)
                    unique_keys[unique_count++] = strdup(tok);
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            int found = 0;
            for (int j = 0; j < unique_count; j++)
                if (strcmp(unique_keys[j], fields[i]) == 0) { found = 1; break; }
            if (!found && unique_count < MAX_FIELDS * 4)
                unique_keys[unique_count++] = fields[i];
        }
    }

    char *extracted[MAX_FIELDS * 4];
    json_get_fields(value, unique_keys, unique_count, extracted);

    IndexThreadArg args[MAX_FIELDS];
    int tcount = 0;

    /* Heap-owned per-index key buffers — freed after parallel_for returns. */
    uint8_t *idx_keys[MAX_FIELDS];
    memset(idx_keys, 0, sizeof(idx_keys));

    for (int i = 0; i < nfields; i++) {
        /* Skip fields whose index type isn't btree — they're maintained
           via update_idx_fn's type-dispatch path. Composites are always
           btree (the non-btree composite case is rejected upstream at
           create-object). */
        if (types && types[i] != IT_BTREE) continue;

        uint8_t *key_buf = NULL;
        size_t key_len = 0;

        if (strchr(fields[i], '+')) {
            /* Composite — ASCII concat of sub-field values (raw bytes). */
            char fbuf[256];
            strncpy(fbuf, fields[i], 255); fbuf[255] = '\0';
            char result[4096];
            int pos = 0;
            int all_present = 1;
            char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
            while (tok) {
                for (int j = 0; j < unique_count; j++) {
                    if (strcmp(unique_keys[j], tok) == 0) {
                        if (!extracted[j] || extracted[j][0] == '\0') { all_present = 0; break; }
                        int len = strlen(extracted[j]);
                        if (pos + len < (int)sizeof(result)) {
                            memcpy(result + pos, extracted[j], len);
                            pos += len;
                        }
                        break;
                    }
                }
                if (!all_present) break;
                tok = strtok_r(NULL, "+", &_tok_save);
            }
            if (all_present && pos > 0) {
                key_buf = malloc((size_t)pos);
                memcpy(key_buf, result, (size_t)pos);
                key_len = (size_t)pos;
            }
        } else {
            /* Single field — encode textual JSON value as index-key bytes. */
            const char *txt = NULL;
            for (int j = 0; j < unique_count; j++) {
                if (strcmp(unique_keys[j], fields[i]) == 0) {
                    txt = extracted[j];
                    break;
                }
            }
            if (txt && txt[0]) {
                int fidx = ts ? typed_field_index(ts, fields[i]) : -1;
                if (fidx >= 0) {
                    const TypedField *f = &ts->fields[fidx];
                    size_t cap = (size_t)(f->size > 8 ? f->size : 8);
                    key_buf = malloc(cap);
                    encode_field_for_index(f, txt, strlen(txt), key_buf, &key_len);
                    if (key_len == 0) { free(key_buf); key_buf = NULL; }
                } else {
                    /* Unknown to typed schema (e.g. legacy untyped object) —
                       fall back to raw bytes so index still builds. */
                    size_t sl = strlen(txt);
                    key_buf = malloc(sl);
                    memcpy(key_buf, txt, sl);
                    key_len = sl;
                }
            }
        }

        if (!key_buf || key_len == 0) { free(key_buf); continue; }

        idx_keys[tcount] = key_buf;
        args[tcount].db_root = db_root;
        args[tcount].object = object;
        args[tcount].field = fields[i];
        args[tcount].splits = splits;
        args[tcount].val = key_buf;
        args[tcount].vlen = key_len;
        args[tcount].hash16 = hash16;
        tcount++;
    }

    parallel_for(index_thread_fn, args, tcount, sizeof(IndexThreadArg));

    for (int i = 0; i < tcount; i++) free(idx_keys[i]);
    for (int i = 0; i < unique_count; i++) free(extracted[i]);
    for (int i = 0; i < unique_count; i++) {
        int is_field = 0;
        for (int j = 0; j < nfields; j++)
            if (unique_keys[i] == fields[j]) { is_field = 1; break; }
        if (!is_field) free((char *)unique_keys[i]);
    }
}

/* ========== In-process sort+dedup for index files ========== */

int cmp_str(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

int cmp_str_numeric(const void *a, const void *b) {
    double va = atof(*(const char **)a);
    double vb = atof(*(const char **)b);
    return (va > vb) - (va < vb);
}

/* Build an index key from a typed record for a (possibly composite) spec.
   See types.h for contract. */
/* Write the index key for `spec` into caller buffer `out` of capacity
   `out_cap`. Returns 1 on a written key, 0 on empty/missing field, -1
   if the key wouldn't fit in `out_cap`. No allocations on the success
   path — the per-call alloc/free pair the malloc'd variant pays is
   what makes this useful for hot pre_commit hooks. */
int build_index_key_from_record_into(const TypedSchema *ts, const uint8_t *record,
                                      const char *spec,
                                      uint8_t *out, size_t out_cap,
                                      size_t *out_len) {
    if (!ts || !record || !spec || !out || !out_len) return 0;
    *out_len = 0;

    if (strchr(spec, '+')) {
        char fb[256]; strncpy(fb, spec, 255); fb[255] = '\0';
        size_t cp = 0;
        char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
        while (tok) {
            int fi = typed_field_index(ts, tok);
            if (fi < 0) return 0;
            char *v = typed_get_field_str(ts, record, fi);
            if (!v) return 0;
            size_t sl = strlen(v);
            if (cp + sl > out_cap) { free(v); return -1; }
            memcpy(out + cp, v, sl); cp += sl;
            free(v);
            tok = strtok_r(NULL, "+", &_tok_save);
        }
        if (cp == 0) return 0;
        *out_len = cp;
        return 1;
    }

    int fi = typed_field_index(ts, spec);
    if (fi < 0) return 0;
    const TypedField *f = &ts->fields[fi];
    size_t cap = (size_t)(f->size > 8 ? f->size : 8);
    if (cap > out_cap) return -1;
    size_t blen = 0;
    typed_field_to_index_key(ts, record, fi, out, &blen);
    if (blen == 0) return 0;
    *out_len = blen;
    return 1;
}

int build_index_key_from_record(const TypedSchema *ts, const uint8_t *record,
                                const char *spec,
                                uint8_t **out_val, size_t *out_len) {
    if (!ts || !record || !spec || !out_val || !out_len) return 0;
    *out_val = NULL;
    *out_len = 0;

    if (strchr(spec, '+')) {
        /* Composite — ASCII concat per field, stays on the string path. */
        char fb[256]; strncpy(fb, spec, 255); fb[255] = '\0';
        char cat[4096]; int cp = 0; int ok = 1;
        char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
        while (tok) {
            int fi = typed_field_index(ts, tok);
            if (fi < 0) { ok = 0; break; }
            char *v = typed_get_field_str(ts, record, fi);
            if (!v) { ok = 0; break; }
            int sl = strlen(v);
            if (cp + sl < (int)sizeof(cat)) { memcpy(cat + cp, v, sl); cp += sl; }
            free(v);
            tok = strtok_r(NULL, "+", &_tok_save);
        }
        if (!ok || cp == 0) return 0;
        *out_val = malloc((size_t)cp);
        memcpy(*out_val, cat, (size_t)cp);
        *out_len = (size_t)cp;
        return 1;
    }

    /* Single field — typed binary → index-key bytes. */
    int fi = typed_field_index(ts, spec);
    if (fi < 0) return 0;
    const TypedField *f = &ts->fields[fi];
    size_t cap = (size_t)(f->size > 8 ? f->size : 8);
    uint8_t *buf = malloc(cap);
    size_t blen = 0;
    typed_field_to_index_key(ts, record, fi, buf, &blen);
    if (blen == 0) { free(buf); return 0; }
    *out_val = buf;
    *out_len = blen;
    return 1;
}

/* Build an index key from JSON for a (possibly composite) spec.
   See types.h for contract. */
int build_index_key_from_json(const TypedSchema *ts, const char *json,
                              const char *spec,
                              uint8_t **out_val, size_t *out_len) {
    if (!json || !spec || !out_val || !out_len) return 0;
    *out_val = NULL;
    *out_len = 0;

    if (strchr(spec, '+')) {
        /* Composite — extract each sub-field and ASCII-concat. */
        char fb[256]; strncpy(fb, spec, 255); fb[255] = '\0';
        const char *subs[16]; int nsub = 0;
        char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
        while (tok && nsub < 16) { subs[nsub++] = tok; tok = strtok_r(NULL, "+", &_tok_save); }
        char *vals[16];
        json_get_fields(json, subs, nsub, vals);
        char cat[4096]; int cp = 0; int ok = 1;
        for (int i = 0; i < nsub; i++) {
            if (!vals[i] || vals[i][0] == '\0') { ok = 0; break; }
            int sl = strlen(vals[i]);
            if (cp + sl < (int)sizeof(cat)) { memcpy(cat + cp, vals[i], sl); cp += sl; }
        }
        for (int i = 0; i < nsub; i++) free(vals[i]);
        if (!ok || cp == 0) return 0;
        *out_val = malloc((size_t)cp);
        memcpy(*out_val, cat, (size_t)cp);
        *out_len = (size_t)cp;
        return 1;
    }

    /* Single field — extract text, encode to index bytes. */
    JsonObj jo;
    json_parse_object(json, strlen(json), &jo);
    char *txt = json_obj_strdup(&jo, spec);
    if (!txt || !txt[0]) { free(txt); return 0; }

    int fi = ts ? typed_field_index(ts, spec) : -1;
    if (fi >= 0) {
        const TypedField *f = &ts->fields[fi];
        size_t cap = (size_t)(f->size > 8 ? f->size : 8);
        uint8_t *buf = malloc(cap);
        size_t blen = 0;
        encode_field_for_index(f, txt, strlen(txt), buf, &blen);
        free(txt);
        if (blen == 0) { free(buf); return 0; }
        *out_val = buf;
        *out_len = blen;
        return 1;
    }

    /* Untyped — passthrough raw bytes. txt was just malloc'd by
       json_obj_strdup; hand ownership to the caller instead of
       malloc+memcpy+free. The output contract is (bytes, length),
       not a C-string, so the missing null terminator is intentional. */
    *out_len = strlen(txt);
    *out_val = (uint8_t *)txt;
    return 1;
}

/* Comparators for raw structs (used by add-index sort). Length-aware so
   binary keys with embedded NULs compare correctly. */
int cmp_btentry_fn(const void *a, const void *b) {
    const BtEntry *ea = a, *eb = b;
    size_t m = ea->vlen < eb->vlen ? ea->vlen : eb->vlen;
    int r = memcmp(ea->value, eb->value, m);
    if (r) return r;
    if (ea->vlen < eb->vlen) return -1;
    if (ea->vlen > eb->vlen) return 1;
    return memcmp(ea->hash, eb->hash, BT_HASH_SIZE);
}
int cmp_str_raw(const void *a, const void *b) {
    return strcmp((const char *)a, (const char *)b);
}
int cmp_str_numeric_raw(const void *a, const void *b) {
    double va = atof((const char *)a);
    double vb = atof((const char *)b);
    return (va > vb) - (va < vb);
}

/* Read file lines, sort, dedup, write back. Replaces system("sort -u"). */
void sort_dedup_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return;

    /* Read all lines */
    size_t cap = 1024, count = 0;
    char **lines = malloc(cap * sizeof(char *));
    char buf[MAX_LINE];
    while (fgets(buf, sizeof(buf), f)) {
        buf[strcspn(buf, "\n")] = '\0';
        if (buf[0] == '\0') continue;
        if (count >= cap) {
            cap *= 2;
            char **t = xrealloc_or_free(lines, cap * sizeof(char *));
            if (!t) { lines = NULL; break; }
            lines = t;
        }
        lines[count++] = strdup(buf);
    }
    fclose(f);

    if (!lines || count == 0) { free(lines); return; }

    /* Sort */
    qsort(lines, count, sizeof(char *), cmp_str);

    /* Write back, skipping duplicates */
    f = fopen(path, "w");
    if (f) {
        fprintf(f, "%s\n", lines[0]);
        for (size_t i = 1; i < count; i++) {
            if (strcmp(lines[i], lines[i-1]) != 0)
                fprintf(f, "%s\n", lines[i]);
        }
        fclose(f);
    }

    for (size_t i = 0; i < count; i++) free(lines[i]);
    free(lines);
}

/* Recursively remove a directory (replaces system("rm -rf")).
   Uses fstatat / unlinkat against the open dirfd so the type check and
   the unlink target the same inode — CodeQL "time-of-check time-of-use
   filesystem race" pattern, even though we own the directory we're
   walking. */
void rmrf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { unlink(path); return; }
    int dfd = dirfd(d);
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.' && (e->d_name[1] == '\0' ||
            (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
        struct stat st;
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) continue;
        if (S_ISDIR(st.st_mode)) {
            char child[PATH_MAX];
            snprintf(child, sizeof(child), "%s/%s", path, e->d_name);
            rmrf(child);
        } else {
            unlinkat(dfd, e->d_name, 0);
        }
    }
    closedir(d);
    rmdir(path);
}

/* ========== ADD-INDEX ========== */

/* Context for parallel index scan */
typedef struct {
    const char *field;
    TypedSchema *ts;
    BtEntry *pairs;
    size_t pair_count;
    size_t pair_cap;
    int is_composite;
    int field_indices[16];
    int field_index_count;
    pthread_mutex_t lock;  /* protects pairs array append only */
} IndexScanCtx;

static int index_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    IndexScanCtx *ic = (IndexScanCtx *)ctx;
    const char *raw = (const char *)(block + hdr->key_len);
    uint8_t *key_buf = NULL;
    size_t key_len = 0;

    /* Key encoding is thread-local — no lock. */
    if (ic->is_composite) {
        char cat[4096]; int cpos = 0; int ok = 1;
        for (int i = 0; i < ic->field_index_count; i++) {
            char *v = typed_get_field_str(ic->ts, (const uint8_t *)raw, ic->field_indices[i]);
            if (v) { int sl = strlen(v); memcpy(cat + cpos, v, sl); cpos += sl; free(v); }
            else { ok = 0; break; }
        }
        if (ok && cpos > 0) {
            key_buf = malloc((size_t)cpos);
            memcpy(key_buf, cat, (size_t)cpos);
            key_len = (size_t)cpos;
        }
    } else {
        int fidx = ic->field_indices[0];
        if (fidx >= 0) {
            const TypedField *f = &ic->ts->fields[fidx];
            /* Allocate exactly what the index key needs. For varchar peek
               the length prefix so a varchar:500 with "hi" reserves 2 bytes
               not 500 — at 25M records the over-allocation is gigabytes. */
            size_t cap;
            if (f->type == FT_VARCHAR) {
                const uint8_t *src = (const uint8_t *)raw + f->offset;
                int content_max = f->size - 2;
                if (content_max < 0) content_max = 0;
                int len = ((int)src[0] << 8) | (int)src[1];
                if (len < 0) len = 0;
                if (len > content_max) len = content_max;
                cap = (size_t)len;
            } else {
                cap = (size_t)f->size;
                if (cap == 0) cap = 8;
            }
            if (cap > 0) {
                key_buf = malloc(cap);
                typed_field_to_index_key(ic->ts, (const uint8_t *)raw, fidx, key_buf, &key_len);
                if (key_len == 0) { free(key_buf); key_buf = NULL; }
            }
        }
    }

    if (key_buf && key_len > 0) {
        pthread_mutex_lock(&ic->lock);
        if (ic->pair_count >= ic->pair_cap) {
            size_t new_cap = ic->pair_cap * 2;
            BtEntry *t = xrealloc_or_free(ic->pairs, new_cap * sizeof(BtEntry));
            if (!t) {
                ic->pairs = NULL;
                ic->pair_count = 0;
                ic->pair_cap = 0;
                pthread_mutex_unlock(&ic->lock);
                free(key_buf);
                return 0;
            }
            ic->pairs = t;
            ic->pair_cap = new_cap;
        }
        ic->pairs[ic->pair_count].value = (const char *)key_buf;
        ic->pairs[ic->pair_count].vlen = key_len;
        memcpy(ic->pairs[ic->pair_count].hash, hdr->hash, 16);
        ic->pair_count++;
        pthread_mutex_unlock(&ic->lock);
    } else {
        free(key_buf);
    }
    return 0;
}

/* Per-field-shard build worker — qsorts its slice and bulk-builds one shard. */
typedef struct {
    char  ipath[PATH_MAX];
    BtEntry *pairs;     /* slice — does NOT own backing memory; freed by caller */
    size_t  pair_count;
} ShardBuildArg;

static void *shard_build_worker(void *arg) {
    ShardBuildArg *sb = (ShardBuildArg *)arg;
    qsort(sb->pairs, sb->pair_count, sizeof(BtEntry), cmp_btentry_fn);
    btree_bulk_build(sb->ipath, sb->pairs, sb->pair_count);
    return NULL;
}

/* Per-shard incremental merge worker — same shape as ShardBuildArg but
   uses btree_bulk_merge so successive batches accumulate into the same
   .idx/.tg file without destroying earlier batches' entries. Used by
   the trigram build pipeline, which batches kf shards to keep peak
   memory bounded by g_index_build_budget_bytes. */
typedef struct {
    char     ipath[PATH_MAX];
    BtEntry *pairs;        /* slice — caller owns backing memory */
    size_t   pair_count;
} ShardMergeArg;

static void *shard_merge_worker(void *arg) {
    ShardMergeArg *sm = (ShardMergeArg *)arg;
    /* btree_bulk_merge sorts new_entries internally; no qsort needed
       here. It also handles the empty-existing case as a clean build. */
    btree_bulk_merge(sm->ipath, sm->pairs, sm->pair_count);
    return NULL;
}

/* Bucket-sort `pairs` (of total `count`) into `nshards` partitions by
   idx_shard_for_hash(pair.hash, splits). Returns a malloc'd contiguous
   BtEntry array of length `count` (caller frees) plus per-shard offset/length
   arrays (out_offsets[i] and out_counts[i]). The original `pairs` array is
   consumed (no copies of the variable-length value strings — pointers are
   moved). */
static BtEntry *partition_by_shard(BtEntry *pairs, size_t count, int splits,
                                   int nshards,
                                   size_t **out_offsets, size_t **out_counts) {
    size_t *counts = calloc((size_t)nshards, sizeof(size_t));
    size_t *offsets = calloc((size_t)nshards, sizeof(size_t));
    BtEntry *out = malloc(count * sizeof(BtEntry));
    if (!counts || !offsets || !out) {
        free(counts); free(offsets); free(out);
        *out_offsets = NULL; *out_counts = NULL;
        return NULL;
    }
    /* First pass: tally per-shard sizes. */
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        counts[s]++;
    }
    /* Compute prefix-sum offsets. */
    size_t acc = 0;
    for (int s = 0; s < nshards; s++) { offsets[s] = acc; acc += counts[s]; }
    /* Second pass: scatter into out[] using a per-shard write cursor. */
    size_t *cursor = calloc((size_t)nshards, sizeof(size_t));
    if (!cursor) { free(counts); free(offsets); free(out); return NULL; }
    for (size_t i = 0; i < count; i++) {
        int s = idx_shard_for_hash(pairs[i].hash, splits);
        out[offsets[s] + cursor[s]++] = pairs[i];
    }
    free(cursor);
    *out_offsets = offsets;
    *out_counts = counts;
    return out;
}

/* Forward decls — full definitions live near the multi-index builder. */
int build_bitmap_pass(const char *db_root, const char *object,
                      const Schema *sch, TypedSchema *ts,
                      const char *field, uint32_t max_values, int force);
int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);

int cmd_add_index(const char *db_root, const char *object,
                         const char *field, int force) {
    /* Parse the spec via the canonical helper so `name`, `name:btree`,
       `name:bitmap[(N)]`, `name:trigram`, and composite `a+b` all funnel
       through one grammar. Auto-promote bare bool/enum names to bitmap
       (same rule as create-object + cmd_add_indexes). */
    ParsedIndexSpec ps;
    enum IndexType type = IT_BTREE;
    uint32_t max_values = 0;
    const char *eff = field;
    if (parse_index_spec(field, &ps) == 0) {
        type = ps.type;
        max_values = ps.max_values;
        eff = ps.name;
        if (!ps.is_composite) {
            TypedSchema *ts_chk = load_typed_schema(db_root, object);
            if (ts_chk) {
                int fi = typed_field_index(ts_chk, ps.name);
                if (fi >= 0 && idx_should_auto_bitmap(ps.had_explicit_type,
                                                     ts_chk->fields[fi].type)) {
                    type = IT_BITMAP;
                    if (ts_chk->fields[fi].type == FT_ENUM &&
                        ts_chk->fields[fi].enum_width == 2 && max_values == 0)
                        max_values = 65535;
                }
            }
        }
    }

    /* Compose the canonical index.conf line for dedupe + write. */
    char canon[300];
    if (type == IT_BITMAP) {
        if (max_values > 0)
            snprintf(canon, sizeof(canon), "%s:bitmap(%u)", eff, max_values);
        else
            snprintf(canon, sizeof(canon), "%s:bitmap", eff);
    } else if (type == IT_TRIGRAM) {
        snprintf(canon, sizeof(canon), "%s:trigram", eff);
    } else {
        snprintf(canon, sizeof(canon), "%s", eff);
    }

    Schema sch = load_schema(db_root, object);
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* Skip-if-exists: bitmap/trigram probe shard-0 of their respective
       on-disk file (matches cmd_add_indexes); btree walks index.conf. */
    if (!force) {
        if (type == IT_BITMAP || type == IT_TRIGRAM) {
            char probe[PATH_MAX];
            struct stat st;
            if (type == IT_BITMAP)
                bm_build_path(probe, sizeof(probe), db_root, object, eff, 0);
            else
                tg_build_path(probe, sizeof(probe), db_root, object, eff, 0);
            if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) {
                OUT("{\"status\":\"exists\",\"field\":\"%s\"}\n", field);
                return 0;
            }
        } else {
            FILE *cf = fopen(conf_path, "r");
            if (cf) {
                char line[256];
                while (fgets(line, sizeof(line), cf)) {
                    line[strcspn(line, "\n")] = '\0';
                    if (strcmp(line, canon) == 0) {
                        OUT("{\"status\":\"exists\",\"field\":\"%s\"}\n", field);
                        fclose(cf); return 0;
                    }
                }
                fclose(cf);
            }
        }
    }

    TypedSchema *ts = load_typed_schema(db_root, object);

    if (type == IT_BITMAP) {
        build_bitmap_pass(db_root, object, &sch, ts, eff, max_values, force);
    } else if (type == IT_TRIGRAM) {
        build_trigram_pass(db_root, object, &sch, ts, eff, force);
    } else {
        /* === btree build (legacy path, now using parsed name) === */
        if (force) btree_idx_unlink_all(db_root, object, eff, sch.splits);
        int idx_n = index_splits_for(sch.splits);

        IndexScanCtx ic;
        memset(&ic, 0, sizeof(ic));
        ic.field = eff;
        ic.ts = ts;
        /* Pre-size from live_count to skip exponential doubling on big objects.
           4096 floor for small / empty objects; 1 Gi cap so a corrupted count
           can't request an absurd allocation. Scan cb's xrealloc fallback still
           handles concurrent inserts that push past the estimate. */
        {
            int live = get_live_count(db_root, object);
            if (live < 0) live = 0;
            size_t initial = (size_t)live + 4096;
            if (initial > (1ULL << 30)) initial = (1ULL << 30);
            ic.pair_cap = initial;
            ic.pairs = malloc(ic.pair_cap * sizeof(BtEntry));
            if (!ic.pairs) {
                ic.pair_cap = 4096;
                ic.pairs = malloc(ic.pair_cap * sizeof(BtEntry));
            }
        }
        ic.is_composite = (strchr(eff, '+') != NULL);
        pthread_mutex_init(&ic.lock, NULL);

        if (ic.is_composite) {
            char fbuf[256]; strncpy(fbuf, eff, 255); fbuf[255] = '\0';
            char *_t_save = NULL; char *t = strtok_r(fbuf, "+", &_t_save);
            while (t && ic.field_index_count < 16) {
                ic.field_indices[ic.field_index_count++] = typed_field_index(ts, t);
                t = strtok_r(NULL, "+", &_t_save);
            }
        } else {
            ic.field_indices[0] = typed_field_index(ts, eff);
            ic.field_index_count = 1;
        }

        /* Parallel shard scan — collects all (value, hash) pairs.
           scan_dispatch routes v1 vs v2 (Phase 3B). */
        char data_dir[PATH_MAX];
        snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
        scan_dispatch(db_root, object, &sch, data_dir, index_scan_cb, &ic);

        /* Partition by idx_shard, then sort+build per shard in parallel. */
        if (ic.pair_count > 0) {
            size_t *offsets = NULL, *counts = NULL;
            BtEntry *parted = partition_by_shard(ic.pairs, ic.pair_count,
                                                 sch.splits, idx_n,
                                                 &offsets, &counts);
            if (parted) {
                ShardBuildArg *sb = malloc((size_t)idx_n * sizeof(ShardBuildArg));
                int sb_count = 0;
                for (int s = 0; s < idx_n; s++) {
                    if (counts[s] == 0) continue;
                    build_idx_path(sb[sb_count].ipath, sizeof(sb[sb_count].ipath),
                                   db_root, object, eff, s);
                    sb[sb_count].pairs = parted + offsets[s];
                    sb[sb_count].pair_count = counts[s];
                    sb_count++;
                }
                parallel_for(shard_build_worker, sb, sb_count, sizeof(ShardBuildArg));
                free(sb);
                free(parted);
                free(offsets);
                free(counts);
            }
        }

        for (size_t i = 0; i < ic.pair_count; i++) free((char *)ic.pairs[i].value);
        free(ic.pairs);
        pthread_mutex_destroy(&ic.lock);
    }

    /* Add canonical line to index.conf (idempotent). */
    mkdirp(dirname_of(conf_path));
    int already = 0;
    FILE *cf = fopen(conf_path, "r");
    if (cf) {
        char line[256];
        while (fgets(line, sizeof(line), cf)) {
            line[strcspn(line, "\n")] = '\0';
            if (strcmp(line, canon) == 0) { already = 1; break; }
        }
        fclose(cf);
    }
    if (!already) {
        FILE *af = fopen(conf_path, "a");
        if (af) { fprintf(af, "%s\n", canon); fclose(af); }
    }

    invalidate_idx_cache(object);
    OUT("{\"status\":\"indexed\",\"field\":\"%s\"}\n", field);
    return 0;
}

/* ========== Multi-index build: single shard scan, all fields at once ========== */

typedef struct {
    int nfields;
    char fields[MAX_FIELDS][256];
    TypedSchema *ts;
    /* Per-field: pre-resolved indices + collectors */
    int is_composite[MAX_FIELDS];
    int field_indices[MAX_FIELDS][16];
    int field_index_count[MAX_FIELDS];
    BtEntry *pairs[MAX_FIELDS];
    size_t pair_count[MAX_FIELDS];
    size_t pair_cap[MAX_FIELDS];
    /* Per-field mutex: the pairs arrays grow independently, so serializing
       each separately lets different fields' appends happen in parallel. */
    pthread_mutex_t lock[MAX_FIELDS];
} MultiIndexCtx;

static int multi_index_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    MultiIndexCtx *mc = (MultiIndexCtx *)ctx;
    const char *raw = (const char *)(block + hdr->key_len);

    for (int fi = 0; fi < mc->nfields; fi++) {
        uint8_t *key_buf = NULL;
        size_t key_len = 0;

        /* Key encoding is thread-local. */
        if (mc->is_composite[fi]) {
            char cat[4096]; int cpos = 0; int ok = 1;
            for (int si = 0; si < mc->field_index_count[fi]; si++) {
                char *v = typed_get_field_str(mc->ts, (const uint8_t *)raw, mc->field_indices[fi][si]);
                if (v) { int sl = strlen(v); memcpy(cat + cpos, v, sl); cpos += sl; free(v); }
                else { ok = 0; break; }
            }
            if (ok && cpos > 0) {
                key_buf = malloc((size_t)cpos);
                memcpy(key_buf, cat, (size_t)cpos);
                key_len = (size_t)cpos;
            }
        } else {
            int fidx = mc->field_indices[fi][0];
            if (fidx >= 0) {
                const TypedField *f = &mc->ts->fields[fidx];
                /* Allocate exactly what the index key needs. See index_scan_cb
                   for the rationale — varchar over-allocation dominates peak
                   memory at scale (×nfields here). */
                size_t cap;
                if (f->type == FT_VARCHAR) {
                    const uint8_t *src = (const uint8_t *)raw + f->offset;
                    int content_max = f->size - 2;
                    if (content_max < 0) content_max = 0;
                    int len = ((int)src[0] << 8) | (int)src[1];
                    if (len < 0) len = 0;
                    if (len > content_max) len = content_max;
                    cap = (size_t)len;
                } else {
                    cap = (size_t)f->size;
                    if (cap == 0) cap = 8;
                }
                if (cap > 0) {
                    key_buf = malloc(cap);
                    typed_field_to_index_key(mc->ts, (const uint8_t *)raw, fidx, key_buf, &key_len);
                    if (key_len == 0) { free(key_buf); key_buf = NULL; }
                }
            }
        }

        if (key_buf && key_len > 0) {
            pthread_mutex_lock(&mc->lock[fi]);
            if (mc->pair_count[fi] >= mc->pair_cap[fi]) {
                size_t new_cap = mc->pair_cap[fi] * 2;
                BtEntry *t = xrealloc_or_free(mc->pairs[fi], new_cap * sizeof(BtEntry));
                if (!t) {
                    mc->pairs[fi] = NULL;
                    mc->pair_count[fi] = 0;
                    mc->pair_cap[fi] = 0;
                    pthread_mutex_unlock(&mc->lock[fi]);
                    free(key_buf);
                    continue;
                }
                mc->pairs[fi] = t;
                mc->pair_cap[fi] = new_cap;
            }
            mc->pairs[fi][mc->pair_count[fi]].value = (const char *)key_buf;
            mc->pairs[fi][mc->pair_count[fi]].vlen = key_len;
            memcpy(mc->pairs[fi][mc->pair_count[fi]].hash, hdr->hash, 16);
            mc->pair_count[fi]++;
            pthread_mutex_unlock(&mc->lock[fi]);
        } else {
            free(key_buf);
        }
    }
    return 0;
}

/* Average ASCII width of typed_get_field_str(f) — used for composite keys,
   which are built as ASCII concatenation of each child field's stringified
   value (see multi_index_scan_cb's composite branch). Numbers come from
   typed_get_field_str in config.c (FT_DATE → "%08d", FT_DATETIME → 14 chars,
   FT_BOOL → "true"/"false", others via decode_field_to_buf). varchars use
   50% fill of f->size-2, same as the single-field estimator. */
static size_t typed_field_str_avg(const TypedField *f) {
    switch (f->type) {
    case FT_NONE:     return 16;  /* unassigned — conservative fallback */
    case FT_VARCHAR: {
        size_t content_max = (size_t)f->size > 2 ? (size_t)f->size - 2 : 0;
        size_t avg = content_max / 2;
        return avg < 1 ? 1 : avg;
    }
    case FT_BOOL:     return 5;   /* "true" / "false" */
    case FT_BYTE:     return 3;   /* up to "255" */
    case FT_SHORT:    return 6;   /* up to "-32768" */
    case FT_INT:      return 11;  /* up to "-2147483648" */
    case FT_LONG:     return 20;  /* up to "-9223372036854775808" */
    case FT_DOUBLE:   return 15;  /* typical %g */
    case FT_FLOAT:   return 12;  /* typical %g */
    case FT_NUMERIC:  return 16;  /* sign + 12 digits + dot + scale */
    case FT_DATE:     return 8;   /* YYYYMMDD via %08d */
    case FT_DATETIME: return 14;  /* YYYYMMDDHHmmss */
    case FT_TIME:     return 8;   /* HH:MM:SS */
    case FT_TIMESTAMP: return 20; /* Unix epoch ms, up to 19 digits + sign */
    case FT_UUID:     return 36;  /* xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx */
    case FT_ENUM: {
        /* Mean length of the declared value strings; pre-declared at
           create-object, so the estimate is exact (not heuristic). */
        if (!f->enum_values || f->n_enum_values <= 0) return 8;
        size_t sum = 0;
        for (int i = 0; i < f->n_enum_values; i++)
            sum += f->enum_values[i] ? strlen(f->enum_values[i]) : 0;
        size_t avg = sum / (size_t)f->n_enum_values;
        return avg < 1 ? 1 : avg;
    }
    }
    return 16;
}

/* Estimate the peak per-field memory cost of a single batch pass in bytes.
   The build pipeline keeps three things alive per field while building:
     - pairs[]: BtEntry array, 32 B per live record
     - parted_per_field[]: partition copy of the BtEntry array (also 32 B/rec)
     - key value buffers: one malloc per record sized to the encoded key
   This estimate is conservative — better to overshoot and run more (smaller)
   batches than to undershoot and OOM. The doubling fallback in the scan cb
   handles concurrent inserts that push live_count over the estimate. */
static size_t estimate_field_build_bytes(const TypedSchema *ts,
                                         const char *field, size_t live_count) {
    size_t key_avg = 16;

    if (strchr(field, '+')) {
        /* Composite key — sum each child field's ASCII width. Composite keys
           are built by concatenating typed_get_field_str(child) per child, so
           the right estimate is the sum of typed_field_str_avg over children,
           NOT a flat constant. status+invoiceDate is ~18 B, not 64. */
        char fb[256]; strncpy(fb, field, 255); fb[255] = '\0';
        size_t sum = 0;
        char *save = NULL;
        char *tok = strtok_r(fb, "+", &save);
        while (tok) {
            int fidx = typed_field_index(ts, tok);
            if (fidx >= 0) sum += typed_field_str_avg(&ts->fields[fidx]);
            else sum += 16;  /* unknown child — conservative fallback */
            tok = strtok_r(NULL, "+", &save);
        }
        if (sum < 8) sum = 8;
        key_avg = sum;
    } else {
        int fidx = typed_field_index(ts, field);
        if (fidx >= 0) {
            const TypedField *f = &ts->fields[fidx];
            if (f->type == FT_VARCHAR) {
                /* varchar:N stores [u16 len][content], max content = size-2.
                   Assume 50% fill on average; floor at 8 B for glibc small-bin
                   overhead so we don't undershoot on near-empty strings. */
                size_t content_max = (size_t)f->size > 2 ? (size_t)f->size - 2 : 0;
                key_avg = content_max / 2;
                if (key_avg < 8) key_avg = 8;
            } else {
                /* Fixed-width types: typed_field_to_index_key writes exactly
                   f->size bytes (binary, total-order encoded). */
                key_avg = (size_t)f->size;
                if (key_avg < 8) key_avg = 8;
            }
        }
    }
    /* +24 B for glibc per-allocation overhead (chunk header). */
    size_t per_record = 32 + 32 + key_avg + 24;
    return per_record * (live_count == 0 ? 1 : live_count);
}

/* Bitmap reindex pass — rebuilds every .bm shard for one field by
   walking live records in their kf shards via slotcask_walk_one_shard_slots,
   encoding the field value (matching the encoding bitmap_update uses on
   the CRUD path), and bm_set'ing the bit at the record's kf slot. */
typedef struct {
    BitmapShard *bm;
    int          field_index;     /* typed schema field index */
    TypedSchema *ts;
} BmRebuildCtx;

static int bm_rebuild_cb(uint32_t slot, const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    BmRebuildCtx *c = (BmRebuildCtx *)ctx;
    if (c->field_index < 0) return 0;
    const TypedField *f = &c->ts->fields[c->field_index];
    /* Pull the raw field bytes out of the typed-record value. */
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
    /* Encode via the same path bitmap_update uses on insert. For fixed
       types the raw stored bytes are already the index-key form;
       varchar carries a 2-byte length prefix in storage that we need
       to strip. */
    uint8_t key_buf[1024];
    size_t  key_len = 0;
    if (f->type == FT_VARCHAR) {
        uint16_t actual_len = (uint16_t)vbase[0] | ((uint16_t)vbase[1] << 8);
        if (actual_len == 0 || actual_len > sizeof(key_buf)) return 0;
        memcpy(key_buf, vbase + 2, actual_len);
        key_len = actual_len;
    } else {
        size_t sz = (size_t)f->size;
        if (sz == 0 || sz > sizeof(key_buf)) return 0;
        memcpy(key_buf, vbase, sz);
        key_len = sz;
    }
    bm_set(c->bm, key_buf, key_len, slot);
    return 0;
}

/* Trigram reindex — walks every record of the object, extracts
   distinct trigrams per varchar field, and writes one (trigram, hash)
   leaf entry per (trigram, record). Mirrors btree's `cmd_add_index`
   pipeline: parallel per-kf-shard walk → contiguous concat →
   partition by `idx_shard_for_hash` → parallel qsort + btree_bulk_build
   per .tg shard. Each .tg file shares the BTRH format with .idx, so
   shard_build_worker is reused unchanged.

   Per-kf-shard parallel build worker. Each worker walks ONE kf shard,
   extracts distinct trigrams per record, and appends (trigram, hash)
   pairs to its own buffer + arena (lock-free). After all workers finish,
   the main thread fixes up the BtEntry.value pointers (which carried
   into-arena offsets to survive realloc), concatenates the per-worker
   pair arrays, partitions by .tg shard, then runs shard_build_worker
   in parallel — exactly the pipeline cmd_add_index uses for btree. */
typedef struct {
    SlotcaskDb  *sdb;
    int          kf_shard;          /* which kf shard to walk */
    int          field_index;       /* typed schema field index */
    TypedSchema *ts;

    BtEntry     *pairs;
    size_t       pair_cap;
    size_t       pair_count;

    uint8_t     *arena;             /* grows during walk; pairs[i].value
                                       carries an offset into this arena
                                       (cast to char*) until the
                                       post-walk fix-up step. */
    size_t       arena_cap;
    size_t       arena_used;
} TgWorkerCtx;

static int tg_build_per_record_cb(uint32_t slot, const uint8_t hash16[16],
                                  const void *key, size_t klen,
                                  const void *value, size_t vlen,
                                  void *ctx) {
    (void)slot; (void)key; (void)klen; (void)vlen;
    TgWorkerCtx *w = (TgWorkerCtx *)ctx;
    const TypedField *f = &w->ts->fields[w->field_index];
    if (f->type != FT_VARCHAR) return 0;

    const uint8_t *vbase = (const uint8_t *)value + f->offset;
    uint16_t actual_len = (uint16_t)vbase[0] | ((uint16_t)vbase[1] << 8);
    if (actual_len == 0) return 0;

    uint8_t trigrams[TG_MAX_DISTINCT][3];
    size_t n = tg_extract_distinct(vbase + 2, actual_len, trigrams, TG_MAX_DISTINCT);
    if (n == 0) return 0;

    /* Grow pairs[] geometrically. */
    if (w->pair_count + n > w->pair_cap) {
        size_t new_cap = w->pair_cap == 0 ? 4096 : w->pair_cap * 2;
        while (new_cap < w->pair_count + n) new_cap *= 2;
        BtEntry *p = xrealloc_or_free(w->pairs, new_cap * sizeof(BtEntry));
        if (!p) {
            /* xrealloc_or_free dropped the old block on failure — clear
               the now-dangling fields so subsequent callbacks no-op
               instead of double-freeing. */
            w->pairs = NULL; w->pair_cap = 0; w->pair_count = 0;
            return 0;
        }
        w->pairs = p;
        w->pair_cap = new_cap;
    }

    /* Grow arena geometrically. Pair value pointers carry the OFFSET
       into the arena (cast to char *); they get rewritten to real
       pointers after the parallel walk finishes — so any realloc move
       here is safe. */
    size_t need = n * 3;
    if (w->arena_used + need > w->arena_cap) {
        size_t new_size = w->arena_cap == 0 ? 65536 : w->arena_cap * 2;
        while (new_size < w->arena_used + need) new_size *= 2;
        uint8_t *a = xrealloc_or_free(w->arena, new_size);
        if (!a) {
            w->arena = NULL; w->arena_cap = 0; w->arena_used = 0;
            /* pairs we already recorded carry stale offsets into a
               freed arena — abandon them too to keep the fix-up step
               safe. */
            free(w->pairs);
            w->pairs = NULL; w->pair_cap = 0; w->pair_count = 0;
            return 0;
        }
        w->arena = a;
        w->arena_cap = new_size;
    }

    for (size_t i = 0; i < n; i++) {
        size_t off = w->arena_used;
        memcpy(w->arena + off, trigrams[i], 3);
        w->arena_used += 3;
        /* Store offset in .value — cast to const char* for now; the
           main thread re-bases to (arena + off) once the arena is
           guaranteed to no longer move. */
        w->pairs[w->pair_count].value = (const char *)(uintptr_t)off;
        w->pairs[w->pair_count].vlen  = 3;
        memcpy(w->pairs[w->pair_count].hash, hash16, BT_HASH_SIZE);
        w->pair_count++;
    }
    return 0;
}

static void *tg_worker_walk(void *arg) {
    TgWorkerCtx *w = (TgWorkerCtx *)arg;
    slotcask_walk_one_shard_slots(w->sdb, w->kf_shard,
                                  tg_build_per_record_cb, w);
    return NULL;
}

int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);
int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force) {
    if (!ts) return -1;
    int fi = typed_field_index(ts, field);
    if (fi < 0) return -1;

    int idx_n = index_splits_for(sch->splits);

    /* Force: unlink existing .tg shards before rebuild (matches btree's
       force semantics). bt_cache is path-keyed, so cached handles on the
       orphaned inode die on next bt_acquire via inode-mismatch reopen. */
    if (force) {
        for (int s = 0; s < idx_n; s++) {
            char tp[PATH_MAX];
            tg_build_path(tp, sizeof(tp), db_root, object, field, s);
            btree_cache_invalidate(tp);
            unlink(tp);
        }
    }

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    int n_kf = sch->splits;

    /* Adaptive batching across kf shards — mirrors the budget logic
       cmd_add_indexes uses across fields, but trigram only has one
       field so we batch the kf-shard dimension instead. At ~8 trigrams
       per record, accumulating ALL records' (trigram, hash) pairs at
       once for a 25M object would spike to 6+ GB — past any reasonable
       budget. Each batch: parallel walk → concat → partition → parallel
       qsort + btree_bulk_merge into the .tg files. */
    int live = get_live_count(db_root, object);
    if (live < 0) live = 0;
    size_t budget = g_index_build_budget_bytes;
    if (budget < 64ULL * 1024 * 1024) budget = 64ULL * 1024 * 1024;

    /* Per-kf-shard estimate: records_in_shard × TG_AVG × (BtEntry + 3-byte
       arena cell) × 2 (partition_by_shard copy). TG_AVG=16 is a generous
       upper bound for short English text; longer / denser fields will
       just produce smaller batch sizes from a higher estimate. */
    const size_t TG_AVG = 16;
    const size_t PER_ENTRY_BYTES = sizeof(BtEntry) + 3;
    size_t per_shard_records = (n_kf > 0) ? ((size_t)live + (size_t)n_kf - 1) / (size_t)n_kf : 0;
    if (per_shard_records < 1024) per_shard_records = 1024;
    size_t per_shard_bytes = per_shard_records * TG_AVG * PER_ENTRY_BYTES * 2;
    int batch_size = (per_shard_bytes > 0) ? (int)(budget / per_shard_bytes) : n_kf;
    if (batch_size < 1) batch_size = 1;
    if (batch_size > n_kf) batch_size = n_kf;

    log_msg(2, "BUILD-TRIGRAM %s/%s: %d kf shards, %d per batch (~%zu MB/batch budget)",
            db_root, object, n_kf, batch_size, budget / (1024 * 1024));

    for (int batch_start = 0; batch_start < n_kf; batch_start += batch_size) {
        int batch_end = batch_start + batch_size;
        if (batch_end > n_kf) batch_end = n_kf;
        int batch_n = batch_end - batch_start;

        TgWorkerCtx *workers = calloc((size_t)batch_n, sizeof(TgWorkerCtx));
        if (!workers) return -1;
        for (int i = 0; i < batch_n; i++) {
            workers[i].sdb         = sdb;
            workers[i].kf_shard    = batch_start + i;
            workers[i].field_index = fi;
            workers[i].ts          = ts;
        }

        /* Phase 1 — parallel walk over this batch's kf shards. */
        parallel_for(tg_worker_walk, workers, batch_n, sizeof(TgWorkerCtx));

        /* Phase 2 — rebase offset → arena pointer. */
        size_t total = 0;
        for (int i = 0; i < batch_n; i++) {
            TgWorkerCtx *w = &workers[i];
            for (size_t j = 0; j < w->pair_count; j++) {
                uintptr_t off = (uintptr_t)w->pairs[j].value;
                w->pairs[j].value = (const char *)(w->arena + off);
            }
            total += w->pair_count;
        }

        if (total == 0) {
            for (int i = 0; i < batch_n; i++) {
                free(workers[i].pairs);
                free(workers[i].arena);
            }
            free(workers);
            continue;
        }

        /* Phase 3 — concat per-worker arrays. */
        BtEntry *all = malloc(total * sizeof(BtEntry));
        if (!all) {
            for (int i = 0; i < batch_n; i++) {
                free(workers[i].pairs);
                free(workers[i].arena);
            }
            free(workers);
            return -1;
        }
        size_t off = 0;
        for (int i = 0; i < batch_n; i++) {
            memcpy(all + off, workers[i].pairs,
                   workers[i].pair_count * sizeof(BtEntry));
            off += workers[i].pair_count;
            free(workers[i].pairs);
            workers[i].pairs = NULL;
        }

        /* Phase 4 — partition by .tg shard, then parallel
           qsort + btree_bulk_merge per shard. Merge (not bulk_build)
           because subsequent batches must accumulate into the same
           .tg files; bulk_merge handles the empty-existing case as
           a clean build internally. */
        size_t *offsets = NULL, *counts = NULL;
        BtEntry *parted = partition_by_shard(all, total, sch->splits, idx_n,
                                             &offsets, &counts);
        free(all);

        if (parted) {
            ShardMergeArg *sm = malloc((size_t)idx_n * sizeof(ShardMergeArg));
            int sm_count = 0;
            for (int s = 0; s < idx_n; s++) {
                if (counts[s] == 0) continue;
                tg_build_path(sm[sm_count].ipath, sizeof(sm[sm_count].ipath),
                              db_root, object, field, s);
                sm[sm_count].pairs = parted + offsets[s];
                sm[sm_count].pair_count = counts[s];
                sm_count++;
            }
            parallel_for(shard_merge_worker, sm, sm_count, sizeof(ShardMergeArg));
            free(sm);
            free(parted);
            free(offsets);
            free(counts);
        }

        /* Free this batch's arenas (no live readers of value pointers
           — bulk_merge already extracted what it needed). */
        for (int i = 0; i < batch_n; i++) {
            free(workers[i].arena);
        }
        free(workers);
    }

    return 0;
}

/* Per-kf-shard worker for parallel bitmap rebuild. Each worker handles
   one (kf_shard, .bm) pair — opens its own .bm writer, walks the
   matching kf shard, and bm_sets per record. Files don't overlap, so
   no locking; mmap absorbs the writes directly. */
typedef struct {
    char         path[PATH_MAX];
    int          kf_shard;
    int          slots_per_shard;
    int          fi;
    TypedSchema *ts;
    SlotcaskDb  *sdb;
} BmShardWalkArg;

static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 0, 0, 0,
                              1 /* writer: reindex bm_set's */);
    if (!bm) return NULL;
    BmRebuildCtx c = { bm, a->fi, a->ts };
    slotcask_walk_one_shard_slots(a->sdb, a->kf_shard, bm_rebuild_cb, &c);
    bm_close(bm);
    return NULL;
}

int build_bitmap_pass(const char *db_root, const char *object,
                      const Schema *sch, TypedSchema *ts,
                      const char *field, uint32_t max_values, int force) {
    (void)force;
    if (!ts) return -1;
    int fi = typed_field_index(ts, field);
    if (fi < 0) return -1;
    const TypedField *f = &ts->fields[fi];
    int bool_fastpath = (f->type == FT_BOOL);

    /* Wipe + re-create every shard's .bm file with the correct cap.
       Invalidate the global bm_cache entry for each path BEFORE the
       unlink + recreate so a stale mmap on the old inode doesn't
       linger and serve later acquires. */
    int slots_per_shard = (int)slotcask_default_slots_for_splits(sch->splits);
    for (int s = 0; s < sch->splits; s++) {
        char bp[PATH_MAX];
        bm_build_path(bp, sizeof(bp), db_root, object, field, s);
        bm_cache_invalidate(bp);
        unlink(bp);
        BitmapShard *bm = bm_open(bp, slots_per_shard, 1, bool_fastpath, max_values, 1);
        if (bm) bm_close(bm);
    }

    /* Open the slotcask db for this object and walk every kf shard. */
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) return -1;

    /* Parallel kf-shard walks: each worker opens its own .bm (paths are
       unique per kf shard), walks its assigned kf shard, and bm_sets
       directly into the mmap'd file. Zero file contention; no memory
       accumulation (mmap is the persistent store). Matches the
       phase-1-parallel shape used by btree/trigram. */
    BmShardWalkArg *args = malloc((size_t)sch->splits * sizeof(BmShardWalkArg));
    if (!args) return -1;
    for (int s = 0; s < sch->splits; s++) {
        bm_build_path(args[s].path, sizeof(args[s].path), db_root, object, field, s);
        args[s].kf_shard       = s;
        args[s].slots_per_shard= slots_per_shard;
        args[s].fi             = fi;
        args[s].ts             = ts;
        args[s].sdb            = sdb;
    }
    parallel_for(bm_shard_walk_worker, args, sch->splits, sizeof(BmShardWalkArg));
    free(args);
    return 0;
}

/* One batch of cmd_add_indexes: scan storage once, accumulate per-field
   BtEntry arrays, partition by idx_shard, parallel-build the (field, shard)
   btree files. Memory peak ≈ Σ estimate_field_build_bytes(field, live).
   Called from cmd_add_indexes per batch so we can bound that peak. */
static void build_indexes_pass(const char *db_root, const char *object,
                               const Schema *sch, TypedSchema *ts,
                               char fields[][256], int start, int n,
                               size_t live_count) {
    int idx_n = index_splits_for(sch->splits);

    MultiIndexCtx mc;
    memset(&mc, 0, sizeof(mc));
    mc.nfields = n;
    mc.ts = ts;

    /* Pre-size pair arrays from live_count + small slack for concurrent
       inserts during the scan. Eliminates exponential doubling (and its
       2× transient peak from the old buffer hanging around during realloc).
       If pre-size malloc fails, fall back to the original 4096 + doubling
       path — the scan cb's xrealloc_or_free still handles growth. */
    size_t initial = live_count + 4096;
    if (initial < 4096) initial = 4096;
    if (initial > (1ULL << 30)) initial = (1ULL << 30);  /* 1 Gi BtEntries hard cap */

    for (int fi = 0; fi < n; fi++) {
        memcpy(mc.fields[fi], fields[start + fi], 256);
        mc.is_composite[fi] = (strchr(fields[start + fi], '+') != NULL);
        mc.pair_cap[fi] = initial;
        mc.pairs[fi] = malloc(initial * sizeof(BtEntry));
        if (!mc.pairs[fi]) {
            mc.pair_cap[fi] = 4096;
            mc.pairs[fi] = malloc(mc.pair_cap[fi] * sizeof(BtEntry));
        }
        pthread_mutex_init(&mc.lock[fi], NULL);

        if (mc.is_composite[fi]) {
            char fb[256]; strncpy(fb, fields[start + fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && mc.field_index_count[fi] < 16) {
                mc.field_indices[fi][mc.field_index_count[fi]++] = typed_field_index(ts, tok);
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            mc.field_indices[fi][0] = typed_field_index(ts, fields[start + fi]);
            mc.field_index_count[fi] = 1;
        }
    }

    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    scan_dispatch(db_root, object, sch, data_dir, multi_index_scan_cb, &mc);
    for (int fi = 0; fi < n; fi++) pthread_mutex_destroy(&mc.lock[fi]);

    ShardBuildArg *sb = malloc((size_t)n * idx_n * sizeof(ShardBuildArg));
    int sb_count = 0;
    BtEntry **parted_per_field = calloc((size_t)n, sizeof(BtEntry *));
    size_t  **offsets_per_field = calloc((size_t)n, sizeof(size_t *));
    size_t  **counts_per_field  = calloc((size_t)n, sizeof(size_t *));

    for (int fi = 0; fi < n; fi++) {
        /* Skip empty / partition-failed fields; the cleanup loop below frees
           mc.pairs[fi] unconditionally, so we must NOT free it here too —
           that's a double-free that only surfaced once reindex_object ran
           on a v2 object (where the legacy v1 scan found no records and
           every field had pair_count = 0). */
        if (mc.pair_count[fi] == 0) continue;
        size_t *offsets = NULL, *counts = NULL;
        BtEntry *parted = partition_by_shard(mc.pairs[fi], mc.pair_count[fi],
                                             sch->splits, idx_n,
                                             &offsets, &counts);
        if (!parted) continue;
        parted_per_field[fi] = parted;
        offsets_per_field[fi] = offsets;
        counts_per_field[fi] = counts;
        for (int s = 0; s < idx_n; s++) {
            if (counts[s] == 0) continue;
            build_idx_path(sb[sb_count].ipath, sizeof(sb[sb_count].ipath),
                           db_root, object, mc.fields[fi], s);
            sb[sb_count].pairs = parted + offsets[s];
            sb[sb_count].pair_count = counts[s];
            sb_count++;
        }
    }

    parallel_for(shard_build_worker, sb, sb_count, sizeof(ShardBuildArg));
    free(sb);

    for (int fi = 0; fi < n; fi++) {
        for (size_t ei = 0; ei < mc.pair_count[fi]; ei++)
            free((char *)mc.pairs[fi][ei].value);
        free(mc.pairs[fi]);
        free(parted_per_field[fi]);
        free(offsets_per_field[fi]);
        free(counts_per_field[fi]);
    }
    free(parted_per_field);
    free(offsets_per_field);
    free(counts_per_field);
}

int cmd_add_indexes(const char *db_root, const char *object,
                    const char *fields_json, int force) {
    /* Parse fields array */
    char fields[MAX_FIELDS][256];
    int nfields = 0;
    const char *p = json_skip(fields_json);
    if (*p == '[') p++;
    while (*p && nfields < MAX_FIELDS) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            int flen = (int)(p - start);
            if (flen > 0 && flen < 255) {
                memcpy(fields[nfields], start, flen);
                fields[nfields][flen] = '\0';
                nfields++;
            }
            if (*p == '"') p++;
        } else p++;
    }
    if (nfields == 0) { OUT("{\"error\":\"No fields specified\"}\n"); return 1; }

    Schema sch = load_schema(db_root, object);
    TypedSchema *ts_for_idx = load_typed_schema(db_root, object);

    /* Parse + auto-promote each spec via the canonical helpers
       (config.c::parse_index_spec + idx_should_auto_bitmap). Same logic
       create-object's wire validator uses — single source of truth. */
    char       names[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    uint32_t   maxes[MAX_FIELDS];
    int        promoted = 0;
    for (int i = 0; i < nfields; i++) {
        ParsedIndexSpec ps;
        if (parse_index_spec(fields[i], &ps) != 0) {
            /* Malformed entry in index.conf — preserve as bare btree. */
            strncpy(names[i], fields[i], 255); names[i][255] = '\0';
            types[i] = IT_BTREE;
            maxes[i] = 0;
            continue;
        }
        types[i] = ps.type;
        maxes[i] = ps.max_values;
        strncpy(names[i], ps.name, 255); names[i][255] = '\0';

        /* Auto-promote bare bool/enum names to bitmap (legacy index.conf
           lines emitted before the rule existed). The rule lives in
           config.c so create-object and reindex can never drift. For
           2-byte enums, bump max_values to 65535 so the bitmap cap
           matches the enum's byte-width domain. */
        if (!ps.is_composite && ts_for_idx) {
            int fi_t = typed_field_index(ts_for_idx, ps.name);
            if (fi_t >= 0 &&
                idx_should_auto_bitmap(ps.had_explicit_type,
                                       ts_for_idx->fields[fi_t].type)) {
                types[i] = IT_BITMAP;
                if (ts_for_idx->fields[fi_t].type == FT_ENUM &&
                    ts_for_idx->fields[fi_t].enum_width == 2 &&
                    maxes[i] == 0) {
                    maxes[i] = 65535;
                }
                promoted++;
            }
        }
    }

    /* Forward-declared further down (definition lives near the build
       workers). Rebuilds every shard's .bm for a single field. */
    int build_bitmap_pass(const char *db_root, const char *object,
                          const Schema *sch, TypedSchema *ts,
                          const char *field, uint32_t max_values, int force);

    /* Bitmap- and trigram-typed fields follow the same skip-if-exists
       semantic as btree: with force, wipe + rebuild; without force,
       no-op when any shard file already exists for the field. */
    int total_fields = nfields;  /* preserved across the btree-only reduction below */
    int btree_count = 0;
    char btree_fields[MAX_FIELDS][256];
    for (int i = 0; i < nfields; i++) {
        if (types[i] == IT_BITMAP) {
            if (!force) {
                /* Probe shard 0's .bm — if it exists, treat the field
                   as already-indexed and skip. */
                char probe[PATH_MAX];
                bm_build_path(probe, sizeof(probe), db_root, object, names[i], 0);
                struct stat st;
                if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) continue;
            }
            build_bitmap_pass(db_root, object, &sch,
                              load_typed_schema(db_root, object),
                              names[i], maxes[i], force);
            continue;
        }
        if (types[i] == IT_TRIGRAM) {
            if (!force) {
                /* Probe shard 0's .tg — same skip-if-exists rule the
                   btree and bitmap branches use. */
                char probe[PATH_MAX];
                tg_build_path(probe, sizeof(probe), db_root, object, names[i], 0);
                struct stat st;
                if (stat(probe, &st) == 0 && S_ISREG(st.st_mode)) continue;
            }
            build_trigram_pass(db_root, object, &sch,
                               load_typed_schema(db_root, object),
                               names[i], force);
            continue;
        }
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, sizeof(btree_fields));
    nfields = btree_count;

    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* === Btree batched-build path (only when btree fields remain after
       the typed-dispatch loop above). Typed builds already ran inline
       — this block handles only IT_BTREE. */
    char actual_fields[MAX_FIELDS][256];
    int actual_count = 0;
    if (nfields > 0) {
        /* Filter out already-existing btree indexes (unless force). */
        for (int i = 0; i < nfields; i++) {
            if (force) {
                btree_idx_unlink_all(db_root, object, fields[i], sch.splits);
            } else if (btree_idx_exists(db_root, object, fields[i], sch.splits)) {
                continue; /* skip existing */
            }
            memcpy(actual_fields[actual_count], fields[i], 256);
            actual_count++;
        }

        if (actual_count > 0) {
            TypedSchema *ts = load_typed_schema(db_root, object);

            /* Adaptive batching: group fields into passes whose combined estimated
               memory fits g_index_build_budget_bytes. Each pass keeps the existing
               parallel scan + parallel build machinery — we just bound peak memory
               so reindex on 25 M× 12-field schemas doesn't OOM the host. A single
               field that alone exceeds the budget is still processed alone (the
               "always include at least one" rule below). */
            int live_count = get_live_count(db_root, object);
            if (live_count < 0) live_count = 0;
            size_t budget = g_index_build_budget_bytes;
            if (budget < 64ULL * 1024 * 1024) budget = 64ULL * 1024 * 1024;

            size_t per_field_bytes[MAX_FIELDS];
            for (int i = 0; i < actual_count; i++)
                per_field_bytes[i] = estimate_field_build_bytes(ts, actual_fields[i],
                                                                (size_t)live_count);

            int n_batches = 0;
            int batch_start = 0;
            while (batch_start < actual_count) {
                size_t batch_bytes = 0;
                int batch_end = batch_start;
                while (batch_end < actual_count) {
                    size_t next = per_field_bytes[batch_end];
                    if (batch_end > batch_start && batch_bytes + next > budget) break;
                    batch_bytes += next;
                    batch_end++;
                }
                build_indexes_pass(db_root, object, &sch, ts, actual_fields,
                                   batch_start, batch_end - batch_start,
                                   (size_t)live_count);
                n_batches++;
                batch_start = batch_end;
            }
            log_msg(2, "ADD-INDEXES %s: %d fields in %d batch(es), live=%d, budget=%zu MB",
                    object, actual_count, n_batches, live_count,
                    budget / (1024 * 1024));
        }
    }

    /* === Write canonical index.conf for ALL original fields (typed +
       btree). Pre-fix this only ran for btree fields, so a plural
       add-index with only :bitmap / :trigram entries left index.conf
       unchanged — reindex saw no record of the field, and remove-index
       couldn't match it. Iterates `total_fields` (saved before the
       btree-only reduction) using the canonical names/types/maxes
       arrays populated at parse time. */
    mkdirp(dirname_of(conf_path));
    if (promoted) {
        /* Full rewrite from (names, types, maxes). Mirrors the writer
           in cmd_create_object's index.conf-emission block. */
        FILE *wf = fopen(conf_path, "w");
        if (wf) {
            for (int i = 0; i < total_fields; i++) {
                switch (types[i]) {
                    case IT_BTREE:
                        fprintf(wf, "%s\n", names[i]);
                        break;
                    case IT_BITMAP:
                        if (maxes[i] && maxes[i] != BM_DEFAULT_MAX_VALUES)
                            fprintf(wf, "%s:bitmap(%u)\n", names[i], maxes[i]);
                        else
                            fprintf(wf, "%s:bitmap\n", names[i]);
                        break;
                    case IT_TRIGRAM:
                        fprintf(wf, "%s:trigram\n", names[i]);
                        break;
                }
            }
            fclose(wf);
        }
    } else {
        /* Append-with-dedupe for every original field (typed lines too). */
        for (int i = 0; i < total_fields; i++) {
            char canon[300];
            switch (types[i]) {
                case IT_BITMAP:
                    if (maxes[i] && maxes[i] != BM_DEFAULT_MAX_VALUES)
                        snprintf(canon, sizeof(canon), "%s:bitmap(%u)", names[i], maxes[i]);
                    else
                        snprintf(canon, sizeof(canon), "%s:bitmap", names[i]);
                    break;
                case IT_TRIGRAM:
                    snprintf(canon, sizeof(canon), "%s:trigram", names[i]);
                    break;
                default:
                    snprintf(canon, sizeof(canon), "%s", names[i]);
                    break;
            }
            int already = 0;
            FILE *cf = fopen(conf_path, "r");
            if (cf) {
                char line[256];
                while (fgets(line, sizeof(line), cf)) {
                    line[strcspn(line, "\n")] = '\0';
                    if (strcmp(line, canon) == 0) { already = 1; break; }
                }
                fclose(cf);
            }
            if (!already) {
                FILE *af = fopen(conf_path, "a");
                if (af) { fprintf(af, "%s\n", canon); fclose(af); }
            }
        }
    }

    invalidate_idx_cache(object);
    /* Response semantics:
         all-typed-only            → {"status":"ok"} (legacy)
         btree present, all exist  → {"status":"all_exist"}
         btree built (>=1)         → {"status":"indexed","count":N}      */
    if (btree_count == 0) {
        OUT("{\"status\":\"ok\"}\n");
    } else if (actual_count == 0) {
        OUT("{\"status\":\"all_exist\"}\n");
    } else {
        OUT("{\"status\":\"indexed\",\"count\":%d}\n", actual_count);
    }
    return 0;
}

/* ========== remove-index ==========
   Drops a single index by exact name (matches whatever was passed to
   add-index, including composite "a+b" forms). Unlinks the .idx file,
   removes its line from index.conf, and invalidates caches. */

/* Unlink the on-disk index files for one canonical index.conf line,
   dispatched by type. Btree, bitmap, and trigram each live in
   `indexes/<field>/NNN.{idx,bm,tg}` — same directory, different
   extension — so we also rmdir the (now-empty) field directory after
   typed unlinks. */
static void unlink_index_by_line(const char *db_root, const char *object,
                                 const char *conf_line, int splits) {
    ParsedIndexSpec ps;
    if (parse_index_spec(conf_line, &ps) != 0) {
        /* Malformed entry — best-effort btree unlink so a botched
           pre-fix line like `username:trigram` (treated as a bare
           field) at least frees any stray btree leftovers. */
        btree_idx_unlink_all(db_root, object, conf_line, splits);
        return;
    }
    int idx_n = index_splits_for(splits);
    if (ps.type == IT_TRIGRAM) {
        for (int s = 0; s < idx_n; s++) {
            char tp[PATH_MAX];
            tg_build_path(tp, sizeof(tp), db_root, object, ps.name, s);
            btree_cache_invalidate(tp);
            unlink(tp);
        }
        char dir_path[PATH_MAX];
        snprintf(dir_path, sizeof(dir_path), "%s/%s/indexes/%s",
                 db_root, object, ps.name);
        rmdir(dir_path);
    } else if (ps.type == IT_BITMAP) {
        for (int s = 0; s < idx_n; s++) {
            char bp[PATH_MAX];
            bm_build_path(bp, sizeof(bp), db_root, object, ps.name, s);
            bm_cache_invalidate(bp);
            unlink(bp);
        }
        char dir_path[PATH_MAX];
        snprintf(dir_path, sizeof(dir_path), "%s/%s/indexes/%s",
                 db_root, object, ps.name);
        rmdir(dir_path);
    } else {
        btree_idx_unlink_all(db_root, object, ps.name, splits);
    }
}

int cmd_remove_index(const char *db_root, const char *object, const char *field) {
    if (!field || !field[0]) {
        OUT("{\"error\":\"field is required\"}\n");
        return 1;
    }

    Schema sch = load_schema(db_root, object);
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* Rewrite index.conf without the target line; capture the matched
       line so the typed unlink below sees the canonical form (and can
       distinguish btree / bitmap / trigram). */
    int found = 0;
    char matched_line[256] = {0};
    FILE *cf = fopen(conf_path, "r");
    if (cf) {
        char tmp_path[PATH_MAX];
        snprintf(tmp_path, sizeof(tmp_path), "%s.new", conf_path);
        FILE *nf = fopen(tmp_path, "w");
        if (!nf) { fclose(cf); OUT("{\"error\":\"Cannot write index.conf.new\"}\n"); return 1; }

        char line[256];
        while (fgets(line, sizeof(line), cf)) {
            char stripped[256];
            strncpy(stripped, line, sizeof(stripped) - 1);
            stripped[sizeof(stripped) - 1] = '\0';
            stripped[strcspn(stripped, "\n")] = '\0';
            if (strcmp(stripped, field) == 0) {
                found = 1;
                strncpy(matched_line, stripped, sizeof(matched_line) - 1);
                matched_line[sizeof(matched_line) - 1] = '\0';
                continue;
            }
            fprintf(nf, "%s", line);
        }
        fclose(cf);
        fclose(nf);
        if (rename(tmp_path, conf_path) != 0) {
            unlink(tmp_path);
            OUT("{\"error\":\"Failed to rewrite index.conf\"}\n");
            return 1;
        }
    }

    if (!found) {
        OUT("{\"status\":\"not_indexed\",\"field\":\"%s\"}\n", field);
        return 0;
    }

    unlink_index_by_line(db_root, object, matched_line, sch.splits);
    invalidate_idx_cache(object);

    log_msg(3, "REMOVE-INDEX %s/%s: %s", db_root, object, field);
    OUT("{\"status\":\"removed\",\"field\":\"%s\"}\n", field);
    return 0;
}

int cmd_remove_indexes(const char *db_root, const char *object, const char *fields_json) {
    char fields[MAX_FIELDS][256];
    int nfields = 0;
    const char *p = json_skip(fields_json);
    if (*p != '[') {
        OUT("{\"error\":\"fields must be a JSON array\"}\n");
        return 1;
    }
    p++;
    while (*p && nfields < MAX_FIELDS) {
        p = json_skip(p);
        if (*p == ']') break;
        if (*p == ',') { p++; continue; }
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            int flen = (int)(p - start);
            if (flen > 0 && flen < 255) {
                memcpy(fields[nfields], start, flen);
                fields[nfields][flen] = '\0';
                nfields++;
            }
            if (*p == '"') p++;
        } else {
            p++;
        }
    }

    if (nfields == 0) {
        OUT("{\"error\":\"fields array is empty\"}\n");
        return 1;
    }

    Schema sch = load_schema(db_root, object);
    int removed = 0, missing = 0;
    for (int i = 0; i < nfields; i++) {
        char conf_path[PATH_MAX];
        snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

        int found = 0;
        char matched_line[256] = {0};
        FILE *cf = fopen(conf_path, "r");
        if (cf) {
            char tmp_path[PATH_MAX];
            snprintf(tmp_path, sizeof(tmp_path), "%s.new", conf_path);
            FILE *nf = fopen(tmp_path, "w");
            if (!nf) { fclose(cf); continue; }
            char line[256];
            while (fgets(line, sizeof(line), cf)) {
                char stripped[256];
                strncpy(stripped, line, sizeof(stripped) - 1);
                stripped[sizeof(stripped) - 1] = '\0';
                stripped[strcspn(stripped, "\n")] = '\0';
                if (strcmp(stripped, fields[i]) == 0) {
                    found = 1;
                    strncpy(matched_line, stripped, sizeof(matched_line) - 1);
                    matched_line[sizeof(matched_line) - 1] = '\0';
                    continue;
                }
                fprintf(nf, "%s", line);
            }
            fclose(cf);
            fclose(nf);
            if (rename(tmp_path, conf_path) != 0) { unlink(tmp_path); continue; }
        }

        if (found) {
            unlink_index_by_line(db_root, object, matched_line, sch.splits);
            removed++;
        } else {
            missing++;
        }
    }

    invalidate_idx_cache(object);
    log_msg(3, "REMOVE-INDEX %s/%s: %d removed, %d not_indexed", db_root, object, removed, missing);
    OUT("{\"status\":\"removed\",\"count\":%d,\"not_indexed\":%d}\n", removed, missing);
    return 0;
}

/* ========== REINDEX ==========
   Rebuilds every index for matching objects. Triggered by the v1→v2 btree
   format migration; safe to run repeatedly (idempotent).

   Walks $DB_ROOT/schema.conf, filters by (dir, object), and for each target
   rebuilds every index listed in that object's indexes/index.conf via
   cmd_add_indexes(..., force=1). cmd_add_indexes's per-call OUT is redirected
   to /dev/null during the loop so reindex emits a single summary document.
   Per-object progress is logged at info level only.

   Also cleans up any legacy single-file <field>.idx artefacts left over
   from the pre-2026.05.1 layout — one ./shard-db reindex after upgrade
   gets the indexes/ directory into the new <field>/<NNN>.idx shape with
   no orphans on disk. */

/* Wipe every per-field idx directory + legacy <field>.idx file under
   indexes/, preserving index.conf. Used by reindex_object before a force=1
   rebuild so the new layout starts from a clean slate (vacuum --splits=N
   in particular needs this — the old layout's idx_splits = old_splits/4
   doesn't match the new splits, and btree_idx_unlink_all only walks the
   new shard count, leaving high-index orphans behind). */
static void reindex_wipe_idx_dirs(const char *eff_root, const char *object) {
    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/%s/indexes", eff_root, object);
    DIR *d = opendir(idx_dir);
    if (!d) return;
    int dfd = dirfd(d);

    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        if (strcmp(e->d_name, "index.conf") == 0) continue;

        struct stat st;
        /* fstatat against the open dirfd ties the metadata check to the
           same inode that unlinkat/rmrf will operate on: TOCTOU-safe. */
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) continue;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", idx_dir, e->d_name);

        if (S_ISDIR(st.st_mode)) {
            /* Per-shard layout: indexes/<field>/<NNN>.{idx,bm,tg}.
               Drop every cached btree mapping under this directory
               before rmrf so ucache doesn't keep stale fds alive.
               Bitmap (.bm) and trigram (.tg) files don't use ucache
               but the rmrf cleans them too — they get rebuilt below
               in the type-aware cmd_add_indexes path. */
            DIR *sub = opendir(path);
            if (sub) {
                struct dirent *se;
                while ((se = readdir(sub))) {
                    if (se->d_name[0] == '.') continue;
                    char sp[PATH_MAX];
                    snprintf(sp, sizeof(sp), "%s/%s", path, se->d_name);
                    btree_cache_invalidate(sp);
                }
                closedir(sub);
            }
            rmrf(path);
        } else if (S_ISREG(st.st_mode)) {
            /* Legacy single-file <field>.idx artefact. */
            btree_cache_invalidate(path);
            unlinkat(dfd, e->d_name, 0);
        }
    }
    closedir(d);
}

/* Rebuild every index for one object: read index.conf for the field list,
   wipe stale on-disk idx files (any layout), then cmd_add_indexes(force=1).
   Used by both cmd_reindex (multi-object walk) and rebuild_object (after a
   vacuum --splits or --compact that may have changed the layout under our
   feet). Returns the number of indexes rebuilt; 0 if the object has no
   index.conf or it's empty. */
int reindex_object(const char *eff_root, const char *object) {
    char ic_path[PATH_MAX];
    snprintf(ic_path, sizeof(ic_path), "%s/%s/indexes/index.conf",
             eff_root, object);
    FILE *ic = fopen(ic_path, "r");
    if (!ic) return 0;

    char fields_json[8192];
    int pos = snprintf(fields_json, sizeof(fields_json), "[");
    int nf = 0;
    char fline[512];
    while (fgets(fline, sizeof(fline), ic)) {
        fline[strcspn(fline, "\n")] = '\0';
        if (!fline[0]) continue;
        int avail = (int)sizeof(fields_json) - pos - 8;
        if (avail <= 0) break;
        pos += snprintf(fields_json + pos, avail,
                        "%s\"%s\"", nf ? "," : "", fline);
        nf++;
    }
    fclose(ic);
    snprintf(fields_json + pos, sizeof(fields_json) - pos, "]");
    if (nf == 0) return 0;

    reindex_wipe_idx_dirs(eff_root, object);

    /* cmd_add_indexes emits its own JSON to OUT; redirect to /dev/null so
       reindex_object stays silent (callers wrap their own response). */
    FILE *saved_out = g_out;
    FILE *devnull = fopen("/dev/null", "w");
    g_out = devnull ? devnull : saved_out;
    cmd_add_indexes(eff_root, object, fields_json, 1);
    g_out = saved_out;
    if (devnull) fclose(devnull);

    log_msg(3, "REINDEX %s/%s: rebuilt %d indexes", eff_root, object, nf);
    return nf;
}

/* Legacy single-file sweep — kept for cmd_reindex's per-object loop where
   reindex_wipe_idx_dirs would already handle it, but documented separately
   so the upgrade path stays clear. */
static void reindex_clean_legacy(const char *eff_root, const char *object) __attribute__((unused));
static void reindex_clean_legacy(const char *eff_root, const char *object) {
    char idx_dir[PATH_MAX];
    snprintf(idx_dir, sizeof(idx_dir), "%s/%s/indexes", eff_root, object);
    DIR *d = opendir(idx_dir);
    if (!d) return;
    int dfd = dirfd(d);

    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.') continue;
        size_t nlen = strlen(e->d_name);
        if (nlen < 5 || strcmp(e->d_name + nlen - 4, ".idx") != 0) continue;

        struct stat st;
        /* fstatat + unlinkat against the open dirfd: TOCTOU-safe. */
        if (fstatat(dfd, e->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0 ||
            !S_ISREG(st.st_mode)) continue;

        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/%s", idx_dir, e->d_name);
        btree_cache_invalidate(path);
        if (unlinkat(dfd, e->d_name, 0) == 0) {
            log_msg(3, "REINDEX %s/%s: cleaned legacy single-file index %s",
                    eff_root, object, e->d_name);
        }
    }
    closedir(d);
}

int cmd_reindex(const char *db_root, const char *dir_filter, const char *obj_filter) {
    char scpath[PATH_MAX];
    snprintf(scpath, sizeof(scpath), "%s/schema.conf", db_root);
    FILE *sf = fopen(scpath, "r");
    if (!sf) {
        OUT("{\"error\":\"cannot open schema.conf\"}\n");
        return 1;
    }

    uint64_t t0 = now_ms_coarse();
    int objects_rebuilt = 0;
    int objects_skipped = 0;
    int indexes_rebuilt = 0;

    char line[1024];
    while (fgets(line, sizeof(line), sf)) {
        line[strcspn(line, "\n")] = '\0';
        if (!line[0] || line[0] == '#') continue;

        /* Parse "dir:object:..." — only the first two colon-separated tokens. */
        char *c1 = strchr(line, ':');
        if (!c1) continue;
        *c1 = '\0';
        const char *dir = line;
        char *rest = c1 + 1;
        char *c2 = strchr(rest, ':');
        if (!c2) continue;
        *c2 = '\0';
        const char *obj = rest;

        if (dir_filter && strcmp(dir, dir_filter) != 0) continue;
        if (obj_filter && strcmp(obj, obj_filter) != 0) continue;

        char eff_root[PATH_MAX];
        snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);

        /* reindex_object handles everything: reads index.conf, wipes any
           stale on-disk artefacts (including high-numbered idx shards left
           behind by a vacuum --splits=N where the new index_splits is
           smaller than the old one — the very situation that bit users
           on the splits=64 → splits=32 path), and rebuilds via
           cmd_add_indexes(force=1). */
        int n = reindex_object(eff_root, obj);
        if (n > 0) {
            objects_rebuilt++;
            indexes_rebuilt += n;
        } else {
            objects_skipped++;
        }
    }
    fclose(sf);

    uint64_t t1 = now_ms_coarse();
    OUT("{\"status\":\"reindexed\",\"objects\":%d,\"skipped\":%d,\"indexes\":%d,\"duration_ms\":%llu}\n",
        objects_rebuilt, objects_skipped, indexes_rebuilt,
        (unsigned long long)(t1 - t0));
    return 0;
}
