#include "types.h"

#ifdef TEST_BUILD
static _Atomic int g_indexed_abort_fail_after;

void index_test_abort_fail_after(int n) {
    atomic_store_explicit(&g_indexed_abort_fail_after, n > 0 ? n : 0,
                          memory_order_release);
}

static int index_test_should_fail_after_success(void) {
    int remaining = atomic_load_explicit(&g_indexed_abort_fail_after,
                                         memory_order_acquire);
    while (remaining > 0) {
        if (atomic_compare_exchange_weak_explicit(
                &g_indexed_abort_fail_after, &remaining, remaining - 1,
                memory_order_acq_rel, memory_order_acquire))
            return remaining == 1;
    }
    return 0;
}
#else
static int index_test_should_fail_after_success(void) { return 0; }
#endif
#include "bitmap.h"
#include "trigram.h"
#include "slotcask.h"
#include "io_direct.h"

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

int btree_idx_insert(const char *db_root, const char *object,
                     const char *field, int splits,
                     const char *value, size_t vlen,
                     const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, idx_shard);
    return btree_insert(idx_path, value, vlen, hash);
}

int btree_idx_delete(const char *db_root, const char *object,
                     const char *field, int splits,
                     const char *value, size_t vlen,
                     const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char idx_path[PATH_MAX];
    build_idx_path(idx_path, sizeof(idx_path), db_root, object, field, idx_shard);
    return btree_delete(idx_path, value, vlen, hash);
}

/* Trigram index entry insert/delete. Mirror of btree_idx_* but routes
   to .tg files instead of .idx — both extensions share the BTRH btree
   format, so the underlying primitives (btree_insert/btree_delete) are
   reused unchanged. A field may carry BOTH .idx and .tg simultaneously;
   bt_cache treats them as independent path-keyed cache entries. */
static int tg_idx_insert(const char *db_root, const char *object,
                         const char *field, int splits,
                         const uint8_t trigram[3],
                         const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char tg_path[PATH_MAX];
    tg_build_path(tg_path, sizeof(tg_path), db_root, object, field, idx_shard);
    return btree_insert(tg_path, (const char *)trigram, 3, hash);
}

static int tg_idx_delete(const char *db_root, const char *object,
                         const char *field, int splits,
                         const uint8_t trigram[3],
                         const uint8_t hash[BT_HASH_SIZE]) {
    int idx_shard = idx_shard_for_hash(hash, splits);
    char tg_path[PATH_MAX];
    tg_build_path(tg_path, sizeof(tg_path), db_root, object, field, idx_shard);
    return btree_delete(tg_path, (const char *)trigram, 3, hash);
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
    parallel_for_io(shard_walk_worker, args, n, sizeof(ShardWalkArg));
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
   Thin adapter: builds per-shard BtOrderedRangeSpec array and delegates
   to btree_walk_ordered_ranges (the single authoritative merge in btree.c). */

void btree_idx_walk_ordered(const char *db_root, const char *object,
                            const char *field, int splits,
                            const char *min_val, size_t min_len, int min_exclusive,
                            const char *max_val, size_t max_len, int max_exclusive,
                            int desc, bt_ordered_result_cb cb, void *ctx) {
    int n = index_splits_for(splits);
    BtOrderedRangeSpec *ranges = calloc((size_t)n, sizeof(BtOrderedRangeSpec));
    char (*paths)[PATH_MAX] = calloc((size_t)n, sizeof(*paths));
    if (!ranges || !paths) { free(ranges); free(paths); return; }

    /* Caller bounds truncated to BT_MAX_VAL_LEN as before. */
    char lo_buf[BT_MAX_VAL_LEN];
    size_t lo_len = min_len > sizeof(lo_buf) ? sizeof(lo_buf) : min_len;
    if (lo_len) memcpy(lo_buf, min_val, lo_len);
    char hi_buf[BT_MAX_VAL_LEN];
    size_t hi_len = max_len > sizeof(hi_buf) ? sizeof(hi_buf) : max_len;
    if (hi_len) memcpy(hi_buf, max_val, hi_len);

    for (int s = 0; s < n; s++) {
        build_idx_path(paths[s], sizeof(paths[s]), db_root, object, field, s);
        ranges[s].path         = paths[s];
        ranges[s].min_val      = lo_len ? lo_buf : NULL;
        ranges[s].min_len      = lo_len;
        ranges[s].min_exclusive = min_exclusive;
        ranges[s].max_val      = hi_len ? hi_buf : NULL;
        ranges[s].max_len      = hi_len;
        ranges[s].max_exclusive = max_exclusive;
        ranges[s].tie_id       = s;
    }

    btree_walk_ordered_ranges(ranges, (size_t)n, desc, cb, ctx);
    free(ranges);
    free(paths);
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
int write_index_entry(const char *db_root, const char *object,
                      const char *field, int splits,
                      const uint8_t *val, size_t vlen,
                      const uint8_t hash16[16]) {
    return btree_idx_insert(db_root, object, field, splits,
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
                         const uint8_t *old_val, size_t old_len,
                         int sync_after) {
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
    if (!bm) return -2;

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
        if (bm_grow(bm, grown) != 0) {
            bm_close(bm);
            return -1;
        }
    }

    int rc = 0;
    if (old_val && bm_clear(bm, old_val, old_len, kf_slot) != 0) rc = -1;
    if (new_val) {
        if (bm_set(bm, new_val, new_len, kf_slot) != 0) {
            rc = -1;
        }
    }
    if (sync_after && bm_sync(bm) != 0) rc = -1;
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
    a->out_error = 0;
    a->out_errno = 0;

    switch (a->type) {
        case IT_BTREE:
            if (a->old_key &&
                delete_index_entry(a->db_root, a->object, a->field, a->splits,
                                   a->old_key, a->old_len, a->hash) != 0) {
                a->out_error = -2;
                a->out_errno = errno;
                break;
            }
            if (a->new_key &&
                write_index_entry(a->db_root, a->object, a->field, a->splits,
                                  a->new_key, a->new_len, a->hash) != 0) {
                a->out_error = -2;
                a->out_errno = errno;
            }
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char idx_path[PATH_MAX];
                build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(idx_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            }
            if (!a->out_error && index_test_should_fail_after_success()) {
                a->out_error = -2;
                a->out_errno = EIO;
            }
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
                    if (tg_idx_delete(a->db_root, a->object, a->field, a->splits,
                                      old_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                }
            }

            if (a->out_error) break;

            /* Insert new-only trigrams. */
            for (size_t i = 0; i < n_new; i++) {
                int in_old = 0;
                for (size_t j = 0; j < n_old; j++) {
                    if (memcmp(new_tg[i], old_tg[j], 3) == 0) { in_old = 1; break; }
                }
                if (!in_old) {
                    if (tg_idx_insert(a->db_root, a->object, a->field, a->splits,
                                      new_tg[i], a->hash) != 0) {
                        a->out_error = -2;
                        a->out_errno = errno;
                        break;
                    }
                }
            }
            if (!a->out_error && a->sync_after) {
                int shard = idx_shard_for_hash(a->hash, a->splits);
                char tg_path[PATH_MAX];
                tg_build_path(tg_path, sizeof(tg_path), a->db_root, a->object, a->field, shard);
                if (btree_sync_path(tg_path) != 0) {
                    a->out_error = -2;
                    a->out_errno = errno;
                }
            }
            if (!a->out_error && index_test_should_fail_after_success()) {
                a->out_error = -2;
                a->out_errno = EIO;
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
                                         a->old_key, a->old_len,
                                         a->sync_after);
            if (!a->out_error && index_test_should_fail_after_success()) {
                a->out_error = -2;
                a->out_errno = EIO;
            }
            break;
    }
    return NULL;
}

/* ── Bitmap prepare/apply split (see types.h for the contract) ────────── */

int bitmap_prepare_set_init(BitmapPrepareSet *set, size_t max_entries) {
    set->entries = calloc(max_entries, sizeof(BitmapPrepareEntry));
    if (!set->entries) return -1;
    set->n = 0;
    set->cap = max_entries;
    return 0;
}

static BitmapShard *bitmap_prepare_open(const UpdateIdxArg *arg) {
    char path[1024];
    bitmap_shard_path(path, sizeof(path), arg->db_root, arg->object, arg->field, arg->kf_shard);
    int bool_fastpath = 0;
    if (arg->new_key && arg->new_len == 1) bool_fastpath = 1;
    else if (arg->old_key && arg->old_len == 1) bool_fastpath = 1;
    int slots = (int)slotcask_default_slots_for_splits(arg->splits);
    /* Growing the shard (if kf_slot doesn't fit yet) is deferred to apply
       time — bm_grow() can fsync+publish a rewritten file, a durable
       mutation that must not happen during prepare, before any commit-
       intent marker exists for a record that might still be rejected. */
    return bm_open(path, slots, 1, bool_fastpath, arg->bm_max_values, 1 /* writer */);
}

/* Grow bm (if needed) so kf_slot fits, right before the apply-phase set/
   clear that targets it. Called only after the window/set's marker is
   already durable — a failure here is a genuine I/O error, not a policy
   rejection, and is propagated like any other apply failure. */
static int bitmap_apply_grow_for_slot(BitmapShard *bm, uint32_t kf_slot) {
    if (kf_slot < bm_slots(bm)) return 0;
    uint32_t want = kf_slot + 1;
    uint32_t grown = 1;
    while (grown < want && grown < 0x80000000u) grown <<= 1;
    return bm_grow(bm, grown);
}

int bitmap_prepare_set_add(BitmapPrepareSet *set, const UpdateIdxArg *arg,
                           char *err_field, size_t err_field_len) {
    if (set->n >= set->cap) return -2;
    BitmapShard *bm = bitmap_prepare_open(arg);
    if (!bm) return -2;

    if (arg->new_key) {
        int would = bm_dict_would_exceed_cap(bm, arg->new_key, arg->new_len);
        if (would < 0) { bm_close(bm); return -2; }
        if (would) {
            bm_close(bm);
            if (err_field) snprintf(err_field, err_field_len, "%s", arg->field);
            return -1;
        }
    }

    BitmapPrepareEntry *e = &set->entries[set->n++];
    snprintf(e->field, sizeof(e->field), "%s", arg->field);
    e->kf_shard = arg->kf_shard;
    e->bm = bm;
    e->new_val = arg->new_key;
    e->new_len = arg->new_len;
    e->old_val = arg->old_key;
    e->old_len = arg->old_len;
    e->kf_slot = arg->kf_slot;
    e->sync_after = arg->sync_after;
    return 0;
}

int bitmap_prepare_set_apply(BitmapPrepareSet *set) {
    int rc = 0;
    for (size_t i = 0; i < set->n; i++) {
        BitmapPrepareEntry *e = &set->entries[i];
        BitmapShard *bm = (BitmapShard *)e->bm;
        if (!bm) continue;
        if (bitmap_apply_grow_for_slot(bm, e->kf_slot) != 0) {
            rc = -1;
        } else {
            if (e->old_val) {
                if (bm_clear(bm, e->old_val, e->old_len, e->kf_slot) != 0) rc = -1;
            }
            if (e->new_val) {
                if (bm_set(bm, e->new_val, e->new_len, e->kf_slot) != 0) rc = -1;
            }
            if (e->sync_after) {
                if (bm_sync(bm) != 0) rc = -1;
            }
        }
        bm_close(bm);
        e->bm = NULL;
    }
    set->n = 0;
    return rc;
}

void bitmap_prepare_set_abort(BitmapPrepareSet *set) {
    for (size_t i = 0; i < set->n; i++) {
        if (set->entries[i].bm) {
            bm_close((BitmapShard *)set->entries[i].bm);
            set->entries[i].bm = NULL;
        }
    }
    set->n = 0;
}

void bitmap_prepare_set_free(BitmapPrepareSet *set) {
    if (!set) return;
    bitmap_prepare_set_abort(set);
    free(set->entries);
    set->entries = NULL;
    set->cap = 0;
}

/* ── Window-scoped variant for bulk ─────────────────────────────────── */

int bitmap_prepare_window_init(BitmapPrepareWindow *win, size_t max_fields, size_t max_records) {
    memset(win, 0, sizeof(*win));
    win->entries = calloc(max_fields, sizeof(BitmapWindowEntry));
    win->ops = calloc(max_records, sizeof(BitmapWindowOp));
    if (!win->entries || !win->ops) {
        free(win->entries); free(win->ops);
        win->entries = NULL; win->ops = NULL;
        return -1;
    }
    win->cap_entries = max_fields;
    win->cap_ops = max_records;
    return 0;
}

static BitmapWindowEntry *bitmap_window_find_or_open(BitmapPrepareWindow *win, const UpdateIdxArg *arg) {
    for (size_t i = 0; i < win->n_entries; i++) {
        BitmapWindowEntry *e = &win->entries[i];
        if (e->kf_shard == arg->kf_shard && strcmp(e->field, arg->field) == 0) return e;
    }
    if (win->n_entries >= win->cap_entries) return NULL;
    BitmapShard *bm = bitmap_prepare_open(arg);
    if (!bm) return NULL;
    BitmapWindowEntry *e = &win->entries[win->n_entries++];
    snprintf(e->field, sizeof(e->field), "%s", arg->field);
    e->kf_shard = arg->kf_shard;
    e->bm = bm;
    e->pending = NULL;
    e->npending = 0;
    e->pending_cap = 0;
    return e;
}

int bitmap_prepare_window_add(BitmapPrepareWindow *win, const UpdateIdxArg *arg,
                              char *err_field, size_t err_field_len) {
    BitmapWindowEntry *e = bitmap_window_find_or_open(win, arg);
    if (!e) return -2;
    BitmapShard *bm = (BitmapShard *)e->bm;

    if (arg->new_key) {
        int already = bm_dict_contains(bm, arg->new_key, arg->new_len);
        int in_pending = 0;
        if (!already) {
            for (size_t i = 0; i < e->npending; i++) {
                if (e->pending[i].vlen == arg->new_len &&
                    memcmp(e->pending[i].value, arg->new_key, arg->new_len) == 0) {
                    in_pending = 1;
                    break;
                }
            }
        }
        if (!already && !in_pending) {
            uint32_t on_disk = bm_n_values(bm);
            uint32_t max_v = bm_max_values(bm);
            if ((uint64_t)on_disk + (uint64_t)e->npending + 1 > (uint64_t)max_v) {
                if (err_field) snprintf(err_field, err_field_len, "%s", arg->field);
                return 1; /* this record rejected; entry/handle stay open for other records */
            }
            if (e->npending >= e->pending_cap) {
                size_t ncap = e->pending_cap ? e->pending_cap * 2 : 8;
                BitmapPendingValue *np = realloc(e->pending, ncap * sizeof(BitmapPendingValue));
                if (!np) return -2;
                e->pending = np;
                e->pending_cap = ncap;
            }
            e->pending[e->npending].value = arg->new_key;
            e->pending[e->npending].vlen = arg->new_len;
            e->npending++;
        }
    }

    if (win->n_ops >= win->cap_ops) return -2;
    BitmapWindowOp *op = &win->ops[win->n_ops++];
    op->entry_idx = (size_t)(e - win->entries);
    op->new_val = arg->new_key;
    op->new_len = arg->new_len;
    op->old_val = arg->old_key;
    op->old_len = arg->old_len;
    op->kf_slot = arg->kf_slot;
    op->sync_after = arg->sync_after;
    return 0;
}

int bitmap_prepare_window_apply(BitmapPrepareWindow *win) {
    int rc = 0;
    for (size_t i = 0; i < win->n_ops; i++) {
        BitmapWindowOp *op = &win->ops[i];
        BitmapShard *bm = (BitmapShard *)win->entries[op->entry_idx].bm;
        if (!bm) continue;
        if (bitmap_apply_grow_for_slot(bm, op->kf_slot) != 0) {
            rc = -1;
            continue;
        }
        if (op->old_val) {
            if (bm_clear(bm, op->old_val, op->old_len, op->kf_slot) != 0) rc = -1;
        }
        if (op->new_val) {
            if (bm_set(bm, op->new_val, op->new_len, op->kf_slot) != 0) rc = -1;
        }
        if (op->sync_after) {
            if (bm_sync(bm) != 0) rc = -1;
        }
    }
    for (size_t i = 0; i < win->n_entries; i++) {
        BitmapWindowEntry *e = &win->entries[i];
        if (e->bm) { bm_close((BitmapShard *)e->bm); e->bm = NULL; }
        free(e->pending);
        e->pending = NULL;
    }
    win->n_entries = 0;
    win->n_ops = 0;
    return rc;
}

void bitmap_prepare_window_abort(BitmapPrepareWindow *win) {
    for (size_t i = 0; i < win->n_entries; i++) {
        BitmapWindowEntry *e = &win->entries[i];
        if (e->bm) { bm_close((BitmapShard *)e->bm); e->bm = NULL; }
        free(e->pending);
        e->pending = NULL;
    }
    win->n_entries = 0;
    win->n_ops = 0;
}

void bitmap_prepare_window_free(BitmapPrepareWindow *win) {
    if (!win) return;
    bitmap_prepare_window_abort(win);
    free(win->entries);
    free(win->ops);
    win->entries = NULL;
    win->ops = NULL;
    win->cap_entries = 0;
    win->cap_ops = 0;
}

void bitmap_prepare_window_checkpoint(const BitmapPrepareWindow *win,
                                      BitmapWindowCheckpoint *cp) {
    cp->n_ops = win->n_ops;
    cp->n_entries = win->n_entries;
    for (size_t i = 0; i < win->n_entries && i < MAX_FIELDS; i++)
        cp->npending[i] = win->entries[i].npending;
}

void bitmap_prepare_window_rollback(BitmapPrepareWindow *win,
                                    const BitmapWindowCheckpoint *cp) {
    win->n_ops = cp->n_ops;
    for (size_t i = 0; i < cp->n_entries && i < win->n_entries && i < MAX_FIELDS; i++)
        win->entries[i].npending = cp->npending[i];
    /* Entries opened for the first time by this record (didn't exist at
       checkpoint time) had no pending values before it either. */
    for (size_t i = cp->n_entries; i < win->n_entries; i++)
        win->entries[i].npending = 0;
}

/* Delete from index — uses B+ tree */
int delete_index_entry(const char *db_root, const char *object,
                       const char *field, int splits,
                       const uint8_t *val, size_t vlen,
                       const uint8_t hash16[16]) {
    return btree_idx_delete(db_root, object, field, splits,
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
    int sync_after;
    int out_error;
    int out_errno;
} IndexThreadArg;

void *index_thread_fn(void *arg) {
    IndexThreadArg *a = (IndexThreadArg *)arg;
    a->out_error = write_index_entry(a->db_root, a->object, a->field,
                                     a->splits, a->val, a->vlen, a->hash16);
    a->out_errno = a->out_error ? errno : 0;
    if (!a->out_error && a->sync_after) {
        int shard = idx_shard_for_hash(a->hash16, a->splits);
        char idx_path[PATH_MAX];
        build_idx_path(idx_path, sizeof(idx_path), a->db_root, a->object, a->field, shard);
        if (btree_sync_path(idx_path) != 0) {
            a->out_error = -2;
            a->out_errno = errno;
        }
    }
    if (!a->out_error && index_test_should_fail_after_success()) {
        a->out_error = -2;
        a->out_errno = EIO;
    }
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

int index_parallel(const char *db_root, const char *object, int splits,
                   const char *value, const uint8_t hash16[16],
                   char fields[][256], int nfields,
                   const enum IndexType *types, int sync_after) {
    if (nfields <= 0) return 0;

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
    for (int j = 0; j < unique_count; j++) {
        if (!extracted[j]) continue;
        int fidx = ts ? typed_field_index(ts, unique_keys[j]) : -1;
        if (fidx < 0 || ts->fields[fidx].type != FT_VARCHAR) continue;
        char *unesc = NULL; size_t ulen = 0;
        errno = 0;
        if (json_unescape_cstring(extracted[j], strlen(extracted[j]), &unesc, &ulen) != 0) {
            /* The record encoder already validated this exact JSON before
               pre_commit. A later failure can still be OOM; it must abort
               the pre_commit, never silently omit this index key. */
            int decode_errno = (errno == ENOMEM) ? ENOMEM : EINVAL;
            for (int k = 0; k < unique_count; k++) free(extracted[k]);
            for (int k = 0; k < unique_count; k++) {
                int is_field = 0;
                for (int m = 0; m < nfields; m++)
                    if (unique_keys[k] == fields[m]) { is_field = 1; break; }
                if (!is_field) free((char *)unique_keys[k]);
            }
            errno = decode_errno;
            return -1;
        }
        free(extracted[j]);
        extracted[j] = unesc;
    }

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
            /* Composite — typed binary concat of sub-field values. */
            char fbuf[256];
            strncpy(fbuf, fields[i], 255); fbuf[255] = '\0';
            char result[4096];
            int pos = 0;
            int all_present = 1;
            char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
            while (tok) {
                int fidx = ts ? typed_field_index(ts, tok) : -1;
                /* Find the extracted text value */
                char *txt = NULL;
                for (int j = 0; j < unique_count; j++) {
                    if (strcmp(unique_keys[j], tok) == 0) {
                        txt = extracted[j];
                        break;
                    }
                }
                if (!txt || txt[0] == '\0') { all_present = 0; break; }
                if (fidx >= 0) {
                    const TypedField *f = &ts->fields[fidx];
                    size_t blen = 0;
                    encode_field_for_index(f, txt, strlen(txt),
                                            (uint8_t *)result + pos, &blen);
                    if (blen == 0) { all_present = 0; break; }
                    if (pos + (int)blen < (int)sizeof(result)) { pos += (int)blen; }
                    else { all_present = 0; break; }
                } else {
                    /* No schema — pass raw bytes */
                    int len = strlen(txt);
                    if (pos + len < (int)sizeof(result)) {
                        memcpy(result + pos, txt, len);
                        pos += len;
                    }
                }
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
        args[tcount].sync_after = sync_after;
        args[tcount].out_error = 0;
        args[tcount].out_errno = 0;
        tcount++;
    }

    parallel_for(index_thread_fn, args, tcount, sizeof(IndexThreadArg));

    int rc = 0;
    int saved_errno = 0;
    for (int i = 0; i < tcount; i++) {
        if (args[i].out_error && rc == 0) {
            rc = -1;
            saved_errno = args[i].out_errno;
        }
        free(idx_keys[i]);
    }
    for (int i = 0; i < unique_count; i++) free(extracted[i]);
    for (int i = 0; i < unique_count; i++) {
        int is_field = 0;
        for (int j = 0; j < nfields; j++)
            if (unique_keys[i] == fields[j]) { is_field = 1; break; }
        if (!is_field) free((char *)unique_keys[i]);
    }
    if (rc != 0) errno = saved_errno ? saved_errno : EIO;
    return rc;
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
            size_t blen = 0;
            typed_field_to_index_key(ts, record, fi, out + cp, &blen);
            if (blen == 0) return 0;
            if (cp + blen > out_cap) return -1;
            cp += (int)blen;
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
        /* Composite — binary index-key concat per field. */
        char fb[256]; strncpy(fb, spec, 255); fb[255] = '\0';
        char cat[4096]; int cp = 0; int ok = 1;
        char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
        while (tok) {
            int fi = typed_field_index(ts, tok);
            if (fi < 0) { ok = 0; break; }
            size_t blen = 0;
            typed_field_to_index_key(ts, record, fi, (uint8_t *)cat + cp, &blen);
            if (blen == 0) { ok = 0; break; }
            if (cp + (int)blen < (int)sizeof(cat)) { cp += (int)blen; }
            else { ok = 0; break; }
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
        /* Composite — extract each sub-field and encode to index-key bytes. */
        char fb[256]; strncpy(fb, spec, 255); fb[255] = '\0';
        const char *subs[16]; int nsub = 0;
        char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
        while (tok && nsub < 16) { subs[nsub++] = tok; tok = strtok_r(NULL, "+", &_tok_save); }
        char *vals[16];
        enum FieldType sub_types[16] = {0};
        for (int i = 0; i < nsub; i++) {
            int fi = ts ? typed_field_index(ts, subs[i]) : -1;
            /* FT_COUNT is a sentinel that can never equal FT_VARCHAR;
               unknown/legacy fields therefore retain json_get_fields'
               original raw-text behavior. */
            sub_types[i] = (fi >= 0) ? ts->fields[fi].type : FT_COUNT;
        }
        errno = 0;
        if (json_get_fields_unescaped(json, subs, nsub, sub_types, vals) != 0) {
            /* Upstream content validation cannot guarantee this second
               allocation succeeds. Preserve the index/record invariant by
               returning the tri-state error, not "field absent". */
            for (int i = 0; i < nsub; i++) free(vals[i]);
            if (errno != ENOMEM) errno = EINVAL;
            return -1;
        }
        char cat[4096]; int cp = 0; int ok = 1;
        for (int i = 0; i < nsub; i++) {
            if (!vals[i] || vals[i][0] == '\0') { ok = 0; break; }
            int fi = ts ? typed_field_index(ts, subs[i]) : -1;
            if (fi >= 0) {
                const TypedField *f = &ts->fields[fi];
                size_t blen = 0;
                encode_field_for_index(f, vals[i], strlen(vals[i]),
                                        (uint8_t *)cat + cp, &blen);
                if (blen == 0) { ok = 0; break; }
                if (cp + (int)blen < (int)sizeof(cat)) { cp += (int)blen; }
                else { ok = 0; break; }
            } else {
                /* No schema → fall back to raw string (backward compat) */
                int sl = strlen(vals[i]);
                if (cp + sl < (int)sizeof(cat)) { memcpy(cat + cp, vals[i], sl); cp += sl; }
                else { ok = 0; break; }
            }
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
    int fi = ts ? typed_field_index(ts, spec) : -1;
    char *txt;
    if (fi >= 0 && ts->fields[fi].type == FT_VARCHAR) {
        const char *v; size_t vl;
        if (!json_obj_unquoted(&jo, spec, &v, &vl)) return 0;
        size_t ulen = 0;
        errno = 0;
        if (json_unescape_cstring(v, vl, &txt, &ulen) != 0) {
            if (errno != ENOMEM) errno = EINVAL;
            return -1;
        }
    } else {
        txt = json_obj_strdup(&jo, spec);
    }
    if (!txt || !txt[0]) { free(txt); return 0; }

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
/* Both the singular (cmd_add_index) and plural (cmd_add_indexes) entry
   points funnel every field type — btree, bitmap, trigram — requested in
   ONE add-index call through the same single-scan engine reindex_object
   uses (build_indexes_streaming_multi -> seg_seq_build_spills +
   resolve_bitmaps). A force add-index over N fields of mixed type does
   exactly one sequential pass over storage, not one pass per type. */

typedef enum {
    INDEX_BUILD_OK = 0,
    INDEX_BUILD_DURABILITY_UNCONFIRMED,
    INDEX_BUILD_FAILED,
} index_build_status;

typedef struct {
    index_build_status status;
    int all_requested_shards_published;
    int error_errno;
} index_build_result;

static index_build_result index_build_ok(void) {
    return (index_build_result){
        .status = INDEX_BUILD_OK,
        .all_requested_shards_published = 1,
        .error_errno = 0,
    };
}

static index_build_result index_build_failed(int error_errno) {
    return (index_build_result){
        .status = INDEX_BUILD_FAILED,
        .all_requested_shards_published = 0,
        .error_errno = error_errno ? error_errno : EIO,
    };
}

static index_build_result
index_build_durability_unconfirmed(int error_errno) {
    return (index_build_result){
        .status = INDEX_BUILD_DURABILITY_UNCONFIRMED,
        .all_requested_shards_published = 1,
        .error_errno = error_errno ? error_errno : EIO,
    };
}

static index_build_result index_build_result_combine(index_build_result left,
                                                      index_build_result right) {
    index_build_result result = left;
    if (right.status > result.status) {
        result.status = right.status;
        result.error_errno = right.error_errno;
    } else if (!result.error_errno && right.error_errno) {
        result.error_errno = right.error_errno;
    }
    result.all_requested_shards_published =
        left.all_requested_shards_published &&
        right.all_requested_shards_published;
    return result;
}

enum { INDEX_CONF_MAX_BYTES = 1024 * 1024 };

static int index_write_all(int fd, const char *buf, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, buf + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) { errno = EIO; return -1; }
        off += (size_t)n;
    }
    return 0;
}

/* Read the complete prior metadata so both add-index entry points publish one
   replacement rather than mutating index.conf in place. ENOENT is an empty
   metadata file; every other error is a command failure. */
static int index_conf_read(const char *path, char **out, size_t *out_len) {
    *out = NULL;
    *out_len = 0;
    int fd = open(path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 0 : -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size < 0 ||
        st.st_size > INDEX_CONF_MAX_BYTES) {
        int saved_errno = errno ? errno : EFBIG;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    size_t len = (size_t)st.st_size;
    char *contents = calloc(len + 1, 1);
    if (!contents) { close(fd); return -1; }
    size_t off = 0;
    while (off < len) {
        ssize_t n = read(fd, contents + off, len - off);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) {
            int saved_errno = n == 0 ? EIO : errno;
            free(contents); close(fd); errno = saved_errno;
            return -1;
        }
        off += (size_t)n;
    }
    if (close(fd) != 0) { int saved_errno = errno; free(contents); errno = saved_errno; return -1; }
    *out = contents;
    *out_len = len;
    return 0;
}

static int index_conf_has_line(const char *contents, size_t contents_len,
                               const char *line) {
    size_t line_len = strlen(line);
    const char *p = contents;
    const char *end = contents + contents_len;
    while (p < end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        const char *line_end = nl ? nl : end;
        if ((size_t)(line_end - p) == line_len &&
            memcmp(p, line, line_len) == 0)
            return 1;
        p = nl ? nl + 1 : end;
    }
    return 0;
}

/* A bare bool/enum index line is legacy spelling for the bitmap index that
   current add-index commands materialise.  Replacing it, rather than merely
   appending the typed spelling, keeps retry idempotent and gives reindex one
   unambiguous descriptor to consume. */
static int index_conf_replaces_legacy_bare(const char *canonical,
                                           const char *line, size_t line_len) {
    const char *colon = strchr(canonical, ':');
    if (!colon || strncmp(colon, ":bitmap", 7) != 0) return 0;
    return (size_t)(colon - canonical) == line_len &&
           memcmp(canonical, line, line_len) == 0;
}

#ifdef TEST_BUILD
static _Atomic int g_index_spill_open_fail_errno;
static _Atomic int g_index_conf_publish_fail_stage;

void index_test_spill_open_fail_errno(int err) {
    atomic_store_explicit(&g_index_spill_open_fail_errno, err,
                          memory_order_release);
}

void index_test_conf_publish_fail_stage(int stage) {
    atomic_store_explicit(&g_index_conf_publish_fail_stage, stage,
                          memory_order_release);
}

static int index_test_take_conf_publish_failure(int stage) {
    int expected = stage;
    return atomic_compare_exchange_strong_explicit(
        &g_index_conf_publish_fail_stage, &expected, 0,
        memory_order_acq_rel, memory_order_acquire);
}
#endif

static index_build_result publish_index_conf(const char *conf_path,
                                             const char *contents,
                                             size_t contents_len) {
    char parent[PATH_MAX];
    char tmp_path[PATH_MAX];
    if (parent_dir_copy(conf_path, parent, sizeof(parent)) != 0)
        return index_build_failed(errno);
    mkdirp(parent);
    int n = snprintf(tmp_path, sizeof(tmp_path), "%s/.index-conf-XXXXXX", parent);
    if (n < 0 || n >= (int)sizeof(tmp_path))
        return index_build_failed(ENAMETOOLONG);
#ifdef TEST_BUILD
    if (index_test_take_conf_publish_failure(1)) {
        return index_build_failed(EIO);
    }
#endif
    int fd = mkstemp(tmp_path);
    if (fd < 0) return index_build_failed(errno);
    int saved_errno = 0;
#ifdef TEST_BUILD
    if (index_test_take_conf_publish_failure(2))
        saved_errno = EIO;
#endif
    if (!saved_errno && index_write_all(fd, contents, contents_len) != 0)
        saved_errno = errno;
#ifdef TEST_BUILD
    if (!saved_errno && index_test_take_conf_publish_failure(3))
        saved_errno = EIO;
#endif
    if (!saved_errno && durability_fsync(fd) != 0) saved_errno = errno;
    int close_rc = close(fd);
    if (!saved_errno && close_rc != 0) saved_errno = errno;
#ifdef TEST_BUILD
    if (!saved_errno && index_test_take_conf_publish_failure(4))
        saved_errno = EIO;
#endif
    if (saved_errno) {
        (void)unlink(tmp_path);
        return index_build_failed(saved_errno);
    }
#ifdef TEST_BUILD
    if (index_test_take_conf_publish_failure(5)) {
        (void)unlink(tmp_path);
        return index_build_failed(EIO);
    }
#endif
    if (rename(tmp_path, conf_path) != 0) {
        saved_errno = errno;
        (void)unlink(tmp_path);
        return index_build_failed(saved_errno);
    }
#ifdef TEST_BUILD
    if (index_test_take_conf_publish_failure(6)) {
        return index_build_durability_unconfirmed(EIO);
    }
#endif
    if (fsync_parent_dir(conf_path) != 0)
        return index_build_durability_unconfirmed(errno);
    return index_build_ok();
}

static index_build_result index_conf_append_unique(const char *conf_path,
                                                   const char *const *lines,
                                                   int n_lines) {
    char *old = NULL;
    size_t old_len = 0;
    if (index_conf_read(conf_path, &old, &old_len) != 0)
        return index_build_failed(errno);
    size_t required = old_len + (old_len && old[old_len - 1] != '\n' ? 1 : 0);
    for (int i = 0; i < n_lines; i++)
        if (!index_conf_has_line(old ? old : "", old_len, lines[i]))
            required += strlen(lines[i]) + 1;
    if (required > INDEX_CONF_MAX_BYTES) { free(old); return index_build_failed(EFBIG); }
    char *next = calloc(required + 1, 1);
    if (!next) { free(old); return index_build_failed(errno); }
    size_t used = 0;
    const char *p = old ? old : "";
    const char *end = p + old_len;
    while (p < end) {
        const char *nl = memchr(p, '\n', (size_t)(end - p));
        const char *line_end = nl ? nl : end;
        int replaced = 0;
        for (int i = 0; i < n_lines; i++) {
            if (index_conf_replaces_legacy_bare(lines[i], p,
                                                (size_t)(line_end - p))) {
                replaced = 1;
                break;
            }
        }
        if (!replaced) {
            size_t kept = (size_t)(line_end - p);
            memcpy(next + used, p, kept);
            used += kept;
            if (nl) next[used++] = '\n';
        }
        p = nl ? nl + 1 : end;
    }
    if (used && next[used - 1] != '\n') next[used++] = '\n';
    for (int i = 0; i < n_lines; i++) {
        if (index_conf_has_line(old ? old : "", old_len, lines[i])) continue;
        size_t line_len = strlen(lines[i]);
        memcpy(next + used, lines[i], line_len);
        used += line_len;
        next[used++] = '\n';
    }
    index_build_result result = publish_index_conf(conf_path, next, used);
    free(next);
    free(old);
    return result;
}

static int index_sweep_generated_files(int dirfd, const char *prefix,
                                       int descend_fields) {
    /* dup() shares the directory stream offset with dirfd. The caller runs
       two passes over indexes/ (metadata siblings, then field directories),
       so open an independent description to keep the second pass at offset 0. */
    int scanfd = openat(dirfd, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (scanfd < 0) return -1;
    DIR *dir = fdopendir(scanfd);
    if (!dir) { int saved_errno = errno; close(scanfd); errno = saved_errno; return -1; }
    int result = 0;
    for (;;) {
        errno = 0;
        struct dirent *entry = readdir(dir);
        if (!entry) {
            if (errno != 0 && !result) result = errno;
            break;
        }
        if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")) continue;
        struct stat st;
        if (fstatat(dirfd, entry->d_name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
            if (errno != ENOENT && !result) result = errno;
            continue;
        }
        if (descend_fields) {
            if (!S_ISDIR(st.st_mode)) continue;
            int fieldfd = openat(dirfd, entry->d_name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
            if (fieldfd < 0) { if (errno != ENOENT && !result) result = errno; continue; }
            if (index_sweep_generated_files(fieldfd, ".rebuild-", 0) != 0 && !result)
                result = errno;
            if (close(fieldfd) != 0 && !result) result = errno;
            continue;
        }
        if (!S_ISREG(st.st_mode) || strncmp(entry->d_name, prefix, strlen(prefix)) != 0)
            continue;
        if (unlinkat(dirfd, entry->d_name, 0) != 0 && errno != ENOENT && !result)
            result = errno;
    }
    if (closedir(dir) != 0 && !result) result = errno;
    if (result) { errno = result; return -1; }
    return 0;
}

/* Sweep only paths described by schema.conf. All directory operations are
   anchored beneath db_root, and fstatat(..., AT_SYMLINK_NOFOLLOW) ensures a
   crash leftover can never turn startup cleanup into link traversal. */
int index_rebuild_temp_sweep(const char *db_root) {
    int rootfd = open(db_root, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
    if (rootfd < 0) return -1;
    int schemafd = openat(rootfd, "schema.conf", O_RDONLY | O_NOFOLLOW);
    if (schemafd < 0) {
        int saved_errno = errno;
        if (close(rootfd) != 0 && saved_errno == ENOENT) saved_errno = errno;
        if (saved_errno == ENOENT) return 0;
        errno = saved_errno;
        return -1;
    }
    FILE *schema = fdopen(schemafd, "r");
    if (!schema) { int saved_errno = errno; close(schemafd); close(rootfd); errno = saved_errno; return -1; }
    int result = 0;
    char line[2048];
    while (fgets(line, sizeof(line), schema)) {
        char *first = strchr(line, ':');
        if (!first) continue;
        *first++ = '\0';
        char *second = strchr(first, ':');
        if (!second || !line[0] || !first[0]) continue;
        *second = '\0';
        /* schema.conf is parsed before the normal startup validator runs.
           Reject non-components here so fd-relative traversal cannot escape
           DB_ROOT through an absolute or parent path. */
        if (!valid_filename(line) || !valid_filename(first)) continue;
        int tenantfd = openat(rootfd, line, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
        if (tenantfd < 0) { if (errno != ENOENT && !result) result = errno; continue; }
        int objectfd = openat(tenantfd, first, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
        int open_errno = errno;
        if (close(tenantfd) != 0 && objectfd >= 0) {
            if (!result) result = errno;
        }
        if (objectfd < 0) errno = open_errno;
        if (objectfd < 0) { if (errno != ENOENT && !result) result = errno; continue; }
        int indexesfd = openat(objectfd, "indexes", O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
        open_errno = errno;
        if (close(objectfd) != 0 && indexesfd >= 0) {
            if (!result) result = errno;
        }
        if (indexesfd < 0) errno = open_errno;
        if (indexesfd < 0) { if (errno != ENOENT && !result) result = errno; continue; }
        if (index_sweep_generated_files(indexesfd, ".index-conf-", 0) != 0 && !result)
            result = errno;
        if (index_sweep_generated_files(indexesfd, NULL, 1) != 0 && !result)
            result = errno;
        if (close(indexesfd) != 0 && !result) result = errno;
    }
    if (ferror(schema) && !result) result = errno ? errno : EIO;
    if (fclose(schema) != 0 && !result) result = errno;
    if (close(rootfd) != 0 && !result) result = errno;
    if (result) { errno = result; return -1; }
    return 0;
}

/* Forward decls — full definitions live near the multi-index builder. */
index_build_result build_bitmap_pass(const char *db_root, const char *object,
                      const Schema *sch, TypedSchema *ts,
                      const char *field, uint32_t max_values, int force);
index_build_result build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);

/* Btree build entry point — routes to the shared segment-sequential
   engine (seg_seq_build_spills). cmd_add_index calls this from its
   IT_BTREE branch. */
index_build_result build_btree_streaming(const char *db_root, const char *object,
                          const Schema *sch, TypedSchema *ts,
                          const char *field, int force);

static void index_canonical_line(char *out, size_t out_size,
                                 const char *name, enum IndexType type,
                                 uint32_t max_values) {
    if (type == IT_BITMAP) {
        if (max_values > 0 && max_values != BM_DEFAULT_MAX_VALUES)
            snprintf(out, out_size, "%s:bitmap(%u)", name, max_values);
        else
            snprintf(out, out_size, "%s:bitmap", name);
    } else if (type == IT_TRIGRAM) {
        snprintf(out, out_size, "%s:trigram", name);
    } else {
        snprintf(out, out_size, "%s", name);
    }
}

int cmd_add_index(const char *db_root, const char *object,
                         const char *field, int force) {
    uint64_t t_start = now_ms();
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
    index_canonical_line(canon, sizeof(canon), eff, type, max_values);

    Schema sch = load_schema(db_root, object);
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* Metadata is the activation record. A physical shard without its
       canonical line may be the residue of a partial first publication, so
       retry must rebuild the complete shard set before repairing metadata. */
    if (!force) {
        char *existing_conf = NULL;
        size_t existing_conf_len = 0;
        if (index_conf_read(conf_path, &existing_conf, &existing_conf_len) != 0) {
            OUT("{\"error\":\"cannot read index metadata: %s\"}\n",
                strerror(errno));
            return -1;
        }
        int already_active = index_conf_has_line(existing_conf ? existing_conf : "",
                                                 existing_conf_len, canon);
        free(existing_conf);
        if (already_active) {
            OUT("{\"status\":\"exists\",\"field\":\"%s\"}\n", field);
            return 0;
        }
    }

    TypedSchema *ts = load_typed_schema(db_root, object);
    if (!ts) {
        OUT("{\"error\":\"cannot load object schema for index build\"}\n");
        return -1;
    }

    index_build_result build_result;
    if (type == IT_BITMAP) {
        build_result = build_bitmap_pass(db_root, object, &sch, ts, eff,
                                         max_values, force);
    } else if (type == IT_TRIGRAM) {
        build_result = build_trigram_pass(db_root, object, &sch, ts, eff, force);
    } else {
        /* === btree build via streaming pipeline (bounded per-worker
           memory; safe at any dataset size). Same machinery as
           build_trigram_pass but for STREAM_BTREE. */
        build_result = build_btree_streaming(db_root, object, &sch, ts, eff, force);
    }

    if (build_result.status == INDEX_BUILD_FAILED) {
        OUT("{\"error\":\"index build failed for %s: %s; index metadata was not "
            "changed; one or more shards may already have been published\"}\n",
            field, strerror(build_result.error_errno ? build_result.error_errno : EIO));
        return -1;
    }

    const char *metadata_lines[] = { canon };
    index_build_result metadata_result = index_conf_append_unique(conf_path,
                                                                   metadata_lines, 1);
    if (metadata_result.status == INDEX_BUILD_FAILED) {
        OUT("{\"error\":\"index shards published but index metadata update failed; "
            "retry add-index\"}\n");
        return -1;
    }

    invalidate_idx_cache(db_root, object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    if (build_result.status == INDEX_BUILD_DURABILITY_UNCONFIRMED ||
        metadata_result.status == INDEX_BUILD_DURABILITY_UNCONFIRMED) {
        OUT("{\"warning\":\"index and metadata published but directory durability "
            "is unconfirmed\"}\n");
        return 1;
    }
    OUT("{\"status\":\"indexed\",\"field\":\"%s\",\"records\":%d,\"duration_ms\":%llu}\n",
        field, records, (unsigned long long)duration_ms);
    return 0;
}

/* Bitmap reindex pass — rebuilds every .bm shard for one field by
   walking live records in its already-locked kf shard via
   slotcask_walk_one_shard_slots_locked, encoding the field value
   (matching the encoding bitmap_update uses on the CRUD path), and
   bm_set'ing the bit at the record's kf slot. */
typedef struct {
    BitmapShard *bm;
    int          field_index;     /* typed schema field index */
    TypedSchema *ts;
    int          failed;
    int          saved_errno;
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
        uint16_t actual_len = ((uint16_t)vbase[0] << 8) | (uint16_t)vbase[1];
        if (actual_len == 0 || actual_len > sizeof(key_buf)) return 0;
        memcpy(key_buf, vbase + 2, actual_len);
        key_len = actual_len;
    } else {
        size_t sz = (size_t)f->size;
        if (sz == 0 || sz > sizeof(key_buf)) return 0;
        memcpy(key_buf, vbase, sz);
        key_len = sz;
    }
    if (bm_set(c->bm, key_buf, key_len, slot) != 0) {
        if (!c->failed) c->saved_errno = errno;
        c->failed = 1;
        return -1;
    }
    return 0;
}

/* === Streaming index build with external-sort merge =========================
 *
 * Bounded per-worker memory AND tight final-file leaf packing at any scale.
 * Used by trigram (always) and btree singular (cmd_add_index).
 *
 * Phase 1 — parallel kf-shard walks, per-worker bounded buffer.
 *   Each worker walks ONE kf shard. It maintains a fixed-size buffer.
 *   When the buffer fills it FLUSHES:
 *     - counting-sort current pairs by idx_shard_for_hash(record_hash, splits)
 *     - for each output_shard slice: qsort + append SORTED RUN to a per-
 *       (worker, output_shard) spill file
 *     - reset buffer
 *   At end of walk: final flush. Worker closes its spill writers.
 *
 * Phase 2 — per output shard, k-way merge spill runs into the final file.
 *   For each output shard (parallel across shards):
 *     - Enumerate every sorted run across every worker's spill file
 *     - Min-heap k-way merge the runs into a single sorted stream
 *     - Stream-insert into the empty target .tg/.idx in sorted-order
 *       batches → leaves fill left-to-right at 100% (same as bulk_build)
 *     - Delete consumed spill files
 *
 * Memory bound:
 *   Phase 1: pool_size × per_worker_buffer ≈ INDEX_BUILD_BUDGET_MB
 *   Phase 2: per-shard merge ≈ n_runs × small read buffer (a few MB)
 *
 * Disk bound:
 *   Spill files total ≈ final index size (one tight copy)
 *   Final files ≈ same (insertions in sorted order = 100% leaf fill)
 *   Peak ≈ 2× during merge transition; drops to 1× as spills deleted.
 *
 * Works at any scale — 25M, 100M, 1B — peak memory is constant. */

enum { STREAM_BTREE = 0, STREAM_TRIGRAM = 1 };

/* Spill writer — buffered append to one (worker, output_shard) file.
   Multiple sorted runs are concatenated in the file; each run is
   prefixed with (count, byte_len) so phase 2 readers can enumerate
   runs without parsing entries. */
#define SPILL_WRITE_BUF_BYTES 65536
typedef struct {
    int      fd;
    uint8_t *wbuf;
    size_t   wbuf_used;
} SpillWriter;

/* Per-run reader — owns no fd (caller holds shared fd, we use pread).
   Holds a peeked entry to support min-heap comparison. */
#define SPILL_READ_BUF_BYTES 4096
typedef struct {
    int       fd;              /* shared with other readers on same file */
    off_t     pos;             /* next pread offset within file */
    off_t     run_end;         /* last byte of THIS run (exclusive) */
    uint32_t  entries_remaining;
    uint8_t   buf[SPILL_READ_BUF_BYTES];
    size_t    buf_used;
    size_t    buf_off;
    /* Peeked entry. */
    uint8_t   value[256];
    size_t    vlen;
    uint8_t   hash[BT_HASH_SIZE];
    int       has_entry;
    int       eof;
} SpillRunReader;

/* === Spill file helpers ==================================================== */

static int spill_writer_open(SpillWriter *sw, const char *path) {
    sw->fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (sw->fd < 0) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_open: open failed for %s: %s", path, strerror(errno));
        return -1;
    }
    sw->wbuf = malloc(SPILL_WRITE_BUF_BYTES);
    if (!sw->wbuf) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_open: malloc(%d) failed for spill write buffer (%s)", SPILL_WRITE_BUF_BYTES, path);
        close(sw->fd); sw->fd = -1; return -1;
    }
    sw->wbuf_used = 0;
    return 0;
}

static int spill_writer_drain(SpillWriter *sw) {
    if (sw->wbuf_used == 0) return 0;
    const uint8_t *p = sw->wbuf;
    size_t left = sw->wbuf_used;
    while (left > 0) {
        ssize_t n = write(sw->fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_drain: write failed on fd %d: %s", sw->fd, strerror(errno));
            return -1;
        }
        if (n == 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_drain: write returned 0 (disk full?) on fd %d", sw->fd);
            return -1;
        }
        p += n; left -= (size_t)n;
    }
    sw->wbuf_used = 0;
    return 0;
}

static int spill_writer_put(SpillWriter *sw, const void *data, size_t len) {
    if (sw->wbuf_used + len > SPILL_WRITE_BUF_BYTES) {
        if (spill_writer_drain(sw) != 0) return -1;
        if (len > SPILL_WRITE_BUF_BYTES) {
            /* Direct write for oversize chunk. */
            const uint8_t *p = data; size_t left = len;
            while (left > 0) {
                ssize_t n = write(sw->fd, p, left);
                if (n < 0) {
                    if (errno == EINTR) continue;
                    LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_put: oversize direct write failed on fd %d (len=%zu): %s", sw->fd, len, strerror(errno));
                    return -1;
                }
                if (n == 0) {
                    LOG_ERROR(LOG_SUB_REINDEX, "spill_writer_put: oversize direct write returned 0 on fd %d (len=%zu)", sw->fd, len);
                    return -1;
                }
                p += n; left -= (size_t)n;
            }
            return 0;
        }
    }
    memcpy(sw->wbuf + sw->wbuf_used, data, len);
    sw->wbuf_used += len;
    return 0;
}

/* Append one sorted run (count entries) to the spill file. Run header
   carries entry_count + body byte length so phase 2 can enumerate
   runs without parsing them. */
static int spill_writer_write_run(SpillWriter *sw, BtEntry *entries, uint32_t count) {
    /* Compute body length. */
    uint64_t body = 0;
    for (uint32_t i = 0; i < count; i++) {
        body += (uint64_t)sizeof(uint16_t) + (uint64_t)entries[i].vlen +
                (uint64_t)BT_HASH_SIZE;
    }
    if (body > 0xFFFFFFFFULL) {
        LOG_WARN(LOG_SUB_REINDEX, "spill_writer_write_run: run body size %llu exceeds 4GB cap (count=%u); dropping run", (unsigned long long)body, count);
        return -1;  /* shouldn't happen with sane buffer caps */
    }
    uint32_t body_u32 = (uint32_t)body;

    if (spill_writer_put(sw, &count,    sizeof(count))    != 0) return -1;
    if (spill_writer_put(sw, &body_u32, sizeof(body_u32)) != 0) return -1;

    for (uint32_t i = 0; i < count; i++) {
        uint16_t vlen = (uint16_t)entries[i].vlen;
        if (spill_writer_put(sw, &vlen,            sizeof(vlen)) != 0) return -1;
        if (spill_writer_put(sw, entries[i].value, vlen)         != 0) return -1;
        if (spill_writer_put(sw, entries[i].hash,  BT_HASH_SIZE) != 0) return -1;
    }
    return 0;
}

static void spill_writer_close(SpillWriter *sw) {
    if (sw->wbuf_used > 0) spill_writer_drain(sw);
    if (sw->fd >= 0) close(sw->fd);
    free(sw->wbuf);
    sw->wbuf = NULL;
    sw->fd = -1;
    sw->wbuf_used = 0;
}

/* === Spill run reader ====================================================== */

static int spill_run_fill(SpillRunReader *r) {
    if (r->buf_off < r->buf_used) return 0;       /* still data buffered */
    if (r->pos >= r->run_end) { r->eof = 1; return 0; }
    size_t want = SPILL_READ_BUF_BYTES;
    if ((off_t)want > r->run_end - r->pos) want = (size_t)(r->run_end - r->pos);
    ssize_t n = pread(r->fd, r->buf, want, r->pos);
    if (n <= 0) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_run_fill: pread failed on fd %d at offset %lld (want=%zu): %s", r->fd, (long long)r->pos, want, strerror(errno));
        r->eof = 1; return -1;
    }
    r->pos += n;
    r->buf_used = (size_t)n;
    r->buf_off  = 0;
    return 0;
}

static int spill_run_take(SpillRunReader *r, void *out, size_t len) {
    uint8_t *o = out;
    while (len > 0) {
        if (r->buf_off >= r->buf_used) {
            if (spill_run_fill(r) != 0) return -1;
            if (r->buf_off >= r->buf_used) {
                LOG_ERROR(LOG_SUB_REINDEX, "spill_run_take: unexpected EOF mid-entry on fd %d (wanted %zu more bytes)", r->fd, len);
                return -1;  /* eof mid-entry */
            }
        }
        size_t avail = r->buf_used - r->buf_off;
        size_t take  = len < avail ? len : avail;
        memcpy(o, r->buf + r->buf_off, take);
        r->buf_off += take;
        o += take;
        len -= take;
    }
    return 0;
}

/* Read the next (vlen|value|hash) into r->value/vlen/hash; sets
   has_entry. Returns 0 on success, -1 on read error. EOF leaves
   r->eof=1 and has_entry=0. */
static int spill_run_advance(SpillRunReader *r) {
    if (r->entries_remaining == 0) { r->has_entry = 0; r->eof = 1; return 0; }
    uint16_t vlen;
    if (spill_run_take(r, &vlen, sizeof(vlen)) != 0) { r->has_entry = 0; return -1; }
    if (vlen > sizeof(r->value)) {
        LOG_ERROR(LOG_SUB_REINDEX, "spill_run_advance: corrupt spill entry on fd %d: vlen=%u exceeds buffer size %zu", r->fd, vlen, sizeof(r->value));
        r->has_entry = 0; return -1;
    }
    if (spill_run_take(r, r->value, vlen)           != 0) { r->has_entry = 0; return -1; }
    if (spill_run_take(r, r->hash,  BT_HASH_SIZE)   != 0) { r->has_entry = 0; return -1; }
    r->vlen = vlen;
    r->has_entry = 1;
    r->entries_remaining--;
    return 0;
}

static int spill_run_reader_init(SpillRunReader *r, int fd, off_t body_start,
                                 off_t body_end, uint32_t entry_count) {
    memset(r, 0, sizeof(*r));
    r->fd = fd;
    r->pos = body_start;
    r->run_end = body_end;
    r->entries_remaining = entry_count;
    return spill_run_advance(r);
}

/* Entry compare via (value, hash) lexicographic. Mirrors cmp_btentry_fn
   on BtEntry — keeps the on-disk order matching what btree expects. */
static int spill_entry_parts_cmp(const uint8_t *a_value, size_t a_vlen,
                                 const uint8_t a_hash[BT_HASH_SIZE],
                                 const uint8_t *b_value, size_t b_vlen,
                                 const uint8_t b_hash[BT_HASH_SIZE]) {
    size_t m = a_vlen < b_vlen ? a_vlen : b_vlen;
    int c = memcmp(a_value, b_value, m);
    if (c != 0) return c;
    if (a_vlen != b_vlen) return a_vlen < b_vlen ? -1 : 1;
    return memcmp(a_hash, b_hash, BT_HASH_SIZE);
}

static int spill_entry_cmp(const SpillRunReader *a, const SpillRunReader *b) {
    return spill_entry_parts_cmp(a->value, a->vlen, a->hash,
                                 b->value, b->vlen, b->hash);
}

/* === Min-heap over SpillRunReader indices for k-way merge ================== */
typedef struct {
    int            *idx;       /* heap of reader indices */
    int             size;
    int             cap;
    SpillRunReader *readers;   /* shared external array */
} MinHeap;

static void mh_swap(MinHeap *h, int i, int j) {
    int t = h->idx[i]; h->idx[i] = h->idx[j]; h->idx[j] = t;
}
static int mh_less(MinHeap *h, int i, int j) {
    return spill_entry_cmp(&h->readers[h->idx[i]], &h->readers[h->idx[j]]) < 0;
}
static void mh_sift_up(MinHeap *h, int i) {
    while (i > 0) {
        int p = (i - 1) / 2;
        if (mh_less(h, i, p)) { mh_swap(h, i, p); i = p; }
        else break;
    }
}
static void mh_sift_down(MinHeap *h, int i) {
    for (;;) {
        int l = 2*i + 1, r = 2*i + 2, m = i;
        if (l < h->size && mh_less(h, l, m)) m = l;
        if (r < h->size && mh_less(h, r, m)) m = r;
        if (m == i) break;
        mh_swap(h, i, m);
        i = m;
    }
}
static void mh_push(MinHeap *h, int reader_idx) {
    h->idx[h->size++] = reader_idx;
    mh_sift_up(h, h->size - 1);
}
/* Advance the top reader to its next entry; either re-sift or pop.
   A spill read/decode error returns -1 and prevents publication. */
static int mh_advance_top(MinHeap *h) {
    SpillRunReader *r = &h->readers[h->idx[0]];
    uint8_t previous_value[sizeof(r->value)];
    uint8_t previous_hash[BT_HASH_SIZE];
    size_t previous_vlen = r->vlen;
    memcpy(previous_value, r->value, previous_vlen);
    memcpy(previous_hash, r->hash, sizeof(previous_hash));
    if (spill_run_advance(r) != 0) return -1;
    if (r->has_entry &&
        spill_entry_parts_cmp(previous_value, previous_vlen, previous_hash,
                              r->value, r->vlen, r->hash) > 0) {
        errno = EINVAL;
        return -1;
    }
    if (!r->has_entry) {
        if (--h->size == 0) return 0;
        h->idx[0] = h->idx[h->size];
    }
    if (h->size > 0) mh_sift_down(h, 0);
    return 0;
}

static index_build_result index_result_from_bt_publish(bt_publish_result r) {
    switch (r) {
        case BT_PUBLISH_OK:
            return index_build_ok();
        case BT_PUBLISH_POST_RENAME_FSYNC_FAILED:
            return index_build_durability_unconfirmed(0);
        default:
            return index_build_failed(0);
    }
}

/* Phase-2 spill-file open wrapper. Phase 1 opens every (worker, shard)
   spill file with O_CREAT up front, so after a clean scan every file
   exists — a missing one is anomalous (an aborted worker) rather than
   normal. Any non-ENOENT open failure is fail-closed: skipping it would
   silently drop that worker's entries from the rebuilt index. */
static int index_spill_open(const char *path) {
#ifdef TEST_BUILD
    /* Errcode injection for the fail-closed regression. Stays armed until
       explicitly reset rather than firing once: the per-shard merge fan-out
       would otherwise publish sibling shards before the injected shard
       fails, making the "no target changed" assertion racy. Test cleanup
       resets it so sequential run-all --jobs 1 stays isolated. */
    int injected_errno = atomic_load_explicit(&g_index_spill_open_fail_errno,
                                              memory_order_acquire);
    if (injected_errno != 0) {
        errno = injected_errno;
        return -1;
    }
#endif
    return open(path, O_RDONLY);
}

/* Validate one complete spill file without publishing any output. This
   preflight runs across every requested btree/trigram field before phase 2
   starts, so a malformed later shard cannot race with publication of an
   earlier sibling. */
static int validate_index_spill_file(const char *path) {
    int fd = index_spill_open(path);
    if (fd < 0) return errno == ENOENT ? 0 : -1;
    struct stat st;
    if (fstat(fd, &st) != 0) {
        int saved_errno = errno;
        close(fd);
        errno = saved_errno;
        return -1;
    }
    off_t end = st.st_size;
    off_t pos = 0;
    while (pos < end) {
        if (end - pos < 8) {
            close(fd);
            errno = EINVAL;
            return -1;
        }
        uint32_t count = 0, body = 0;
        if (pread(fd, &count, sizeof(count), pos) != sizeof(count) ||
            pread(fd, &body, sizeof(body), pos + 4) != sizeof(body)) {
            int saved_errno = errno ? errno : EIO;
            close(fd);
            errno = saved_errno;
            return -1;
        }
        off_t body_start = pos + 8;
        if ((uint64_t)body > (uint64_t)(end - body_start)) {
            close(fd);
            errno = EINVAL;
            return -1;
        }
        off_t body_end = body_start + (off_t)body;
        if (count == 0) {
            if (body != 0) {
                close(fd);
                errno = EINVAL;
                return -1;
            }
        } else {
            SpillRunReader reader;
            if (spill_run_reader_init(&reader, fd, body_start, body_end,
                                      count) != 0 || !reader.has_entry) {
                close(fd);
                errno = EINVAL;
                return -1;
            }
            while (reader.entries_remaining > 0) {
                uint8_t previous_value[sizeof(reader.value)];
                uint8_t previous_hash[BT_HASH_SIZE];
                size_t previous_vlen = reader.vlen;
                memcpy(previous_value, reader.value, previous_vlen);
                memcpy(previous_hash, reader.hash, sizeof(previous_hash));
                if (spill_run_advance(&reader) != 0 || !reader.has_entry) {
                    close(fd);
                    errno = EINVAL;
                    return -1;
                }
                if (spill_entry_parts_cmp(
                        previous_value, previous_vlen, previous_hash,
                        reader.value, reader.vlen, reader.hash) > 0) {
                    close(fd);
                    errno = EINVAL;
                    return -1;
                }
            }
            off_t consumed = reader.pos -
                             (off_t)(reader.buf_used - reader.buf_off);
            if (consumed != body_end) {
                close(fd);
                errno = EINVAL;
                return -1;
            }
        }
        pos = body_end;
    }
    if (close(fd) != 0) return -1;
    return 0;
}

static int validate_index_spills(const char *spill_dir,
                                 int workers, int shards) {
    for (int w = 0; w < workers; w++) {
        for (int s = 0; s < shards; s++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w, s);
            if (validate_index_spill_file(path) != 0) return -1;
        }
    }
    return 0;
}

/* Phase 2 — merge all per-worker spill files for one output shard into
   the final .tg/.idx via k-way merge. Output sorted insertions →
   leaves fill at 100%. Deletes spill files as it goes. Always builds and
   publishes a valid tree (even an empty one) unless a fatal error occurs;
   a fatal error aborts the temporary build and leaves the existing target
   untouched. */
static index_build_result merge_spills_into_index(int type,
                                   const char *db_root, const char *object,
                                   const char *field,
                                   int idx_n, int n_kf, int shard,
                                   const char *spill_dir) {
    (void)idx_n;
    index_build_result result = index_build_ok();

    /* Open every worker's spill file for this output shard. Files may
       not exist if a worker had no entries for this shard — skip those. */
    int *fds = calloc((size_t)n_kf, sizeof(int));
    if (!fds) {
        LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: calloc failed for %d fd slots (%s/%s/%s shard %d)", n_kf, db_root, object, field, shard);
        return index_build_failed(0);
    }
    for (int w = 0; w < n_kf; w++) fds[w] = -1;

    /* Enumerate runs across all spill files. */
    size_t reader_cap = 256, reader_count = 0;
    SpillRunReader *readers = malloc(reader_cap * sizeof(SpillRunReader));
    if (!readers) {
        LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: malloc failed for %zu spill run readers (%s/%s/%s shard %d)", reader_cap, db_root, object, field, shard);
        free(fds);
        return index_build_failed(0);
    }

    for (int w = 0; w < n_kf; w++) {
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w, shard);
        int fd = index_spill_open(path);
        if (fd < 0) {
            int saved_errno = errno;
            if (saved_errno == ENOENT) continue;
            LOG_ERROR(LOG_SUB_REINDEX,
                      "merge_spills_into_index: open(%s) failed: %s",
                      path, strerror(saved_errno));
            result = (index_build_result){
                .status = INDEX_BUILD_FAILED,
                .all_requested_shards_published = 0,
                .error_errno = saved_errno,
            };
            goto cleanup;
        }
        fds[w] = fd;

        struct stat st;
        if (fstat(fd, &st) != 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: fstat(%s) failed: %s", path, strerror(errno));
            result = index_build_failed(0);
            goto cleanup;
        }
        off_t end = st.st_size, pos = 0;
        while (pos + 8 <= end) {
            uint32_t count = 0, body = 0;
            if (pread(fd, &count, sizeof(count), pos)     != sizeof(count) ||
                pread(fd, &body,  sizeof(body),  pos + 4) != sizeof(body)) {
                LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: malformed run header at offset %lld in %s", (long long)pos, path);
                result = index_build_failed(0);
                goto cleanup;
            }
            off_t body_start = pos + 8;
            off_t body_end   = body_start + body;
            if (body_end > end) {
                LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: run body at offset %lld in %s extends beyond EOF", (long long)pos, path);
                result = index_build_failed(0);
                goto cleanup;
            }

            if (count > 0) {
                if (reader_count >= reader_cap) {
                    reader_cap *= 2;
                    SpillRunReader *t = realloc(readers, reader_cap * sizeof(SpillRunReader));
                    if (!t) {
                        result = index_build_failed(0);
                        goto cleanup;
                    }
                    readers = t;
                }
                if (spill_run_reader_init(&readers[reader_count], fd, body_start,
                                          body_end, count) != 0 ||
                    !readers[reader_count].has_entry) {
                    LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: malformed run body at offset %lld in %s", (long long)pos, path);
                    result = index_build_failed(0);
                    goto cleanup;
                }
                reader_count++;
            }
            pos = body_end;
        }
        if (pos != end) {
            LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: %s has %lld trailing bytes too short for a run header", path, (long long)(end - pos));
            result = index_build_failed(0);
            goto cleanup;
        }
    }

    /* Build target file path. */
    char target[PATH_MAX];
    if (type == STREAM_TRIGRAM)
        tg_build_path(target, sizeof(target), db_root, object, field, shard);
    else
        build_idx_path(target, sizeof(target), db_root, object, field, shard);

    /* Build heap. reader_count may legitimately be 0 (no entries for this
       shard anywhere) — the stream builder below still opens, finishes,
       and publishes a valid empty tree in that case. */
    MinHeap heap = { .readers = readers, .cap = (int)reader_count, .size = 0 };
    heap.idx = NULL;
    if (reader_count > 0) {
        heap.idx = malloc(reader_count * sizeof(int));
        if (!heap.idx) {
            result = index_build_failed(0);
            goto cleanup;
        }
        for (size_t i = 0; i < reader_count; i++) mh_push(&heap, (int)i);
    }

    /* Stream the merged sorted output directly into a btree_stream
       builder — no in-memory materialisation of the per-shard data.
       Memory per-shard merge is now O(spill_read_buffers + leaf_buffer)
       — a few MB regardless of how many entries flow through. Safe at
       any scale; phase 2 concurrency cap below is now effectively
       just bounded by pool_size. */
    BtStreamBuilder *builder = bt_stream_build_open(target);
    if (!builder) {
        free(heap.idx);
        result = index_build_failed(0);
        goto cleanup;
    }

    while (heap.size > 0) {
        SpillRunReader *r = &readers[heap.idx[0]];
        if (bt_stream_build_add(builder, (const char *)r->value, r->vlen,
                                r->hash) != 0) {
            LOG_ERROR(LOG_SUB_REINDEX,
                      "merge_spills_into_index: btree build failed for %s/%s/%s shard %d",
                      db_root, object, field, shard);
            bt_stream_build_abort(builder);
            result = index_build_failed(0);
            break;
        }
        if (mh_advance_top(&heap) != 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "merge_spills_into_index: spill read error mid-merge for %s/%s/%s shard %d — aborting build", db_root, object, field, shard);
            bt_stream_build_abort(builder);
            break;
        }
    }

    bt_publish_result publish = bt_stream_build_finish(builder);
    if (result.status != INDEX_BUILD_FAILED)
        result = index_result_from_bt_publish(publish);
    free(heap.idx);

cleanup:
    free(readers);
    for (int w = 0; w < n_kf; w++) {
        if (fds[w] >= 0) close(fds[w]);
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w, shard);
        unlink(path);
    }
    free(fds);
    return result;
}

/* Merge wrapper for parallel_for over output shards. */
typedef struct {
    int         type;
    const char *db_root;
    const char *object;
    const char *field;
    int         idx_n;
    int         n_kf;
    int         shard;
    const char *spill_dir;
    index_build_result rc;
} MergeShardArg;
static void *merge_shard_worker_fn(void *arg) {
    MergeShardArg *m = (MergeShardArg *)arg;
    m->rc = merge_spills_into_index(m->type, m->db_root, m->object, m->field,
                                    m->idx_n, m->n_kf, m->shard, m->spill_dir);
    return NULL;
}

/* Sentinel distinguishing bitmap from STREAM_BTREE / STREAM_TRIGRAM. */
#define MF_BITMAP 99

/* Descriptor for one field to be indexed — read-only, shared across workers. */
typedef struct {
    int      type;                  /* STREAM_BTREE, STREAM_TRIGRAM, or MF_BITMAP */
    char     name[256];             /* bare field name (dir name under indexes/) */
    int      field_indices[16];
    int      field_index_count;
    int      is_composite;
    uint32_t bm_max_values;
    int      bm_bool_fastpath;
} MFFieldDesc;

/* Segment-sequential btree/trigram builder — defined further down; both
   add-index (single field) and reindex (multi field) funnel through it so
   there is exactly one scan code path. */
static index_build_result seg_seq_build_spills(const char *db_root, const char *object,
                                const Schema *sch, TypedSchema *ts,
                                SlotcaskDb *sdb,
                                const MFFieldDesc *descs, int n_fields);

/* Forward decl — full definition lives further down, after
   seg_seq_build_spills/resolve_bitmaps. cmd_add_indexes calls this to
   build every requested field (btree+bitmap+trigram) in one scan, same
   engine reindex_object uses. */
static index_build_result build_indexes_streaming_multi(const char *db_root, const char *object,
                                          const Schema *sch, TypedSchema *ts,
                                          const MFFieldDesc *descs, int n_fields);

index_build_result build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);
index_build_result build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force) {
    if (!ts) return index_build_failed(0);
    int fi = typed_field_index(ts, field);
    if (fi < 0) return index_build_failed(0);

    /* Force builds complete replacement shards and publishes them atomically
       over the existing generation. Non-force skip was handled above. */
    (void)force;

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_TRIGRAM, "build_trigram_pass: slotcask_registry_get failed for %s/%s", db_root, object);
        return index_build_failed(0);
    }

    LOG_WARN(LOG_SUB_TRIGRAM, "BUILD-TRIGRAM %s/%s/%s: segment-sequential scan",
            db_root, object, field);

    MFFieldDesc d;
    memset(&d, 0, sizeof(d));
    d.type = STREAM_TRIGRAM;
    strncpy(d.name, field, sizeof(d.name) - 1);
    d.field_indices[0] = fi;
    d.field_index_count = 1;
    return seg_seq_build_spills(db_root, object, sch, ts, sdb, &d, 1);
}

/* Streaming btree build — bounded per-worker memory, safe at any
   dataset size. Mirror of build_trigram_pass but with STREAM_BTREE
   and composite-field handling. Called from cmd_add_index's IT_BTREE
   branch (and could be lifted into cmd_add_indexes / reindex later). */
index_build_result build_btree_streaming(const char *db_root, const char *object,
                          const Schema *sch, TypedSchema *ts,
                          const char *field, int force) {
    if (!ts) return index_build_failed(0);
    int idx_n = index_splits_for(sch->splits);

    (void)force;
    (void)idx_n;

    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_BTREE, "build_btree_streaming: slotcask_registry_get failed for %s/%s/%s", db_root, object, field);
        return index_build_failed(0);
    }

    MFFieldDesc d;
    memset(&d, 0, sizeof(d));
    d.type = STREAM_BTREE;
    strncpy(d.name, field, sizeof(d.name) - 1);
    d.is_composite = (strchr(field, '+') != NULL);
    if (d.is_composite) {
        char fbuf[256]; strncpy(fbuf, field, 255); fbuf[255] = '\0';
        char *save = NULL;
        for (char *t = strtok_r(fbuf, "+", &save);
             t && d.field_index_count < 16;
             t = strtok_r(NULL, "+", &save)) {
            d.field_indices[d.field_index_count++] = typed_field_index(ts, t);
        }
    } else {
        d.field_indices[0] = typed_field_index(ts, field);
        d.field_index_count = 1;
    }
    if (d.field_index_count == 0 || d.field_indices[0] < 0)
        return index_build_failed(0);

    LOG_WARN(LOG_SUB_BTREE, "BUILD-BTREE %s/%s/%s: segment-sequential scan",
            db_root, object, field);
    return seg_seq_build_spills(db_root, object, sch, ts, sdb, &d, 1);
}

/* Per-kf-shard worker for parallel bitmap rebuild. Each worker handles
   one (kf_shard, .bm) pair: acquire the kf reader first, then open its
   .bm writer and walk the already-locked kf shard. Distinct workers use
   distinct files; the cache-entry locks protect mapping lifetime. */
typedef struct {
    char         target_path[PATH_MAX];
    char         tmp_path[PATH_MAX];
    int          kf_shard;
    int          slots_per_shard;
    int          fi;
    int          bool_fastpath;
    uint32_t     max_values;
    TypedSchema *ts;
    SlotcaskDb  *sdb;
    index_build_result result;
    int          saved_errno;
    BmRebuildCtx rebuild;
} BmShardWalkArg;

static int bm_rebuild_temp_path(const char *target, char out[PATH_MAX]) {
    char parent[PATH_MAX];
    if (parent_dir_copy(target, parent, sizeof(parent)) != 0) return -1;
    int n = snprintf(out, PATH_MAX, "%s/.rebuild-XXXXXX", parent);
    if (n < 0 || n >= PATH_MAX) { errno = ENAMETOOLONG; return -1; }
    int fd = mkstemp(out);
    if (fd < 0) return -1;
    if (close(fd) != 0) {
        int saved_errno = errno;
        unlink(out);
        errno = saved_errno;
        return -1;
    }
    if (unlink(out) != 0) return -1;
    return 0;
}

static void *bm_shard_walk_worker(void *arg) {
    BmShardWalkArg *a = (BmShardWalkArg *)arg;
    a->result = index_build_failed(0);
    a->rebuild = (BmRebuildCtx){ 0 };

    char kf_path[PATH_MAX];
    slotcask_kf_path(kf_path, sizeof(kf_path),
                     a->sdb->data_dir, a->kf_shard);
    SlotcaskKfHandle kh;
    if (kfcache_acquire(&kh, kf_path,
                        a->sdb->slots_per_shard, 0) != 0) {
        a->saved_errno = errno;
        LOG_ERROR(LOG_SUB_BITMAP,
                  "bm_shard_walk_worker: kfcache_acquire failed for %s",
                  kf_path);
        return NULL;
    }

    if (bm_rebuild_temp_path(a->target_path, a->tmp_path) != 0) {
        a->saved_errno = errno;
        LOG_ERROR(LOG_SUB_BITMAP,
                  "bm_shard_walk_worker: temp path failed for %s: %s",
                  a->target_path, strerror(errno));
        kfcache_release(&kh);
        return NULL;
    }

    BitmapShard *bm = bm_open(a->tmp_path, a->slots_per_shard, 1,
                              a->bool_fastpath, a->max_values,
                              1 /* writer: reindex bm_set's */);
    if (!bm) {
        a->saved_errno = errno;
        LOG_ERROR(LOG_SUB_BITMAP,
                  "bm_shard_walk_worker: bm_open failed for temporary %s (kf_shard=%d): %s",
                  a->tmp_path, a->kf_shard, strerror(errno));
        unlink(a->tmp_path);
        kfcache_release(&kh);
        return NULL;
    }

    a->rebuild.bm = bm;
    a->rebuild.field_index = a->fi;
    a->rebuild.ts = a->ts;
    int walk_rc = slotcask_walk_one_shard_slots_locked(
        a->sdb, a->kf_shard, &kh, bm_rebuild_cb, &a->rebuild);
    int sync_rc = (!walk_rc && !a->rebuild.failed) ? bm_sync(bm) : -1;
    int saved_errno = a->rebuild.saved_errno;
    if (sync_rc != 0 && !saved_errno) saved_errno = errno;
    if (bm_close_checked(bm) != 0 && !saved_errno) saved_errno = errno;
    kfcache_release(&kh);

    if (walk_rc != 0 || a->rebuild.failed || sync_rc != 0 || saved_errno) {
        a->saved_errno = saved_errno;
        if (saved_errno) errno = saved_errno;
        LOG_ERROR(LOG_SUB_BITMAP,
                  "bm_shard_walk_worker: materialisation failed for %s shard %d: %s",
                  a->target_path, a->kf_shard, strerror(errno));
        unlink(a->tmp_path);
        return NULL;
    }
    a->result = index_build_ok();
    return NULL;
}

index_build_result build_bitmap_pass(const char *db_root, const char *object,
                      const Schema *sch, TypedSchema *ts,
                      const char *field, uint32_t max_values, int force) {
    (void)force;
    if (!ts) return index_build_failed(0);
    int fi = typed_field_index(ts, field);
    if (fi < 0) return index_build_failed(0);
    const TypedField *f = &ts->fields[fi];
    int bool_fastpath = (f->type == FT_BOOL);

    /* Build banner — matches BUILD-BTREE / BUILD-TRIGRAM pattern so
       operators can grep one prefix and see every index-build event.
       Without this entry, bitmap rebuilds were silent — a partial
       bulk-load (the 60s-client-timeout bug on the Netcup deploy)
       left 3 of 8 shards empty for stories' bitmaps but produced no
       log to spot it from. */
    LOG_WARN(LOG_SUB_BITMAP,
        "BUILD-BITMAP %s/%s/%s: %d shards, max_values=%u, bool_fastpath=%d",
        db_root, object, field, sch->splits, max_values, bool_fastpath);

    int slots_per_shard = (int)slotcask_default_slots_for_splits(sch->splits);

    /* Sibling temporaries require the field directory to exist. A first-time
       bitmap build has no live shard whose creation would have made it yet. */
    char first_target[PATH_MAX];
    char field_dir[PATH_MAX];
    bm_build_path(first_target, sizeof(first_target), db_root, object, field, 0);
    if (parent_dir_copy(first_target, field_dir, sizeof(field_dir)) != 0)
        return index_build_failed(errno);
    mkdirp(field_dir);

    /* Open the slotcask db for this object and walk every kf shard. */
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size,
        .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_BITMAP, "build_bitmap_pass: slotcask_registry_get failed for %s/%s/%s", db_root, object, field);
        return index_build_failed(0);
    }

    /* Parallel kf-shard walks: each worker opens its own .bm (paths are
       unique per kf shard), walks its assigned kf shard, and bm_sets
       directly into the mmap'd file. Zero file contention; no memory
       accumulation (mmap is the persistent store). Matches the
       phase-1-parallel shape used by btree/trigram. */
    BmShardWalkArg *args = calloc((size_t)sch->splits, sizeof(BmShardWalkArg));
    if (!args) {
        LOG_ERROR(LOG_SUB_BITMAP, "build_bitmap_pass: malloc failed for %d BmShardWalkArg entries (%s/%s/%s)", sch->splits, db_root, object, field);
        return index_build_failed(0);
    }
    for (int s = 0; s < sch->splits; s++) {
        bm_build_path(args[s].target_path, sizeof(args[s].target_path),
                      db_root, object, field, s);
        args[s].kf_shard       = s;
        args[s].slots_per_shard= slots_per_shard;
        args[s].fi             = fi;
        args[s].bool_fastpath  = bool_fastpath;
        args[s].max_values     = max_values;
        args[s].ts             = ts;
        args[s].sdb            = sdb;
    }
    parallel_for(bm_shard_walk_worker, args, sch->splits, sizeof(BmShardWalkArg));

    index_build_result result = index_build_ok();
    for (int s = 0; s < sch->splits; s++) {
        if (args[s].result.status == INDEX_BUILD_FAILED) {
            int saved_errno = args[s].saved_errno ? args[s].saved_errno : EIO;
            for (int i = 0; i < sch->splits; i++)
                if (args[i].tmp_path[0]) unlink(args[i].tmp_path);
            free(args);
            return index_build_failed(saved_errno);
        }
    }
    for (int s = 0; s < sch->splits; s++) {
        bm_publish_result publish = bm_publish_replace(args[s].target_path,
                                                      args[s].tmp_path);
        if (publish == BM_PUBLISH_PRE_RENAME_FAILED) {
            for (int i = s; i < sch->splits; i++)
                if (args[i].tmp_path[0]) unlink(args[i].tmp_path);
            result = index_build_failed(0);
            break;
        }
        if (publish == BM_PUBLISH_POST_RENAME_FSYNC_FAILED)
            result.status = INDEX_BUILD_DURABILITY_UNCONFIRMED;
    }
    free(args);
    return result;
}

int cmd_add_indexes(const char *db_root, const char *object,
                    const char *fields_json, int force) {
    uint64_t t_start = now_ms();
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
    if (sch.splits <= 0 || sch.streams <= 0 || !ts_for_idx) {
        OUT("{\"error\":\"cannot load object schema for index build\"}\n");
        return -1;
    }

    /* Parse + auto-promote each spec via the canonical helpers
       (config.c::parse_index_spec + idx_should_auto_bitmap). Same logic
       create-object's wire validator uses — single source of truth. */
    char       names[MAX_FIELDS][256];
    enum IndexType types[MAX_FIELDS];
    uint32_t   maxes[MAX_FIELDS];
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
            }
        }
    }

    /* Bitmap- and trigram-typed fields follow the same metadata-authoritative
       skip semantic as btree: with force, publish replacement shards; without
       force, no-op only when the canonical index.conf entry is active. All three
       types are accumulated into ONE combined MFFieldDesc array below and
       built via ONE call to build_indexes_streaming_multi — the same
       single-scan engine reindex_object uses — instead of the old
       build_bitmap_pass-per-field + build_trigram_pass-per-field +
       build_indexes_pass-batch triple dispatch (up to 3 separate
       full-object scans for one add-index call). */
    int total_fields = nfields;  /* preserved across the btree-only reduction below */
    int btree_count = 0;
    char btree_fields[MAX_FIELDS][256];
    MFFieldDesc *descs = total_fields > 0
                       ? calloc((size_t)total_fields, sizeof(MFFieldDesc))
                       : NULL;
    if (total_fields > 0 && !descs) {
        OUT("{\"error\":\"cannot allocate index build descriptors\"}\n");
        return -1;
    }
    int n_desc = 0;

    /* Resolve every requested field before any build or metadata write.
       A missing simple field, or a composite with an unresolved component,
       is a command error — silently skipping it would later write
       index.conf entries for a field that can never be built, or no-op a
       command the caller believed succeeded. Composites are btree-only
       (mirrors create-object's validator). */
    for (int i = 0; i < nfields; i++) {
        if (types[i] != IT_BTREE && strchr(names[i], '+')) {
            OUT("{\"error\":\"composite indexes are btree-only (got \\\"%s\\\")\"}\n",
                names[i]);
            free(descs);
            return -1;
        }
        if (strchr(names[i], '+')) {
            char fbuf[256];
            strncpy(fbuf, names[i], 255); fbuf[255] = '\0';
            char *save = NULL;
            int total = 0, resolved = 0;
            for (char *t = strtok_r(fbuf, "+", &save); t;
                 t = strtok_r(NULL, "+", &save)) {
                total++;
                if (typed_field_index(ts_for_idx, t) >= 0) resolved++;
            }
            if (resolved != total) {
                OUT("{\"error\":\"unknown field \\\"%s\\\" in add-indexes request\"}\n",
                    names[i]);
                free(descs);
                return -1;
            }
        } else if (typed_field_index(ts_for_idx, names[i]) < 0) {
            OUT("{\"error\":\"unknown field \\\"%s\\\" in add-indexes request\"}\n",
                names[i]);
            free(descs);
            return -1;
        }
    }

    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
    char *existing_conf = NULL;
    size_t existing_conf_len = 0;
    if (!force && index_conf_read(conf_path, &existing_conf, &existing_conf_len) != 0) {
        OUT("{\"error\":\"cannot read index metadata: %s\"}\n",
            strerror(errno));
        free(descs);
        return -1;
    }

    for (int i = 0; i < nfields; i++) {
        char canonical[300];
        index_canonical_line(canonical, sizeof(canonical),
                             names[i], types[i], maxes[i]);
        if (types[i] == IT_BITMAP) {
            if (!force && index_conf_has_line(existing_conf ? existing_conf : "",
                                              existing_conf_len, canonical))
                continue;
            if (descs) {
                int fi_t = typed_field_index(ts_for_idx, names[i]);
                MFFieldDesc *d = &descs[n_desc++];
                memset(d, 0, sizeof(*d));
                d->type = MF_BITMAP;
                strncpy(d->name, names[i], sizeof(d->name) - 1);
                d->field_indices[0] = fi_t;
                d->field_index_count = 1;
                d->bm_max_values = maxes[i];
                d->bm_bool_fastpath = (ts_for_idx->fields[fi_t].type == FT_BOOL) ? 1 : 0;
            }
            continue;
        }
        if (types[i] == IT_TRIGRAM) {
            if (!force && index_conf_has_line(existing_conf ? existing_conf : "",
                                              existing_conf_len, canonical))
                continue;
            if (descs) {
                int fi_t = typed_field_index(ts_for_idx, names[i]);
                MFFieldDesc *d = &descs[n_desc++];
                memset(d, 0, sizeof(*d));
                d->type = STREAM_TRIGRAM;
                strncpy(d->name, names[i], sizeof(d->name) - 1);
                d->field_indices[0] = fi_t;
                d->field_index_count = 1;
            }
            continue;
        }
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, (size_t)btree_count * sizeof(btree_fields[0]));
    nfields = btree_count;

    /* === Btree fields: same skip-if-exists / force-replacement semantics as
       before — just add one MFFieldDesc per field to the SAME combined
       array built above instead of running a separate batched pass. */
    char actual_fields[MAX_FIELDS][256];
    int actual_count = 0;
    if (nfields > 0) {
        /* Filter out already-existing btree indexes (unless force). */
        for (int i = 0; i < nfields; i++) {
            if (!force && index_conf_has_line(existing_conf ? existing_conf : "",
                                              existing_conf_len, fields[i])) {
                continue; /* skip existing */
            }
            memcpy(actual_fields[actual_count], fields[i], 256);
            actual_count++;
        }

        if (actual_count > 0 && descs) {
            for (int i = 0; i < actual_count; i++) {
                int fi_t = typed_field_index(ts_for_idx, actual_fields[i]);
                MFFieldDesc *d = &descs[n_desc++];
                memset(d, 0, sizeof(*d));
                d->type = STREAM_BTREE;
                strncpy(d->name, actual_fields[i], sizeof(d->name) - 1);
                d->is_composite = (strchr(actual_fields[i], '+') != NULL);
                if (d->is_composite) {
                    char fbuf[256];
                    strncpy(fbuf, actual_fields[i], 255); fbuf[255] = '\0';
                    char *save = NULL;
                    for (char *t = strtok_r(fbuf, "+", &save);
                         t && d->field_index_count < 16;
                         t = strtok_r(NULL, "+", &save)) {
                        int ci = typed_field_index(ts_for_idx, t);
                        if (ci >= 0)
                            d->field_indices[d->field_index_count++] = ci;
                    }
                } else {
                    d->field_indices[0] = fi_t;
                    d->field_index_count = 1;
                }
            }
        }
    }
    free(existing_conf);

    /* Single scan: build every requested bitmap/trigram/btree field in
       ONE call to the same engine reindex uses. This is the fix for the
       "N separate full-object scans per add-index call" incident. */
    index_build_result build_result = index_build_ok();
    if (n_desc > 0) {
        LOG_AUDIT(LOG_SUB_INDEX, "ADD-INDEXES %s: %d field(s), single scan",
                 object, n_desc);
        build_result = build_indexes_streaming_multi(db_root, object, &sch, ts_for_idx, descs, n_desc);
    }
    free(descs);

    if (build_result.status == INDEX_BUILD_FAILED) {
        OUT("{\"error\":\"index build failed; index metadata was not changed; "
            "one or more shards may already have been published\"}\n");
        return -1;
    }

    /* Build complete canonical additions in memory and atomically publish the
       resulting metadata file only after every requested descriptor built. */
    char canonical_lines[MAX_FIELDS][300];
    const char *metadata_lines[MAX_FIELDS];
    for (int i = 0; i < total_fields; i++) {
        index_canonical_line(canonical_lines[i], sizeof(canonical_lines[i]),
                             names[i], types[i], maxes[i]);
        metadata_lines[i] = canonical_lines[i];
    }
    index_build_result metadata_result = index_conf_append_unique(conf_path,
                                                                   metadata_lines,
                                                                   total_fields);
    if (metadata_result.status == INDEX_BUILD_FAILED) {
        OUT("{\"error\":\"index shards published but index metadata update failed; "
            "retry add-index\"}\n");
        return -1;
    }

    invalidate_idx_cache(db_root, object);
    uint64_t duration_ms = now_ms() - t_start;
    int records = get_live_count(db_root, object);
    if (records < 0) records = 0;
    if (build_result.status == INDEX_BUILD_DURABILITY_UNCONFIRMED ||
        metadata_result.status == INDEX_BUILD_DURABILITY_UNCONFIRMED) {
        OUT("{\"warning\":\"index and metadata published but directory durability "
            "is unconfirmed\"}\n");
        return 1;
    }
    /* Response semantics:
         all-typed-only            → {"status":"ok","records":..,"duration_ms":..}
         btree present, all exist  → {"status":"all_exist"}
         btree built (>=1)         → {"status":"indexed","count":N,"records":..,"duration_ms":..} */
    if (btree_count == 0) {
        OUT("{\"status\":\"ok\",\"records\":%d,\"duration_ms\":%llu}\n",
            records, (unsigned long long)duration_ms);
    } else if (actual_count == 0) {
        OUT("{\"status\":\"all_exist\"}\n");
    } else {
        OUT("{\"status\":\"indexed\",\"count\":%d,\"records\":%d,\"duration_ms\":%llu}\n",
            actual_count, records, (unsigned long long)duration_ms);
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
        for (int s = 0; s < splits; s++) {
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
    invalidate_idx_cache(db_root, object);

    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %s", db_root, object, field);
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

    invalidate_idx_cache(db_root, object);
    LOG_AUDIT(LOG_SUB_INDEX, "REMOVE-INDEX %s/%s: %d removed, %d not_indexed", db_root, object, removed, missing);
    OUT("{\"status\":\"removed\",\"count\":%d,\"not_indexed\":%d}\n", removed, missing);
    return 0;
}

/* ========== MULTI-FIELD STREAMING REINDEX ====================================
   Scans all kf shards exactly ONCE, extracting index values for every field
   simultaneously into per-field spill buffers (btree/trigram) or writing
   directly to mmap'd .bm files (bitmap).  Phase 2 merges per-field spills
   into final index files independently.  One scan for N fields.
   ============================================================================ */

/* MF_BITMAP + MFFieldDesc are defined earlier (above build_trigram_pass)
   so the add-index builders can construct descriptors too. */

/* Per-worker, per-field mutable buffers.  One set per (kf_shard × field). */
typedef struct {
    /* btree / trigram path */
    BtEntry     *pairs;
    size_t       pairs_cap, pair_count;
    uint8_t     *arena;
    size_t       arena_cap, arena_used;
    BtEntry     *flush_out;
    size_t      *flush_counts, *flush_offsets, *flush_cursors;
    SpillWriter *spill_writers;   /* [idx_n] */
    /* bitmap path */
    BitmapShard *bm;              /* opened at scan start, closed at end */
    int          had_error;
} MFWorkerField;

/* Flush one btree/trigram field's sort buffer to its per-shard spill files. */
static void mf_flush_field(MFWorkerField *f, int splits, int idx_n) {
    if (f->pair_count == 0) return;
    for (size_t i = 0; i < f->pair_count; i++) {
        uintptr_t off = (uintptr_t)f->pairs[i].value;
        f->pairs[i].value = (const char *)(f->arena + off);
    }
    memset(f->flush_counts, 0, (size_t)idx_n * sizeof(size_t));
    for (size_t i = 0; i < f->pair_count; i++)
        f->flush_counts[idx_shard_for_hash(f->pairs[i].hash, splits)]++;
    f->flush_offsets[0] = 0;
    for (int i = 1; i < idx_n; i++)
        f->flush_offsets[i] = f->flush_offsets[i-1] + f->flush_counts[i-1];
    memcpy(f->flush_cursors, f->flush_offsets, (size_t)idx_n * sizeof(size_t));
    for (size_t i = 0; i < f->pair_count; i++) {
        int s = idx_shard_for_hash(f->pairs[i].hash, splits);
        f->flush_out[f->flush_cursors[s]++] = f->pairs[i];
    }
    for (int s = 0; s < idx_n; s++) {
        if (f->flush_counts[s] == 0) continue;
        BtEntry *sl = f->flush_out + f->flush_offsets[s];
        qsort(sl, f->flush_counts[s], sizeof(BtEntry), cmp_btentry_fn);
        if (spill_writer_write_run(&f->spill_writers[s], sl,
                                   (uint32_t)f->flush_counts[s]) != 0)
            f->had_error = 1;
    }
    f->pair_count = 0;
    f->arena_used = 0;
}

/* Extract one btree/trigram field's index key(s) from a record value and
   append to the field's spill buffer (flushing first if it would overflow).
   Shared by the kf-walk bitmap path's siblings AND the segment-sequential
   scan, so both produce byte-identical spill output. Bitmap fields are NOT
   handled here (they need the kf slot). */
static void mf_append_field(MFWorkerField *f, const MFFieldDesc *d,
                            const uint8_t hash16[16], const uint8_t *value,
                            TypedSchema *ts, int splits, int idx_n) {
    if (d->type == STREAM_TRIGRAM) {
        int tidx = d->field_indices[0];
        const TypedField *tf = &ts->fields[tidx];
        if (tf->type != FT_VARCHAR) return;
        const uint8_t *vb = value + tf->offset;
        uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
        if (al == 0) return;
        /* al is an on-disk length prefix; clamp it to the field's actual
           declared content size before using it to read past vb + 2
           (CID 1696428). */
        size_t max_content = tf->size > 2 ? (size_t)tf->size - 2 : 0;
        if ((size_t)al > max_content) al = (uint16_t)max_content;
        if (al == 0) return;
        uint8_t tg[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
        if (n == 0) return;
        /* Process trigrams in batches to handle n > pairs_cap.
           When reindex builds multiple indexes, per_field_budget is divided
           by n_fields, reducing pairs_cap below TG_MAX_DISTINCT. Records with
           many distinct trigrams would be silently dropped. Batch processing
           flushes between chunks so all trigrams are indexed. */
        size_t i = 0;
        while (i < n) {
            /* Flush if buffer is full or would overflow with next trigram */
            if (f->pair_count >= f->pairs_cap || f->arena_used + 3 > f->arena_cap) {
                mf_flush_field(f, splits, idx_n);
            }

            /* Add as many trigrams as will fit in this batch */
            size_t batch = 0;
            while (i + batch < n &&
                   f->pair_count + batch + 1 <= f->pairs_cap &&
                   f->arena_used + (batch + 1) * 3 <= f->arena_cap) {
                size_t off = f->arena_used;
                memcpy(f->arena + off, tg[i + batch], 3);
                f->arena_used += 3;
                f->pairs[f->pair_count + batch].value = (const char *)(uintptr_t)off;
                f->pairs[f->pair_count + batch].vlen  = 3;
                memcpy(f->pairs[f->pair_count + batch].hash, hash16, BT_HASH_SIZE);
                batch++;
            }
            f->pair_count += batch;
            i += batch;

            /* Safety: if batch == 0, we're stuck (shouldn't happen with proper caps) */
            if (batch == 0) break;
        }
        return;
    }

    /* STREAM_BTREE (simple or composite) */
    uint8_t kb[4096]; size_t kl = 0;
    if (d->is_composite) {
        char cat[4096]; int cpos = 0; int ok = 1;
        for (int i = 0; i < d->field_index_count; i++) {
            size_t blen = 0;
            typed_field_to_index_key(ts, value, d->field_indices[i],
                                      (uint8_t *)cat + cpos, &blen);
            if (blen == 0) { ok = 0; break; }
            if (cpos + (int)blen < (int)sizeof(cat)) cpos += (int)blen;
            else { ok = 0; break; }
        }
        if (!ok || cpos == 0) return;
        kl = (size_t)cpos;
        if (kl > sizeof(kb)) return;
        memcpy(kb, cat, kl);
    } else {
        int fidx = d->field_indices[0];
        if (fidx < 0) return;
        typed_field_to_index_key(ts, value, fidx, kb, &kl);
        if (kl == 0) return;
    }
    if (f->pair_count + 1 > f->pairs_cap || f->arena_used + kl > f->arena_cap)
        mf_flush_field(f, splits, idx_n);
    if (kl > f->arena_cap || 1 > f->pairs_cap) return;
    size_t off = f->arena_used;
    memcpy(f->arena + off, kb, kl);
    f->arena_used += kl;
    f->pairs[f->pair_count].value = (const char *)(uintptr_t)off;
    f->pairs[f->pair_count].vlen  = kl;
    memcpy(f->pairs[f->pair_count].hash, hash16, BT_HASH_SIZE);
    f->pair_count++;
}

/* Free per-worker spill buffers for one field (btree/trigram only). */
static void mf_worker_field_free_spill(MFWorkerField *f, int idx_n) {
    if (f->spill_writers) {
        for (int s = 0; s < idx_n; s++) spill_writer_close(&f->spill_writers[s]);
        free(f->spill_writers); f->spill_writers = NULL;
    }
    free(f->pairs);        f->pairs        = NULL;
    free(f->arena);        f->arena        = NULL;
    free(f->flush_out);    f->flush_out    = NULL;
    free(f->flush_counts); f->flush_counts = NULL;
    free(f->flush_offsets);f->flush_offsets= NULL;
    free(f->flush_cursors);f->flush_cursors= NULL;
}

/* Allocate one spill field's sort+flush buffers. Returns 0 on success.
   spill_writers[].fd is initialised to -1 (files opened lazily by the
   worker). est_value_bytes drives the arena/pair sizing. */
static int mf_worker_field_alloc(MFWorkerField *f, const MFFieldDesc *d,
                                 TypedSchema *ts, size_t per_field_budget,
                                 int idx_n) {
    size_t est = (d->type == STREAM_TRIGRAM) ? 3 : 0;
    if (est == 0) {
        for (int i = 0; i < d->field_index_count; i++) {
            int ti = d->field_indices[i];
            if (ti >= 0) est += (size_t)ts->fields[ti].size;
        }
        if (est < 8)   est = 8;
        if (est > 256) est = 256;
    }
    size_t cap = per_field_budget / (2 * sizeof(BtEntry) + est);
    if (cap < 4096)    cap = 4096;
    if (cap > 2000000) cap = 2000000;

    f->pairs_cap = cap;
    f->arena_cap = cap * est < 65536 ? 65536 : cap * est;
    f->pairs         = calloc(cap, sizeof(BtEntry));
    f->arena         = calloc(f->arena_cap, 1);
    f->flush_out     = calloc(cap, sizeof(BtEntry));
    f->flush_counts  = calloc((size_t)idx_n, sizeof(size_t));
    f->flush_offsets = calloc((size_t)idx_n, sizeof(size_t));
    f->flush_cursors = calloc((size_t)idx_n, sizeof(size_t));
    if (!f->pairs || !f->arena || !f->flush_out ||
        !f->flush_counts || !f->flush_offsets || !f->flush_cursors) {
        LOG_ERROR(LOG_SUB_REINDEX, "mf_worker_field_alloc: allocation failed for field %s (cap=%zu, idx_n=%d)", d->name, cap, idx_n);
        return -1;
    }
    f->spill_writers = calloc((size_t)idx_n, sizeof(SpillWriter));
    if (!f->spill_writers) {
        LOG_ERROR(LOG_SUB_REINDEX, "mf_worker_field_alloc: calloc failed for %d spill_writers (field %s)", idx_n, d->name);
        return -1;
    }
    for (int s = 0; s < idx_n; s++) f->spill_writers[s].fd = -1;
    return 0;
}

/* ====================================================================
   Segment-sequential index build (btree/trigram only).

   Instead of walking kf shards (whose records are hash-scattered 1-in-
   `splits` across the entire value store — pathological random I/O on a
   dataset that exceeds RAM), this reads every segment .dat file ONCE,
   front-to-back, at fixed slot_size stride. For each live (flag==1)
   record it extracts every index field's key(s) and routes to per-worker
   per-output-shard spill files; Phase 2 merges them into the final
   .idx/.tg files via the existing streaming merge.

   Correctness: on update/delete the superseded segment record is set to
   flag=2 (seg_write_flag(...,2)); a crash mid-emit leaves flag=0. So
   flag==1 == the current live version. The caller holds objlock_wrlock,
   so no writes race the scan.

   Bitmap fields are built in the SAME scan: a bitmap needs (kf_slot, value),
   and the slot isn't in the segment record — only the keyfile has it. So we
   capture (kf_shard, value, hash) during the scan into per-worker append
   files, then a resolve phase reads the keyfile (sequential, 1.6GB) to turn
   hash→slot and set the bits. One pass over the 151GB value store builds
   btree, trigram, AND bitmap indexes.
   ==================================================================== */
typedef struct { uint8_t sid; uint32_t fid; } SegRef;

typedef struct {
    const char        *db_root, *object, *data_dir;
    int                splits, idx_n, slot_size, worker_idx;
    TypedSchema       *ts;
    int                n_fields;
    const MFFieldDesc *descs;     /* all fields (btree/trigram/bitmap) */
    MFWorkerField     *fields;    /* [n_fields]; sort buffers for btree/trigram */
    SpillWriter       *bm_writers;/* [n_fields]; append file for bitmap fields */
    const SegRef      *segs;
    int                seg_start, seg_count;
    _Atomic int       *segs_done; /* progress counter (shared) */
    int                had_error;
    uint8_t           *padded_value; /* ts->total_size bytes; zero-pads compact VARIABLE records */
} SegScanWorker;

/* Bitmap spill entry on disk: [u16 kf_shard][u16 vlen][vlen bytes][16B hash].
   Unsorted append — bitmaps don't need ordering, just (value→slot) membership. */
static void bm_spill_append(SpillWriter *bw, int kf_shard,
                            const uint8_t *val, size_t vlen,
                            const uint8_t hash[16], int *had_error) {
    uint16_t s16 = (uint16_t)kf_shard, l16 = (uint16_t)vlen;
    if (spill_writer_put(bw, &s16, 2) != 0 ||
        spill_writer_put(bw, &l16, 2) != 0 ||
        spill_writer_put(bw, val, vlen) != 0 ||
        spill_writer_put(bw, hash, 16) != 0)
        *had_error = 1;
}

static int reindex_seg_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *ctx) {
    SegScanWorker *w = (SegScanWorker *)ctx;
    uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
    const uint8_t *value = rec + 24 + klen;
    /* Compact VARIABLE records may be shorter than ts->total_size (trailing
       zero fields trimmed). Pad to total_size so field access at tf->offset
       is always safe — fields beyond vlen read back as zero. */
    if (w->padded_value && vlen < (size_t)w->ts->total_size) {
        memset(w->padded_value, 0, (size_t)w->ts->total_size);
        if (vlen > 0) memcpy(w->padded_value, value, vlen);
        value = w->padded_value;
    }
    for (int fi = 0; fi < w->n_fields; fi++) {
        const MFFieldDesc *d = &w->descs[fi];
        if (d->type == MF_BITMAP) {
            int tidx = d->field_indices[0];
            if (tidx < 0) continue;
            const TypedField *tf = &w->ts->fields[tidx];
            const uint8_t *vb = value + tf->offset;
            const uint8_t *bval; size_t blen;
            if (tf->type == FT_VARCHAR) {
                uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
                if (al == 0) continue;
                bval = vb + 2; blen = al;
            } else {
                if (tf->size == 0) continue;
                bval = vb; blen = (size_t)tf->size;
            }
            int kf_shard = compute_record_shard(hash16, w->splits);
            bm_spill_append(&w->bm_writers[fi], kf_shard, bval, blen,
                            hash16, &w->had_error);
            continue;
        }
        mf_append_field(&w->fields[fi], d, hash16, value,
                        w->ts, w->splits, w->idx_n);
    }
    return 0;
}

static void *seg_scan_worker(void *arg) {
    SegScanWorker *w = (SegScanWorker *)arg;

    /* Lazily open this worker's spill files. btree/trigram → one file per
       idx shard (sorted-run spill). bitmap → one append file per field. */
    for (int fi = 0; fi < w->n_fields; fi++) {
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 w->db_root, w->object, w->descs[fi].name, fi);
        if (w->descs[fi].type == MF_BITMAP) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/bmw%d.bin", spill_dir, w->worker_idx);
            if (spill_writer_open(&w->bm_writers[fi], path) != 0)
                w->had_error = 1;
            continue;
        }
        MFWorkerField *f = &w->fields[fi];
        for (int s = 0; s < w->idx_n; s++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/w%d_s%d.bin",
                     spill_dir, w->worker_idx, s);
            if (spill_writer_open(&f->spill_writers[s], path) != 0)
                f->had_error = 1;
        }
    }

    for (int si = w->seg_start; si < w->seg_start + w->seg_count; si++) {
        const SegRef *sr = &w->segs[si];
        char path[PATH_MAX];
        snprintf(path, sizeof(path), "%s/data/streams/%03d/%06u.dat",
                 w->data_dir, (int)sr->sid, (unsigned)sr->fid);
        int rc = seg_scan_o_direct(path, (size_t)w->slot_size,
                                          reindex_seg_cb, w);
        if (rc < 0) {
            LOG_ERROR(LOG_SUB_REINDEX,
                      "REINDEX %s/%s: segment scan failed for %s (rc=%d %s)",
                      w->db_root, w->object, path, rc, strerror(-rc));
            w->had_error = 1;
        }
        if (w->segs_done) atomic_fetch_add(w->segs_done, 1);
    }

    /* Final flush of btree/trigram buffers; drain+close bitmap append files. */
    for (int fi = 0; fi < w->n_fields; fi++) {
        if (w->descs[fi].type == MF_BITMAP)
            spill_writer_close(&w->bm_writers[fi]);
        else
            mf_flush_field(&w->fields[fi], w->splits, w->idx_n);
    }
    return NULL;
}

/* Enumerate every segment .dat file under data/streams/<sid>/. A successful
   empty result is distinct from a setup failure. */
static int enumerate_segments(const char *data_dir, int n_streams,
                              SegRef **out_segs, int *out_n) {
    size_t cap = 256, n = 0;
    *out_segs = NULL;
    *out_n = 0;
    SegRef *segs = malloc(cap * sizeof(SegRef));
    if (!segs) {
        LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: malloc failed for %zu SegRef entries (%s)", cap, data_dir);
        return -1;
    }
    for (int sid = 0; sid < n_streams; sid++) {
        char dir[PATH_MAX];
        snprintf(dir, sizeof(dir), "%s/data/streams/%03d", data_dir, sid);
        DIR *d = opendir(dir);
        if (!d) {
            LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: opendir(%s) failed: %s", dir, strerror(errno));
            free(segs);
            return -1;
        }
        struct dirent *e;
        errno = 0;
        while ((e = readdir(d))) {
            if (e->d_name[0] == '.') continue;
            size_t L = strlen(e->d_name);
            if (L < 5 || strcmp(e->d_name + L - 4, ".dat") != 0) continue;
            char *end = NULL;
            unsigned long fid = strtoul(e->d_name, &end, 10);
            if (!end || end == e->d_name) continue;
            if (n == cap) {
                cap *= 2;
                SegRef *t = realloc(segs, cap * sizeof(SegRef));
                if (!t) {
                    LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: realloc failed growing to %zu SegRef entries (%s)", cap, data_dir);
                    closedir(d);
                    free(segs);
                    return -1;
                }
                segs = t;
            }
            segs[n].sid = (uint8_t)sid;
            segs[n].fid = (uint32_t)fid;
            n++;
        }
        if (errno != 0) {
            LOG_ERROR(LOG_SUB_REINDEX, "enumerate_segments: readdir(%s) failed: %s", dir, strerror(errno));
            closedir(d);
            free(segs);
            return -1;
        }
        closedir(d);
    }
    *out_n = (int)n;
    *out_segs = segs;
    return 0;
}

/* Single segment-sequential build for a set of btree/trigram fields. */
/* Read-only mmap of one kf shard for hash→slot probing. */
typedef struct {
    uint8_t                *map;       /* full mmap base (NULL if absent) */
    size_t                  map_size;
    const SlotcaskKfEntry  *ent;       /* map + 24 */
    size_t                  cap;
} KfMap;

/* Open-addressed probe matching slotcask's lookup: stop at flag==0 (empty),
   skip flag==2 (tombstone), match flag==1 + hash. Returns slot or -1. */
static long kf_probe_slot(const KfMap *k, const uint8_t hash[16]) {
    if (!k->map || k->cap == 0) return -1;
    size_t start = kf_slot_for(hash, k->cap);
    for (size_t i = 0; i < k->cap; i++) {
        size_t slot = (start + i) % k->cap;
        const SlotcaskKfEntry *e = &k->ent[slot];
        if (e->flag == 0) return -1;
        if (e->flag == 1 && memcmp(e->hash, hash, 16) == 0) return (long)slot;
    }
    return -1;
}

/* Phase 2b — turn captured (kf_shard, value, hash) bitmap spills into .bm
   files. Reads every keyfile shard once (sequential, ~1.6GB total) to map
   hash→slot, then sets bits. Single-threaded: per field opens all `splits`
   bm writers + holds `splits` kf mmaps, so fd use is ~2×splits. */
static index_build_result resolve_bitmaps(const char *db_root, const char *object,
                           const Schema *sch, TypedSchema *ts,
                           SlotcaskDb *sdb,
                           const MFFieldDesc *descs, int n_fields, int P) {
    (void)ts;
    int splits = sch->splits;
    int n_bm = 0;
    for (int fi = 0; fi < n_fields; fi++) if (descs[fi].type == MF_BITMAP) n_bm++;
    if (n_bm == 0) return index_build_ok();

    index_build_result result = index_build_ok();

    /* mmap every kf shard once, read-only, shared across all bitmap fields. */
    KfMap *kf = calloc((size_t)splits, sizeof(KfMap));
    if (!kf) {
        LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: calloc failed for %d KfMap entries (%s/%s)", splits, db_root, object);
        return index_build_failed(0);
    }
    for (int s = 0; s < splits; s++) {
        char kp[PATH_MAX];
        slotcask_kf_path(kp, sizeof(kp), sdb->data_dir, s);
        int fd = open(kp, O_RDONLY);
        if (fd < 0) {
            LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: open(%s) failed: %s",
                      kp, strerror(errno));
            result = index_build_failed(0);
            goto cleanup_kf;
        }
        struct stat st;
        if (fstat(fd, &st) != 0 || st.st_size <= 24 ||
            ((size_t)st.st_size - 24) % sizeof(SlotcaskKfEntry) != 0 ||
            ((size_t)st.st_size - 24) / sizeof(SlotcaskKfEntry) == 0) {
            LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: malformed kf shard %s",
                      kp);
            close(fd);
            result = index_build_failed(0);
            goto cleanup_kf;
        }
        void *m = mmap(NULL, (size_t)st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (m == MAP_FAILED) {
            LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: mmap(%s) failed: %s",
                      kp, strerror(errno));
            result = index_build_failed(0);
            goto cleanup_kf;
        }
        kf[s].map = m;
        kf[s].map_size = (size_t)st.st_size;
        kf[s].ent = (const SlotcaskKfEntry *)((uint8_t *)m + 24);
        kf[s].cap = ((size_t)st.st_size - 24) / sizeof(SlotcaskKfEntry);
    }

    LOG_WARN(LOG_SUB_BITMAP, "BUILD-BITMAP %s/%s: resolving %d bitmap field(s) via kf join",
             db_root, object, n_bm);

    for (int fi = 0; fi < n_fields; fi++) {
        if (descs[fi].type != MF_BITMAP) continue;

        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);

        /* Materialise every shard at an unreferenced sibling path first.
           A scan, spill, or bitmap-write error must leave all live paths
           for this field untouched. */
        BitmapShard **bm = calloc((size_t)splits, sizeof(BitmapShard *));
        char (*targets)[PATH_MAX] = calloc((size_t)splits, sizeof(*targets));
        char (*temps)[PATH_MAX] = calloc((size_t)splits, sizeof(*temps));
        int field_failed = !bm || !targets || !temps;
        if (field_failed) {
            LOG_ERROR(LOG_SUB_BITMAP, "resolve_bitmaps: allocation failed for %s/%s/%s",
                      db_root, object, descs[fi].name);
            goto field_cleanup;
        }
        durability_test_pause(spill_dir, "bm-resolve-before-open");
        for (int s = 0; s < splits; s++) {
            bm_build_path(targets[s], sizeof(targets[s]), db_root, object,
                          descs[fi].name, s);
            if (bm_rebuild_temp_path(targets[s], temps[s]) != 0) {
                field_failed = 1;
                break;
            }
            /* Keyfile shards can auto-resplit independently. Rebuild each
               bitmap at its actual keyfile capacity, not the original
               default tier, otherwise a live record whose kf slot is above
               that tier makes bm_set() fail during the join. */
            bm[s] = bm_open(temps[s], (int)kf[s].cap, 1 /* create */,
                            descs[fi].bm_bool_fastpath, descs[fi].bm_max_values,
                            1 /* writer */);
            if (!bm[s]) {
                field_failed = 1;
                break;
            }
        }

        for (int w = 0; w < P; w++) {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/bmw%d.bin", spill_dir, w);
            int fd = open(path, O_RDONLY);
            if (fd < 0) {
                if (errno == ENOENT) continue;
                field_failed = 1;
                break;
            }
            struct stat st;
            if (fstat(fd, &st) != 0 || st.st_size < 0) {
                close(fd);
                field_failed = 1;
                break;
            }
            if (st.st_size > 0) {
                uint8_t *m = mmap(NULL, (size_t)st.st_size, PROT_READ,
                                  MAP_PRIVATE, fd, 0);
                if (m == MAP_FAILED) {
                    close(fd);
                    field_failed = 1;
                    break;
                }
#ifdef __linux__
                madvise(m, (size_t)st.st_size, MADV_SEQUENTIAL);
#endif
                size_t pos = 0, sz = (size_t)st.st_size;
                while (pos + 4 <= sz) {
                    uint16_t kfs = (uint16_t)m[pos] | ((uint16_t)m[pos+1] << 8);
                    uint16_t vl  = (uint16_t)m[pos+2] | ((uint16_t)m[pos+3] << 8);
                    pos += 4;
                    if (pos + (size_t)vl + 16 > sz || kfs >= splits || !bm[kfs]) {
                        field_failed = 1;
                        break;
                    }
                    const uint8_t *val  = m + pos;
                    const uint8_t *hash = m + pos + vl;
                    pos += (size_t)vl + 16;
                    long slot = kf_probe_slot(&kf[kfs], hash);
                    if (slot < 0 || bm_set(bm[kfs], val, vl, (uint32_t)slot) != 0) {
                        field_failed = 1;
                        break;
                    }
                }
                if (pos != sz) field_failed = 1;
#ifdef __linux__
                madvise(m, (size_t)st.st_size, MADV_DONTNEED);
#endif
                munmap(m, (size_t)st.st_size);
            }
            close(fd);
            unlink(path);
            if (field_failed) break;
        }

        for (int s = 0; s < splits; s++) {
            if (bm[s] && !field_failed && bm_sync(bm[s]) != 0) field_failed = 1;
            if (bm[s] && bm_close_checked(bm[s]) != 0) field_failed = 1;
            bm[s] = NULL;
        }
        if (!field_failed) {
            for (int s = 0; s < splits; s++) {
                if (bm_cache_invalidate_checked(temps[s]) != 0) {
                    field_failed = 1;
                    break;
                }
            }
        }
        if (!field_failed) {
            for (int s = 0; s < splits; s++) {
                bm_publish_result publish = bm_publish_replace(targets[s], temps[s]);
                if (publish == BM_PUBLISH_PRE_RENAME_FAILED) {
                    field_failed = 1;
                    break;
                }
                if (publish == BM_PUBLISH_POST_RENAME_FSYNC_FAILED &&
                    result.status == INDEX_BUILD_OK)
                    result.status = INDEX_BUILD_DURABILITY_UNCONFIRMED;
            }
        }

field_cleanup:
        if (field_failed) {
            result = index_build_failed(0);
            for (int s = 0; s < splits; s++) {
                if (bm && bm[s]) bm_close(bm[s]);
                if (temps && temps[s][0]) unlink(temps[s]);
            }
        }
        free(bm);
        free(targets);
        free(temps);
        rmdir(spill_dir);
        if (field_failed) break;
    }

cleanup_kf:
    for (int s = 0; s < splits; s++)
        if (kf[s].map) munmap(kf[s].map, kf[s].map_size);
    free(kf);
    return result;
}

static index_build_result seg_seq_build_spills(const char *db_root, const char *object,
                                const Schema *sch, TypedSchema *ts,
                                SlotcaskDb *sdb,
                                const MFFieldDesc *descs, int n_fields) {
    if (n_fields <= 0) return index_build_ok();
    int idx_n = index_splits_for(sch->splits);

    for (int fi = 0; fi < n_fields; fi++) {
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        mkdirp(spill_dir);
    }

    int n_segs = 0;
    SegRef *segs = NULL;
    if (enumerate_segments(sdb->data_dir, sch->streams, &segs, &n_segs) != 0) {
        for (int fi = 0; fi < n_fields; fi++) {
            char spill_dir[PATH_MAX];
            snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                     db_root, object, descs[fi].name, fi);
            rmdir(spill_dir);
        }
        return index_build_failed(0);
    }

    int pool_size = parallel_pool_size();
    if (pool_size < 1) pool_size = 1;
    int P = pool_size;
    if (n_segs > 0 && P > n_segs) P = n_segs;

    size_t budget = g_index_build_budget_bytes;
    if (budget < 64ULL * 1024 * 1024) budget = 64ULL * 1024 * 1024;
    size_t per_worker_total = budget / (size_t)P / 2;
    if (per_worker_total < 8ULL * 1024 * 1024) per_worker_total = 8ULL * 1024 * 1024;
    size_t per_field_budget = per_worker_total / (size_t)n_fields;
    if (per_field_budget < 4ULL * 1024 * 1024) per_field_budget = 4ULL * 1024 * 1024;

    LOG_WARN(LOG_SUB_REINDEX,
        "BUILD-SEQ %s/%s: %d fields, %d segments, %d workers, slot_size=%d, per-field budget=%zu MB",
        db_root, object, n_fields, n_segs, P, sch->slot_size,
        per_field_budget / (1024 * 1024));

    SegScanWorker *workers = calloc((size_t)P, sizeof(SegScanWorker));
    if (!workers) {
        LOG_ERROR(LOG_SUB_REINDEX, "seg_seq_build_spills: calloc failed for %d SegScanWorker entries (%s/%s)", P, db_root, object);
        free(segs);
        return index_build_failed(0);
    }

    /* Contiguous segment ranges per worker so each reads its files in order. */
    int base = n_segs / P, extra = n_segs % P, cursor = 0;
    int alloc_ok = 1;
    for (int w = 0; w < P && alloc_ok; w++) {
        int cnt = base + (w < extra ? 1 : 0);
        workers[w].db_root    = db_root;
        workers[w].object     = object;
        workers[w].data_dir   = sdb->data_dir;
        workers[w].splits     = sch->splits;
        workers[w].idx_n      = idx_n;
        workers[w].slot_size  = sch->slot_size;
        workers[w].worker_idx = w;
        workers[w].ts         = ts;
        workers[w].n_fields   = n_fields;
        workers[w].descs      = descs;
        workers[w].segs       = segs;
        workers[w].seg_start  = cursor;
        workers[w].seg_count  = cnt;
        cursor += cnt;
        workers[w].fields = calloc((size_t)n_fields, sizeof(MFWorkerField));
        workers[w].bm_writers = calloc((size_t)n_fields, sizeof(SpillWriter));
        if (!workers[w].fields || !workers[w].bm_writers) { alloc_ok = 0; break; }
        if (ts->total_size > 0) {
            workers[w].padded_value = calloc(1, (size_t)ts->total_size);
            if (!workers[w].padded_value) { alloc_ok = 0; break; }
        }
        for (int fi = 0; fi < n_fields; fi++) workers[w].bm_writers[fi].fd = -1;
        for (int fi = 0; fi < n_fields && alloc_ok; fi++) {
            if (descs[fi].type == MF_BITMAP) continue;  /* bitmap → append file, no sort buffers */
            if (mf_worker_field_alloc(&workers[w].fields[fi], &descs[fi],
                                      ts, per_field_budget, idx_n) != 0)
                alloc_ok = 0;
        }
    }

    if (!alloc_ok) {
        LOG_ERROR(LOG_SUB_REINDEX, "seg_seq_build_spills: per-worker buffer allocation failed (%s/%s, %d workers, %d fields)", db_root, object, P, n_fields);
        for (int w = 0; w < P; w++) {
            if (workers[w].fields) {
                for (int fi = 0; fi < n_fields; fi++)
                    mf_worker_field_free_spill(&workers[w].fields[fi], idx_n);
                free(workers[w].fields);
            }
            free(workers[w].bm_writers);
        }
        free(workers); free(segs);
        return index_build_failed(0);
    }

    /* Phase 1: parallel sequential scan (parallel_for_io = independent
       threads, immune to pool starvation while we hold objlock_wrlock). */
    _Atomic int segs_done = 0;
    for (int w = 0; w < P; w++) workers[w].segs_done = &segs_done;
    uint64_t t_scan = now_ms_coarse();
    parallel_for_io(seg_scan_worker, workers, P, sizeof(SegScanWorker));
    LOG_INFO(LOG_SUB_REINDEX, "BUILD-SEQ %s/%s: scan done (%d/%d segments) in %llums, merging...",
             db_root, object, atomic_load(&segs_done), n_segs,
             (unsigned long long)(now_ms_coarse() - t_scan));

    int any_error = 0;
    for (int w = 0; w < P; w++) {
        if (workers[w].had_error) any_error = 1;
        for (int fi = 0; fi < n_fields; fi++) {
            if (workers[w].fields[fi].had_error) any_error = 1;
            mf_worker_field_free_spill(&workers[w].fields[fi], idx_n);
        }
        free(workers[w].fields);
        free(workers[w].bm_writers);
        free(workers[w].padded_value);
    }
    free(workers);
    free(segs);

    if (any_error) {
        for (int fi = 0; fi < n_fields; fi++) {
            char spill_dir[PATH_MAX];
            snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                     db_root, object, descs[fi].name, fi);
            for (int w = 0; w < P; w++) {
                for (int s = 0; s < idx_n; s++) {
                    char path[PATH_MAX];
                    snprintf(path, sizeof(path), "%s/w%d_s%d.bin", spill_dir, w, s);
                    unlink(path);
                }
                char bm_path[PATH_MAX];
                snprintf(bm_path, sizeof(bm_path), "%s/bmw%d.bin", spill_dir, w);
                unlink(bm_path);
            }
            rmdir(spill_dir);
        }
        return index_build_failed(0);
    }

    /* Fail the whole invocation before the first rename if any tree spill is
       malformed or unreadable. Per-shard validation inside the parallel
       merge remains defense in depth, but cannot provide this publication
       ordering guarantee by itself. */
    for (int fi = 0; fi < n_fields; fi++) {
        if (descs[fi].type == MF_BITMAP) continue;
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        durability_test_pause(spill_dir, "idx-spills-before-merge");
        if (validate_index_spills(spill_dir, P, idx_n) != 0) {
            for (int cleanup_fi = 0; cleanup_fi < n_fields; cleanup_fi++) {
                char cleanup_dir[PATH_MAX];
                snprintf(cleanup_dir, sizeof(cleanup_dir),
                         "%s/%s/indexes/%s/.spill_%d", db_root, object,
                         descs[cleanup_fi].name, cleanup_fi);
                for (int w = 0; w < P; w++) {
                    for (int s = 0; s < idx_n; s++) {
                        char path[PATH_MAX];
                        snprintf(path, sizeof(path), "%s/w%d_s%d.bin",
                                 cleanup_dir, w, s);
                        unlink(path);
                    }
                    char bm_path[PATH_MAX];
                    snprintf(bm_path, sizeof(bm_path), "%s/bmw%d.bin",
                             cleanup_dir, w);
                    unlink(bm_path);
                }
                rmdir(cleanup_dir);
            }
            return index_build_failed(errno);
        }
    }

    /* Phase 2a: merge btree/trigram spills per field. Spill files are
       w{0..P-1}_s{shard}.bin, so the merge's "n_kf" arg is P. Bitmap fields
       are resolved separately in Phase 2b (resolve_bitmaps). */
    index_build_result merge_result = index_build_ok();
    for (int fi = 0; fi < n_fields; fi++) {
        if (descs[fi].type == MF_BITMAP) continue;
        char spill_dir[PATH_MAX];
        snprintf(spill_dir, sizeof(spill_dir), "%s/%s/indexes/%s/.spill_%d",
                 db_root, object, descs[fi].name, fi);
        MergeShardArg *margs = calloc((size_t)idx_n, sizeof(MergeShardArg));
        if (!margs) {
            merge_result.status = INDEX_BUILD_FAILED;
            merge_result.all_requested_shards_published = 0;
            continue;
        }
        for (int s = 0; s < idx_n; s++) {
            margs[s].type      = descs[fi].type;
            margs[s].db_root   = db_root;
            margs[s].object    = object;
            margs[s].field     = descs[fi].name;
            margs[s].idx_n     = idx_n;
            margs[s].n_kf      = P;
            margs[s].shard     = s;
            margs[s].spill_dir = spill_dir;
        }
        for (int b = 0; b < idx_n; b += pool_size) {
            int cnt = (b + pool_size <= idx_n) ? pool_size : (idx_n - b);
            parallel_for_io(merge_shard_worker_fn, margs + b, cnt, sizeof(MergeShardArg));
        }
        index_build_result field_result = index_build_ok();
        for (int s = 0; s < idx_n; s++)
            field_result = index_build_result_combine(field_result,
                                                      margs[s].rc);
        merge_result = index_build_result_combine(merge_result, field_result);
        free(margs);
        rmdir(spill_dir);
    }

    /* Phase 2b: resolve bitmap fields (kf hash→slot join). */
    index_build_result bm_result = resolve_bitmaps(db_root, object, sch, ts, sdb, descs, n_fields, P);

    return index_build_result_combine(merge_result, bm_result);
}

/*
 * build_indexes_streaming_multi — build every index for one object in ONE
 * sequential pass over the value store. btree/trigram/bitmap all extracted
 * during the single segment scan (seg_seq_build_spills); bitmap slots are
 * resolved from the keyfile afterwards (resolve_bitmaps).
 *
 * The object write lock protects the scan from concurrent mutations.
 */
static index_build_result build_indexes_streaming_multi(const char *db_root, const char *object,
                                          const Schema *sch, TypedSchema *ts,
                                          const MFFieldDesc *descs, int n_fields) {
    SlotcaskSchemaInfo info = {
        .splits = sch->splits, .slot_size = sch->slot_size, .streams = sch->streams,
    };
    SlotcaskDb *sdb = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        LOG_ERROR(LOG_SUB_REINDEX, "build_indexes_streaming_multi: slotcask_registry_get failed for %s/%s", db_root, object);
        return index_build_failed(0);
    }

    return seg_seq_build_spills(db_root, object, sch, ts, sdb, descs, n_fields);
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

/* Post-build cleanup for reindex: with the upfront wipe removed, indexes
   are published in place via bt_publish_replace/bm_publish_replace,
   so a partially-failed run always leaves the previous live shard intact.
   Once every requested shard has published, this sweeps
   only what the new layout can never reference again: a legacy pre-2026.05.1
   single-file <field>.idx, and numeric shard files left behind by a higher
   split count than the object's current index_splits_for(splits). It must
   not run before publication completes — see reindex_object_checked. */
static void reindex_cleanup_obsolete(const char *eff_root, const char *object,
                                     char (*field_specs)[512],
                                     const MFFieldDesc *descs, int nf,
                                     int bitmap_splits, int tree_splits) {
    char idx_root[PATH_MAX];
    snprintf(idx_root, sizeof(idx_root), "%s/%s/indexes", eff_root, object);

    for (int i = 0; i < nf; i++) {
        const char *extension;
        int live_shards;
        switch (descs[i].type) {
            case MF_BITMAP:
                extension = ".bm";
                live_shards = bitmap_splits;
                break;
            case STREAM_TRIGRAM:
                extension = ".tg";
                live_shards = tree_splits;
                break;
            case STREAM_BTREE:
                extension = ".idx";
                live_shards = tree_splits;
                break;
            default:
                continue;
        }
        char fname[512];
        strncpy(fname, field_specs[i], 511);
        fname[511] = '\0';
        char *colon = strchr(fname, ':');
        if (colon) *colon = '\0';

        char legacy[PATH_MAX];
        snprintf(legacy, sizeof(legacy), "%s/%s.idx", idx_root, fname);
        btree_cache_invalidate(legacy);
        if (unlink(legacy) != 0 && errno != ENOENT) {
            LOG_WARN(LOG_SUB_REINDEX,
                     "reindex cleanup: unlink %s failed: %s",
                     legacy, strerror(errno));
        }

        char fdir[PATH_MAX];
        snprintf(fdir, sizeof(fdir), "%s/%s", idx_root, fname);
        DIR *d = opendir(fdir);
        if (!d) continue;
        int dfd = dirfd(d);
        struct dirent *e;
        for (;;) {
            errno = 0;
            e = readdir(d);
            if (!e) {
                if (errno != 0)
                    LOG_WARN(LOG_SUB_REINDEX,
                             "reindex cleanup: readdir(%s) failed: %s",
                             fdir, strerror(errno));
                break;
            }
            size_t name_len = strlen(e->d_name);
            size_t extension_len = strlen(extension);
            if (name_len != 3 + extension_len) continue;
            if (strcmp(e->d_name + 3, extension) != 0) continue;

            char shard_text[4] = {
                e->d_name[0], e->d_name[1], e->d_name[2], '\0'
            };
            char *end = NULL;
            unsigned long shard = strtoul(shard_text, &end, 16);
            if (end != shard_text + 3 || shard > INT_MAX) continue;
            if ((int)shard < live_shards) continue;

            char sp[PATH_MAX];
            snprintf(sp, sizeof(sp), "%s/%s", fdir, e->d_name);
            if (descs[i].type == MF_BITMAP) bm_cache_invalidate(sp);
            else btree_cache_invalidate(sp);
            if (unlinkat(dfd, e->d_name, 0) != 0 && errno != ENOENT) {
                LOG_WARN(LOG_SUB_REINDEX,
                         "reindex cleanup: unlink %s failed: %s",
                         sp, strerror(errno));
            }
        }
        closedir(d);
    }
}

/* Rebuild every index for one object: read index.conf for the field list,
   publish replacement shards, then remove type-specific obsolete files in a single
   kf-scan via build_indexes_streaming_multi (one pass, all fields).
   Caller must hold objlock_wrlock(eff_root, object) — cmd_reindex takes it
   per-object; rebuild_object_v2 (vacuum) inherits it from the server dispatch.
   Checked form returns 0 on success and writes the rebuilt count; the
   compatibility wrapper below retains the historic count-or-zero result. */
static int reindex_object_checked_impl(const char *eff_root, const char *object,
                                       int composites_only, int *out_count,
                                       int *out_errno) {
    if (!out_count) return -1;
    *out_count = 0;
    char ic_path[PATH_MAX];
    snprintf(ic_path, sizeof(ic_path), "%s/%s/indexes/index.conf",
             eff_root, object);
    FILE *ic = fopen(ic_path, "r");
    if (!ic) {
        if (errno == ENOENT) return 0;
        if (out_errno) *out_errno = errno;
        return -1;
    }

    char (*field_specs)[512] = malloc((size_t)MAX_FIELDS * 512);
    if (!field_specs) {
        fclose(ic);
        if (out_errno) *out_errno = ENOMEM;
        return -1;
    }
    int nf = 0;
    char fline[512];
    while (fgets(fline, sizeof(fline), ic) && nf < MAX_FIELDS) {
        fline[strcspn(fline, "\n")] = '\0';
        if (!fline[0]) continue;
        if (composites_only && !strchr(fline, '+')) continue;
        strncpy(field_specs[nf], fline, 511);
        field_specs[nf][511] = '\0';
        nf++;
    }
    fclose(ic);
    if (nf == 0) { free(field_specs); return 0; }

    LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: starting (%d indexes%s)...",
             eff_root, object, nf, composites_only ? ", composites-only" : "");

    /* Build MFFieldDesc array: parse each index.conf line, resolve type
       (with the same auto-promotion logic as cmd_add_index), fill indices. */
    Schema sch = load_schema(eff_root, object);
    if (sch.splits <= 0 || sch.streams <= 0) {
        free(field_specs);
        LOG_ERROR(LOG_SUB_REINDEX, "REINDEX %s/%s: cannot load schema",
                  eff_root, object);
        if (out_errno) *out_errno = EINVAL;
        return -1;
    }
    TypedSchema *ts = load_typed_schema(eff_root, object);
    if (!ts) {
        free(field_specs);
        LOG_ERROR(LOG_SUB_REINDEX, "REINDEX %s/%s: cannot load typed schema", eff_root, object);
        if (out_errno) *out_errno = EINVAL;
        return -1;
    }

    MFFieldDesc *descs = calloc((size_t)nf, sizeof(MFFieldDesc));
    if (!descs) {
        free(field_specs);
        if (out_errno) *out_errno = ENOMEM;
        return -1;
    }
    int n_desc = 0;

    for (int i = 0; i < nf; i++) {
        MFFieldDesc *d = &descs[n_desc];
        ParsedIndexSpec ps;
        enum IndexType t = IT_BTREE;
        uint32_t max_values = 0;
        int is_composite = 0;
        const char *eff_name = field_specs[i];

        if (parse_index_spec(field_specs[i], &ps) == 0) {
            t = ps.type;
            max_values = ps.max_values;
            is_composite = ps.is_composite;
            eff_name = ps.name;
            if (!ps.is_composite) {
                int fi_t = typed_field_index(ts, ps.name);
                if (fi_t >= 0 && idx_should_auto_bitmap(ps.had_explicit_type,
                                                        ts->fields[fi_t].type)) {
                    t = IT_BITMAP;
                    if (ts->fields[fi_t].type == FT_ENUM &&
                        ts->fields[fi_t].enum_width == 2 && max_values == 0)
                        max_values = 65535;
                }
            }
        }

        strncpy(d->name, eff_name, 255); d->name[255] = '\0';
        d->bm_max_values = max_values;
        d->is_composite  = is_composite;

        if (t == IT_BITMAP) {
            d->type = MF_BITMAP;
            int fi_t = typed_field_index(ts, eff_name);
            d->field_indices[0]  = fi_t;
            d->field_index_count = 1;
            d->bm_bool_fastpath  = (fi_t >= 0 && ts->fields[fi_t].type == FT_BOOL) ? 1 : 0;
        } else if (t == IT_TRIGRAM) {
            d->type = STREAM_TRIGRAM;
            d->field_indices[0]  = typed_field_index(ts, eff_name);
            d->field_index_count = 1;
        } else {
            d->type = STREAM_BTREE;
            if (is_composite) {
                char fb[256]; strncpy(fb, eff_name, 255); fb[255] = '\0';
                char *save = NULL; d->field_index_count = 0;
                for (char *tok = strtok_r(fb, "+", &save);
                     tok && d->field_index_count < 16;
                     tok = strtok_r(NULL, "+", &save))
                    d->field_indices[d->field_index_count++] = typed_field_index(ts, tok);
            } else {
                d->field_indices[0]  = typed_field_index(ts, eff_name);
                d->field_index_count = 1;
            }
        }
        n_desc++;
    }

    index_build_result build_result = index_build_ok();
    if (n_desc > 0)
        build_result = build_indexes_streaming_multi(eff_root, object, &sch, ts,
                                                       descs, n_desc);

    if (build_result.status == INDEX_BUILD_FAILED) {
        free(descs);
        free(field_specs);
        LOG_ERROR(LOG_SUB_REINDEX, "REINDEX %s/%s: index build failed (errno=%d %s)",
                  eff_root, object, build_result.error_errno,
                  strerror(build_result.error_errno));
        if (out_errno) *out_errno = build_result.error_errno;
        return -1;
    }
    if (build_result.all_requested_shards_published)
        reindex_cleanup_obsolete(eff_root, object, field_specs, descs, nf,
                                 sch.splits, index_splits_for(sch.splits));
    free(descs);
    free(field_specs);
    *out_count = nf;
    LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: rebuilt %d indexes", eff_root, object, nf);
    return 0;
}

int reindex_object_checked(const char *eff_root, const char *object,
                           int composites_only, int *out_count) {
    return reindex_object_checked_impl(eff_root, object, composites_only,
                                       out_count, NULL);
}

int reindex_object_checked_ex(const char *eff_root, const char *object,
                              int composites_only, int *out_count,
                              int *out_errno) {
    return reindex_object_checked_impl(eff_root, object, composites_only,
                                       out_count, out_errno);
}

int reindex_object(const char *eff_root, const char *object,
                   int composites_only) {
    int count = 0;
    return reindex_object_checked(eff_root, object, composites_only, &count) == 0
               ? count : 0;
}

/* Legacy single-file sweep — kept for cmd_reindex's per-object loop, now
   superseded by reindex_cleanup_obsolete's post-publication sweep, but
   documented separately so the upgrade path stays clear. */
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
            LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: cleaned legacy single-file index %s",
                    eff_root, object, e->d_name);
        }
    }
    closedir(d);
}

int cmd_reindex(const char *db_root, const char *dir_filter, const char *obj_filter, int composites_only) {
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
    int objects_failed = 0;
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

        /* Exclusive lock for the full per-shard replacement cycle: inserts
           arriving while replacement shards are being materialised could
           otherwise update the live generation and be absent from a later
           published shard. The lock queues them until reindex completes.
           rebuild_object_v2 (vacuum) holds
           this lock already via the server dispatch; reindex must take it
           explicitly here since it bypasses that dispatch path. */
        objlock_wrlock(eff_root, obj);
        int n = 0;
        int rebuild_rc = reindex_object_checked(eff_root, obj,
                                                composites_only, &n);
        objlock_wrunlock(eff_root, obj);
        if (rebuild_rc != 0) {
            objects_failed++;
        } else if (n > 0) {
            objects_rebuilt++;
            indexes_rebuilt += n;
        } else {
            objects_skipped++;
        }
    }
    fclose(sf);

    uint64_t t1 = now_ms_coarse();
    if (objects_failed > 0) {
        OUT("{\"error\":\"reindex failed\",\"objects\":%d,\"failed\":%d,"
            "\"skipped\":%d,\"indexes\":%d,\"duration_ms\":%llu}\n",
            objects_rebuilt, objects_failed, objects_skipped, indexes_rebuilt,
            (unsigned long long)(t1 - t0));
        return -1;
    }
    OUT("{\"status\":\"reindexed\",\"objects\":%d,\"skipped\":%d,\"indexes\":%d,\"duration_ms\":%llu}\n",
        objects_rebuilt, objects_skipped, indexes_rebuilt,
        (unsigned long long)(t1 - t0));
    return 0;
}
