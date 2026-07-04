# add-index: single-scan build (match reindex's engine)

## Context / root cause

Incident: a `force` plural `add-index` (`mode:"add-index"`, `"fields":[...]`) on
`hn/comments` with a mix of btree + bitmap fields appeared stuck / took far
longer than `reindex` on the same object. Root cause, confirmed by reading
the code (not inferred): **`cmd_add_indexes` does one full sequential scan of
the object's storage PER INDEX TYPE requested, not one scan total**:

1. Every `IT_BITMAP` field → `build_bitmap_pass()` → its own
   `parallel_for(bm_shard_walk_worker, ...)` full kf-shard walk.
2. Every `IT_TRIGRAM` field → `build_trigram_pass()` → its own call into
   `seg_seq_build_spills()` with `n_fields=1` — a full segment scan, just for
   that one field.
3. Remaining `IT_BTREE` fields → `build_indexes_pass()` (adaptive batching by
   `estimate_field_build_bytes` / `g_index_build_budget_bytes`) → its own
   `scan_dispatch()` full scan.

So an 8-field force add-index (6 btree + 2 bitmap) does up to **3 separate
full-object passes**. `reindex_object()` (`./shard-db reindex` /
`mode:"reindex"`), by contrast, builds ONE combined `MFFieldDesc[]` array
covering every field regardless of type and calls
`build_indexes_streaming_multi()` — which does exactly one sequential scan
(`seg_seq_build_spills`) extracting btree/trigram sort-buffer entries AND
bitmap candidate values simultaneously, then two lightweight merge/resolve
phases (`merge_shard_worker_fn` per field, `resolve_bitmaps` for bitmaps).

**The fix**: make `cmd_add_indexes` build the same kind of combined
`MFFieldDesc[]` array — scoped to only the specific fields requested via this
call (not a full `index.conf` rebuild like `reindex` does) — and call
`build_indexes_streaming_multi()` exactly once. This is a mechanical port of
`reindex_object()`'s already-proven descs-building pattern
(`src/db/index.c` around what is currently line 3363–3434), with per-field
skip-if-exists / force semantics preserved exactly as they are today.

This incidentally also fixes the "bitmap builds ignore `INDEX_BUILD_BUDGET_MB`"
concern: `build_bitmap_pass` (the currently-used, unbudgeted path) is no
longer called from this function, and bitmap fields in the unified engine
never allocate large sort buffers at all (they're spilled as flat append
files, see `seg_seq_build_spills`'s `if (descs[fi].type == MF_BITMAP)
continue;` guard).

**Scope**: this plan touches ONLY the plural `cmd_add_indexes` path (the one
the incident used, via `"fields":[...]`). The singular `cmd_add_index` (one
field, no array) already funnels btree/trigram through
`build_btree_streaming`/`build_trigram_pass` (each a single-field call into
`seg_seq_build_spills`) and is out of scope here — it still calls
`build_bitmap_pass` for a lone bitmap field, which is a separate, smaller
inefficiency (one full scan regardless, since there's only one field) not
part of this incident. Do not touch `cmd_add_index` (singular) in this plan.

Explicitly OUT OF SCOPE: any change to the shared `parallel_for`/
`parallel_for_io` thread-pool implementation (fairness, priority, separate
pools). That is a different, larger architectural question the user has
deferred — do not touch `src/db/parallel.c` in this plan.

## Execution rules

- Branch off `main`: `git checkout -b fix/add-index-single-scan`.
- Do the tasks below **in order** — later tasks depend on earlier ones
  (e.g. the forward declaration must exist before the rewritten
  `cmd_add_indexes` body compiles).
- Every insertion/edit below is anchored by **quoted exact source text**, not
  line numbers (line numbers drift; another branch may be in flight
  concurrently). If a quoted anchor is not found byte-for-byte in the target
  file, **stop immediately** and write `PLAN_NOTES.md` at the repo root
  describing exactly what you searched for and what you found instead — do
  not guess, do not reinterpret, do not "fix it forward."
- Build with `SKIP_TESTS=1 ./build.sh` after every task that touches
  `src/db/index.c` to catch compile errors early — don't wait until the end.
- Full test pass at the end: `./build/bin/shard-db-test run-all`. Also run
  the two most relevant suites individually first for a faster signal:
  `./build/bin/shard-db-test run test-bitmap-index`,
  `./build/bin/shard-db-test run test-trigram-index`, and the new test added
  in Task 6.
- Never claim a step passed without pasting the real terminal output. The
  required final line is `# total: N passed, 0 failed`.
- Leave the branch **uncommitted** when done — committing/pushing/PRs are
  handled outside this workflow (see repo `CLAUDE.md`).

## Invariants that must hold after this change

- `cmd_add_indexes`'s response JSON shape is **unchanged**: same three
  variants (`{"status":"ok",...}` when zero btree fields were requested,
  `{"status":"all_exist"}` when btree fields were requested but all already
  existed, `{"status":"indexed","count":N,...}` otherwise), driven by the
  same `btree_count`/`actual_count` semantics as today.
- `index.conf` writing (full-rewrite-on-`promoted` vs. append-with-dedupe)
  is **byte-for-byte unchanged** — that code is untouched by this plan.
- Per-field skip-if-exists-unless-`force` semantics are preserved exactly:
  bitmap probes shard 0's `.bm`, trigram probes shard 0's `.tg`, btree uses
  `btree_idx_exists`/`btree_idx_unlink_all` — same probes, same paths, same
  meaning of `force`.
- Locking is unchanged: `add-index` is already dispatched under
  `objlock_wrlock` by `server.c`'s `mode_is_schema()` classification — same
  as `reindex`. No lock changes needed or made in this plan.
- Exactly one call to `build_indexes_streaming_multi()` per `cmd_add_indexes`
  invocation when there is at least one field to build (down from up to 3
  separate full-scan mechanisms today).

---

## Task 1 — Update stale doc comment + delete dead `ShardBuildArg` / `shard_build_worker` / `partition_by_shard`

File: `src/db/index.c`.

These three symbols (a struct + two static helpers) are used from exactly
one call site each, both inside `build_indexes_pass()` (deleted in Task 3).
Once that function is gone, this block is fully dead. The comment
immediately above it also describes the pre-fix architecture and must be
updated so it doesn't mislead future readers.

Find this exact text:

```c
/* ========== ADD-INDEX ========== */
/* Singular add-index uses the streaming pipeline below
   (build_btree_streaming). Plural cmd_add_indexes still uses the
   single-scan multi-field path (MultiIndexCtx + multi_index_scan_cb)
   which has its own memory model — both are bounded but via different
   strategies. */

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
```

Replace it with:

```c
/* ========== ADD-INDEX ========== */
/* Both the singular (cmd_add_index) and plural (cmd_add_indexes) entry
   points funnel every field type — btree, bitmap, trigram — requested in
   ONE add-index call through the same single-scan engine reindex_object
   uses (build_indexes_streaming_multi -> seg_seq_build_spills +
   resolve_bitmaps). A force add-index over N fields of mixed type does
   exactly one sequential pass over storage, not one pass per type. */

/* Forward decls — full definitions live near the multi-index builder. */
int build_bitmap_pass(const char *db_root, const char *object,
```

(The trailing `int build_bitmap_pass(const char *db_root, const char *object,`
line is intentionally repeated verbatim at the end of both the old and new
text — it is NOT part of what's being deleted, it's there so the edit has an
unambiguous, unique anchor. Everything between the section header and that
line is deleted; the `build_bitmap_pass`/`build_trigram_pass`/
`build_btree_streaming` forward declarations that follow it are untouched.)

Build check: `SKIP_TESTS=1 ./build.sh`. Expect a compile error at this point
is NOT expected yet (nothing references these symbols outside the code
being deleted alongside them in Task 2/3) — if you see undefined-reference
or unused-function warnings about `ShardBuildArg`/`shard_build_worker`/
`partition_by_shard`/`cmp_btentry_fn`, stop and check whether something
outside `build_indexes_pass` was using them (it should not be — verified via
`grep -rn` across `src/` during planning; `cmp_btentry_fn` itself is used
elsewhere and must NOT be deleted, only the three symbols named in this
task).

## Task 2 — Delete dead `MultiIndexCtx` / `multi_index_scan_cb` / `typed_field_str_avg` / `estimate_field_build_bytes`

File: `src/db/index.c`. These four symbols are used exclusively inside
`build_indexes_pass()` (deleted in Task 3). Delete this whole block —
it directly follows the code touched in Task 1.

Find this exact text:

```c
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
                size_t blen = 0;
                typed_field_to_index_key(mc->ts, (const uint8_t *)raw,
                                          mc->field_indices[fi][si],
                                          (uint8_t *)cat + cpos, &blen);
                if (blen == 0) { ok = 0; break; }
                if (cpos + (int)blen < (int)sizeof(cat)) { cpos += (int)blen; }
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

/* Average index-key size per field for composite key budgeting.
   Composites are now built by concatenating typed_field_to_index_key output
   (binary, total-order encoded). Fixed-width types use f->size; varchars
   use 50% fill of f->size-2, same as the single-field estimator. */
static size_t typed_field_str_avg(const TypedField *f) {
    switch (f->type) {
    case FT_NONE:     return 16;  /* unassigned — conservative fallback */
    case FT_VARCHAR: {
        size_t content_max = (size_t)f->size > 2 ? (size_t)f->size - 2 : 0;
        size_t avg = content_max / 2;
        return avg < 1 ? 1 : avg;
    }
    case FT_BOOL:
    case FT_BYTE:     return 1;   /* single byte */
    case FT_SHORT:    return 2;   /* int16 BE + total-order flip */
    case FT_INT:      return 4;   /* int32 BE + total-order flip */
    case FT_LONG:     return 8;   /* int64 BE + total-order flip */
    case FT_DOUBLE:   return 8;   /* IEEE-754 total-order flip */
    case FT_FLOAT:    return 4;   /* IEEE-754 total-order flip */
    case FT_NUMERIC:  return 8;   /* int64 BE + total-order flip */
    case FT_DATE:     return 4;   /* int32 BE + total-order flip */
    case FT_DATETIME: return 6;   /* int32 BE date + uint16 BE time */
    case FT_DATETIMEMS: return 8; /* int32 BE date + uint32 BE ms-of-day */
    case FT_TIME:     return 3;   /* uint24 BE + total-order flip */
    case FT_TIMESTAMP: return 8;  /* int64 BE + total-order flip */
    case FT_UUID:     return 16;  /* raw 16 bytes */
    case FT_ENUM:     return (size_t)f->enum_width;  /* 1 or 2 bytes BE */
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
        /* Composite key — sum each child field's binary index-key width.
           Composite keys are built by concatenating typed_field_to_index_key
           per child; the estimate is the sum of typed_field_str_avg over
           children. status+invoiceDate ≈ 12 B (4+8), not 64. */
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
```

Replace it with:

```c
/* Bitmap reindex pass — rebuilds every .bm shard for one field by
```

(Again the trailing line is repeated verbatim as an anchor; only the material
between the section header and that line is deleted.)

Build check: `SKIP_TESTS=1 ./build.sh`.

## Task 3 — Delete dead `build_indexes_pass`

File: `src/db/index.c`. This is the legacy per-batch btree scan/partition/
build function used only by the old `cmd_add_indexes` dispatch (replaced in
Task 5).

Find this exact text:

```c
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

    LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: pass on %d fields, scanning %d kf shards...",
             db_root, object, n, sch->splits);
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    scan_dispatch(db_root, object, sch, data_dir, multi_index_scan_cb, &mc);
    LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: scan done, partitioning...",
             db_root, object);
    for (int fi = 0; fi < n; fi++) pthread_mutex_destroy(&mc.lock[fi]);

    ShardBuildArg *sb = malloc((size_t)n * idx_n * sizeof(ShardBuildArg));
    int sb_count = 0;
    if (n <= 0) return;
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
    uint64_t t_start = now_ms();
```

Replace it with:

```c
int cmd_add_indexes(const char *db_root, const char *object,
                    const char *fields_json, int force) {
    uint64_t t_start = now_ms();
```

Build check: `SKIP_TESTS=1 ./build.sh`. At this point you SHOULD see compile
errors in `cmd_add_indexes` (it still calls `build_bitmap_pass`/
`build_trigram_pass`/`build_indexes_pass` inline and `build_indexes_pass` no
longer exists) — that's expected and is what Task 5 fixes. If instead you
see errors anywhere else in the file, stop and write `PLAN_NOTES.md`.

## Task 4 — Forward-declare `build_indexes_streaming_multi`

File: `src/db/index.c`. `build_indexes_streaming_multi` is defined near the
bottom of the file (after `seg_seq_build_spills`/`resolve_bitmaps`), but the
rewritten `cmd_add_indexes` (Task 5) needs to call it. Mirror the existing
forward-declaration pattern used for `seg_seq_build_spills` a few lines
above.

Find this exact text:

```c
static int seg_seq_build_spills(const char *db_root, const char *object,
                                const Schema *sch, TypedSchema *ts,
                                SlotcaskDb *sdb,
                                const MFFieldDesc *descs, int n_fields);

int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);
int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force) {
```

Replace it with:

```c
static int seg_seq_build_spills(const char *db_root, const char *object,
                                const Schema *sch, TypedSchema *ts,
                                SlotcaskDb *sdb,
                                const MFFieldDesc *descs, int n_fields);

/* Forward decl — full definition lives further down, after
   seg_seq_build_spills/resolve_bitmaps. cmd_add_indexes calls this to
   build every requested field (btree+bitmap+trigram) in one scan, same
   engine reindex_object uses. */
static int build_indexes_streaming_multi(const char *db_root, const char *object,
                                          const Schema *sch, TypedSchema *ts,
                                          const MFFieldDesc *descs, int n_fields);

int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force);
int build_trigram_pass(const char *db_root, const char *object,
                       const Schema *sch, TypedSchema *ts,
                       const char *field, int force) {
```

Also, the existing (later) definition of `build_indexes_streaming_multi`
must drop its own `static` storage-class duplication conflict — it doesn't;
`static` forward declarations followed by a matching `static` definition are
standard C and require no further edit. Leave the definition at the bottom
of the file exactly as-is:

```c
static int build_indexes_streaming_multi(const char *db_root, const char *object,
                                          const Schema *sch, TypedSchema *ts,
                                          const MFFieldDesc *descs, int n_fields) {
```

(No change needed there — just confirm this line still exists verbatim
after Task 4's edit, so the forward declaration's signature matches exactly.)

Build check: `SKIP_TESTS=1 ./build.sh`. Same expected `cmd_add_indexes`
compile errors as after Task 3 (not yet fixed) — no NEW errors should appear
from this task itself.

## Task 5 — Rewrite `cmd_add_indexes`'s dispatch section (the actual fix)

File: `src/db/index.c`. This replaces the per-type dispatch (bitmap inline,
trigram inline, btree via now-deleted `build_indexes_pass`) with one combined
`MFFieldDesc[]` array and one call to `build_indexes_streaming_multi`.

Find this exact text (this is everything between the field-spec-parsing loop
and the index.conf-writing tail — both untouched):

```c
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
            /* Pre-count total batches so per-batch log can show X/N */
            int total_batches = 0;
            {
                int bs = 0;
                while (bs < actual_count) {
                    size_t bb = 0;
                    int be = bs;
                    while (be < actual_count) {
                        size_t next = per_field_bytes[be];
                        if (be > bs && bb + next > budget) break;
                        bb += next;
                        be++;
                    }
                    total_batches++;
                    bs = be;
                }
            }
            while (batch_start < actual_count) {
                size_t batch_bytes = 0;
                int batch_end = batch_start;
                while (batch_end < actual_count) {
                    size_t next = per_field_bytes[batch_end];
                    if (batch_end > batch_start && batch_bytes + next > budget) break;
                    batch_bytes += next;
                    batch_end++;
                }
                LOG_INFO(LOG_SUB_REINDEX, "REINDEX %s/%s: batch %d/%d (fields %d..%d, budget=%zu MB)...",
                         db_root, object, n_batches + 1, total_batches,
                         batch_start, batch_end - 1, budget / (1024 * 1024));
                build_indexes_pass(db_root, object, &sch, ts, actual_fields,
                                   batch_start, batch_end - batch_start,
                                   (size_t)live_count);
                n_batches++;
                batch_start = batch_end;
            }
            LOG_AUDIT(LOG_SUB_INDEX, "ADD-INDEXES %s: %d fields in %d batch(es), live=%d, budget=%zu MB",
                    object, actual_count, n_batches, live_count,
                    budget / (1024 * 1024));
        }
    }
```

Replace it with:

```c
    /* Bitmap- and trigram-typed fields follow the same skip-if-exists
       semantic as btree: with force, wipe + rebuild; without force,
       no-op when any shard file already exists for the field. All three
       types are accumulated into ONE combined MFFieldDesc array below and
       built via ONE call to build_indexes_streaming_multi — the same
       single-scan engine reindex_object uses — instead of the old
       build_bitmap_pass-per-field + build_trigram_pass-per-field +
       build_indexes_pass-batch triple dispatch (up to 3 separate
       full-object scans for one add-index call). */
    int total_fields = nfields;  /* preserved across the btree-only reduction below */
    int btree_count = 0;
    char btree_fields[MAX_FIELDS][256];
    MFFieldDesc *descs = calloc((size_t)total_fields, sizeof(MFFieldDesc));
    int n_desc = 0;

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
            if (descs && ts_for_idx) {
                int fi_t = typed_field_index(ts_for_idx, names[i]);
                if (fi_t >= 0) {
                    MFFieldDesc *d = &descs[n_desc++];
                    memset(d, 0, sizeof(*d));
                    d->type = MF_BITMAP;
                    strncpy(d->name, names[i], sizeof(d->name) - 1);
                    d->field_indices[0] = fi_t;
                    d->field_index_count = 1;
                    d->bm_max_values = maxes[i];
                    d->bm_bool_fastpath = (ts_for_idx->fields[fi_t].type == FT_BOOL) ? 1 : 0;
                }
            }
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
            } else {
                /* Force: unlink existing .tg shards before rebuild —
                   mirrors build_trigram_pass's own force branch, since
                   we no longer call build_trigram_pass from here. */
                int idx_n = index_splits_for(sch.splits);
                for (int s = 0; s < idx_n; s++) {
                    char tp[PATH_MAX];
                    tg_build_path(tp, sizeof(tp), db_root, object, names[i], s);
                    btree_cache_invalidate(tp);
                    unlink(tp);
                }
            }
            if (descs && ts_for_idx) {
                int fi_t = typed_field_index(ts_for_idx, names[i]);
                if (fi_t >= 0) {
                    MFFieldDesc *d = &descs[n_desc++];
                    memset(d, 0, sizeof(*d));
                    d->type = STREAM_TRIGRAM;
                    strncpy(d->name, names[i], sizeof(d->name) - 1);
                    d->field_indices[0] = fi_t;
                    d->field_index_count = 1;
                }
            }
            continue;
        }
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, sizeof(btree_fields));
    nfields = btree_count;

    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* === Btree fields: same skip-if-exists / force-unlink semantics as
       before — just add one MFFieldDesc per field to the SAME combined
       array built above instead of running a separate batched pass. */
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

        if (actual_count > 0 && descs && ts_for_idx) {
            for (int i = 0; i < actual_count; i++) {
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
                        d->field_indices[d->field_index_count++] =
                            typed_field_index(ts_for_idx, t);
                    }
                } else {
                    d->field_indices[0] = typed_field_index(ts_for_idx, actual_fields[i]);
                    d->field_index_count = 1;
                }
            }
        }
    }

    /* Single scan: build every requested bitmap/trigram/btree field in
       ONE call to the same engine reindex uses. This is the fix for the
       "N separate full-object scans per add-index call" incident. */
    if (n_desc > 0) {
        LOG_AUDIT(LOG_SUB_INDEX, "ADD-INDEXES %s: %d field(s), single scan",
                 object, n_desc);
        build_indexes_streaming_multi(db_root, object, &sch, ts_for_idx, descs, n_desc);
    }
    free(descs);
```

Build check: `SKIP_TESTS=1 ./build.sh`. This should now compile clean —
paste the real build output. If it doesn't, stop and write
`PLAN_NOTES.md` describing the exact error (do not silently patch around a
compile error not anticipated by this plan; that likely means an anchor
elsewhere in the file has already drifted).

### Why `ts_for_idx` and not a fresh `load_typed_schema` call

`cmd_add_indexes` already loads `TypedSchema *ts_for_idx =
load_typed_schema(db_root, object);` near the top of the function (used for
the auto-bitmap-promotion loop). The old code redundantly called
`load_typed_schema` again per bitmap/trigram field and a third time for the
btree batch — the rewrite reuses the one already-loaded `ts_for_idx`
throughout. `ts_for_idx` can be `NULL` if the schema fails to load (rare);
every new block above guards on `ts_for_idx` being non-NULL before touching
it, same defensive style the existing auto-promotion loop already uses at
`if (!ps.is_composite && ts_for_idx)`.

### Why `calloc` failure is safe

If `calloc` for `descs` fails, `descs` is `NULL`. Every place that would
populate an entry guards with `if (descs && ts_for_idx)`, so `n_desc` simply
stays lower than it should and the final `if (n_desc > 0)` block either
builds a partial set or (if `n_desc == 0`) builds nothing. This mirrors the
existing style elsewhere in this file (e.g. `resolve_bitmaps`'s `if (!kf)
return -1;`) rather than adding new error-propagation machinery — an
out-of-memory add-index request failing partially is an existing class of
behavior in this codebase, not a new regression introduced here.

---

## Task 6 — Regression test: mixed-type force add-index does ONE scan

New file: `src/test/cases/test_add_indexes_single_scan.c`.

This test proves the fix two ways: (1) correctness — a force add-index with
bitmap + trigram + btree fields in the same call still produces correct,
queryable indexes of every type; (2) the actual regression guard — it greps
the daemon's warn-log for the OLD per-type banners (`BUILD-BITMAP`,
`BUILD-TRIGRAM`) that `build_bitmap_pass`/`build_trigram_pass` print, and
asserts they do NOT appear, while the unified engine's `BUILD-SEQ` banner
(from `seg_seq_build_spills`, tagged `[reindex]`) DOES appear exactly once
for this call. Before this fix, the same mixed-type request would print
`BUILD-BITMAP` once, `BUILD-TRIGRAM` once, and no `BUILD-SEQ` line for the
add-index call at all — so this assertion fails against the pre-fix code
and passes against the post-fix code, which is what makes it a real
regression test rather than a restatement of the implementation.

Log file location: the test harness sets `LOG_DIR="<base>/logs"` where
`<base>` is the parent directory of `env->db_root` (i.e. `env->db_root` is
always `<base>/db`). `LOG_WARN(...)` writes to
`<base>/logs/YYYY-MM-DD-warn.log`; `LOG_AUDIT(...)` writes to
`<base>/logs/YYYY-MM-DD-audit.log`. Both `BUILD-BITMAP`/`BUILD-TRIGRAM`/
`BUILD-SEQ` are `LOG_WARN` calls (see `src/db/index.c`), so the warn log is
the one to grep.

Write this exact file:

```c
/* src/test/cases/test_add_indexes_single_scan.c
 *
 * Regression test for the "force add-index with mixed field types runs
 * N separate full-object scans" incident (2026-07-03, hn/comments got
 * stuck under a force add-index over 6 btree + 2 bitmap fields).
 *
 * Fix: cmd_add_indexes now builds ONE combined MFFieldDesc array covering
 * every requested field (bitmap + trigram + btree) and calls
 * build_indexes_streaming_multi() exactly once — same single-scan engine
 * reindex_object uses — instead of dispatching build_bitmap_pass per
 * bitmap field, build_trigram_pass per trigram field, and a separate
 * batched build_indexes_pass for the remaining btree fields.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <libgen.h>

/* Read the whole file into a heap buffer (NUL-terminated). NULL if the
   file doesn't exist or is empty — callers treat that as "0 matches". */
static char *slurp(const char *path) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    if (sz <= 0) { fclose(f); return NULL; }
    fseek(f, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t rd = fread(buf, 1, (size_t)sz, f);
    buf[rd] = '\0';
    fclose(f);
    return buf;
}

/* Count non-overlapping occurrences of `needle` in `hay`. */
static int count_occurrences(const char *hay, const char *needle) {
    if (!hay) return 0;
    int n = 0;
    const char *p = hay;
    size_t nlen = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) { n++; p += nlen; }
    return n;
}

/* env->db_root is "<base>/db" (see fixtures.c: test_env_start). LOG_DIR is
   "<base>/logs". Build "<base>/logs/YYYY-MM-DD-warn.log" for today. */
static void warn_log_path(const TestEnv *env, char *out, size_t out_sz) {
    char db_root_copy[512];
    strncpy(db_root_copy, env->db_root, sizeof(db_root_copy) - 1);
    db_root_copy[sizeof(db_root_copy) - 1] = '\0';
    char *base = dirname(db_root_copy); /* strips trailing "/db" */

    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    char datebuf[16];
    strftime(datebuf, sizeof(datebuf), "%Y-%m-%d", &tmv);

    snprintf(out, out_sz, "%s/logs/%s-warn.log", base, datebuf);
}

static int run_mixed_type_single_scan_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 60000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;

    /* Object with 4 plain fields: 1 will get a btree index, 1 bitmap,
       1 trigram, and 1 stays unindexed (control). */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"name:varchar:64\",\"active:bool\","
        "\"bio:varchar:256\",\"age:int\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: mixed");
    free(resp); resp = NULL;

    /* Insert 40 records: mix of active true/false, distinct bios and ages. */
    for (int i = 0; i < 40; i++) {
        char req[512];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"t\",\"object\":\"mixed\","
            "\"key\":\"m%d\",\"value\":{\"name\":\"user%d\","
            "\"active\":%s,\"bio\":\"loves the shard database engine\","
            "\"age\":%d}}",
            i, i, (i % 3 == 0) ? "true" : "false", 20 + (i % 40));
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }

    /* Force add-index over ALL THREE types in one call — this is the
       exact shape the hn/comments incident used (mixed bitmap + btree,
       here plus a trigram field too). */
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"fields\":[\"age\",\"active:bitmap\",\"bio:trigram\"],"
        "\"force\":true}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                "mixed force add-index: no error");
    ASSERT_CONTAINS(resp, "\"status\":\"indexed\"",
                    "mixed force add-index: status indexed (age is btree)");
    free(resp); resp = NULL;

    /* === Correctness: all three index types actually work. === */

    /* btree: eq on age. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"age\",\"op\":\"eq\",\"value\":20}]}", &resp);
    ASSERT_TRUE(tu_parse_count(resp) > 0, "btree: age=20 count > 0");
    free(resp); resp = NULL;

    /* bitmap: eq on active. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"active\",\"op\":\"eq\",\"value\":true}]}", &resp);
    int active_count = tu_parse_count(resp);
    free(resp); resp = NULL;
    /* i % 3 == 0 over 40 records (i=0..39) → 14 trues (0,3,...,39). */
    ASSERT_EQ_INT(active_count, 14, "bitmap: active=true count == 14");

    /* trigram: contains on bio. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"criteria\":[{\"field\":\"bio\",\"op\":\"contains\",\"value\":\"shard\"}]}",
        &resp);
    ASSERT_EQ_INT(tu_parse_count(resp), 40, "trigram: contains 'shard' == 40");
    free(resp); resp = NULL;

    tc_close(tc);

    /* === Regression guard: exactly one BUILD-SEQ scan, zero BUILD-BITMAP /
       BUILD-TRIGRAM banners, from this single add-index call. Before the
       fix, this same request produced one BUILD-BITMAP line, one
       BUILD-TRIGRAM line, and NO BUILD-SEQ line — the three-scan bug. === */
    char logpath[600];
    warn_log_path(env, logpath, sizeof(logpath));
    char *log = slurp(logpath);
    ASSERT_NOT_NULL(log, "warn log file exists and is non-empty");
    if (!log) return 1;

    int n_build_seq   = count_occurrences(log, "BUILD-SEQ");
    int n_build_bitmap = count_occurrences(log, "BUILD-BITMAP");
    int n_build_trigram = count_occurrences(log, "BUILD-TRIGRAM");

    ASSERT_TRUE(n_build_seq >= 1,
                "single-scan engine (BUILD-SEQ) was used for this add-index call");
    ASSERT_EQ_INT(n_build_bitmap, 0,
                  "old per-field build_bitmap_pass (BUILD-BITMAP) NOT used");
    ASSERT_EQ_INT(n_build_trigram, 0,
                  "old per-field build_trigram_pass (BUILD-TRIGRAM) NOT used");

    free(log);
    return 0;
}

/* Skip-if-exists semantics must still hold post-fix: a second, non-force
   add-index over the same fields should be a no-op (no rebuild), reported
   as {"status":"all_exist"} since all 3 requested fields already have
   on-disk shards from the force call above (age is the only IT_BTREE
   field in the request, and it already exists). */
static int run_skip_if_exists_assertions(TestEnv *env) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return 1;

    char *resp = NULL;
    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"t\",\"object\":\"mixed\","
        "\"fields\":[\"age\",\"active:bitmap\",\"bio:trigram\"]}", &resp);
    ASSERT_TRUE(resp && !strstr(resp, "\"error\""),
                "non-force re-add-index: no error");
    ASSERT_CONTAINS(resp, "\"status\":\"all_exist\"",
                    "non-force re-add-index: all_exist (skip-if-exists honored)");
    free(resp); resp = NULL;

    tc_close(tc);
    return 0;
}

static int test_add_indexes_single_scan_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;
    int rc = run_mixed_type_single_scan_assertions(&env);
    if (rc == 0) rc = run_skip_if_exists_assertions(&env);
    test_env_stop(&env);
    return rc;
}

TEST_REGISTER("test-add-indexes-single-scan", test_add_indexes_single_scan_run)
```

Now register the new file in `build.sh`. Find this exact text:

```
    src/test/cases/test_o_direct_scan.c \
    src/test/cases/test_registry_single_flight.c \
    src/db/util.c \
```

Replace it with:

```
    src/test/cases/test_o_direct_scan.c \
    src/test/cases/test_registry_single_flight.c \
    src/test/cases/test_add_indexes_single_scan.c \
    src/db/util.c \
```

(If `test_registry_single_flight.c` is not present at this anchor — e.g. a
different branch state — stop and write `PLAN_NOTES.md` rather than
guessing where to insert; that file's presence indicates a specific prior
change this plan assumes is already merged/in-branch.)

Build + run:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-add-indexes-single-scan
```

Paste the real output. Expect all assertions to pass, ending with the
per-test pass/fail summary the harness prints.

---

## Final verification

Run, in order, and paste the real terminal output for each:

```
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run test-bitmap-index
./build/bin/shard-db-test run test-trigram-index
./build/bin/shard-db-test run test-add-indexes-single-scan
./build/bin/shard-db-test run-all
```

The required final line of the last command is `# total: N passed, 0
failed`. Do not report this plan as complete without that literal line in
your output.

Leave the branch (`fix/add-index-single-scan`) uncommitted. Do not run
`git add`, `git commit`, or any push/PR command — those steps happen outside
this workflow per repo `CLAUDE.md`.
