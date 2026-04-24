#include "types.h"

/* File-scope worker for parallel index builds in cmd_add_indexes */
typedef struct { char ipath[PATH_MAX]; BtEntry *pairs; size_t pair_count; } IdxBuildArgIdx;
static void *idx_build_worker_idx(void *arg) {
    IdxBuildArgIdx *ib = (IdxBuildArgIdx *)arg;
    qsort(ib->pairs, ib->pair_count, sizeof(BtEntry), cmp_btentry_fn);
    btree_bulk_build(ib->ipath, ib->pairs, ib->pair_count);
    return NULL;
}

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


/* Wrapper for insert-time indexing — uses B+ tree */
void write_index_entry(const char *db_root, const char *object,
                              const char *field,
                              const uint8_t *val, size_t vlen,
                              const uint8_t hash16[16]) {
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);
    mkdirp(dirname_of(idx_path));
    btree_insert(idx_path, (const char *)val, vlen, hash16);
}

/* Delete from index — uses B+ tree */
void delete_index_entry(const char *db_root, const char *object,
                               const char *field,
                               const uint8_t *val, size_t vlen,
                               const uint8_t hash16[16]) {
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);
    btree_delete(idx_path, (const char *)val, vlen, hash16);
}

/* ========== Parallel indexing ========== */

typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    uint8_t *val;               /* heap-owned bytes (index-key encoding); freed by caller */
    size_t vlen;
    const uint8_t *hash16;
} IndexThreadArg;

void *index_thread_fn(void *arg) {
    IndexThreadArg *a = (IndexThreadArg *)arg;
    write_index_entry(a->db_root, a->object, a->field, a->val, a->vlen, a->hash16);
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

void index_parallel(const char *db_root, const char *object,
                           const char *value, const uint8_t hash16[16],
                           char fields[][256], int nfields) {
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

    /* Untyped — passthrough raw bytes. */
    size_t sl = strlen(txt);
    *out_val = malloc(sl);
    memcpy(*out_val, txt, sl);
    *out_len = sl;
    free(txt);
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
    return 0;
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
        if (count >= cap) { cap *= 2; lines = realloc(lines, cap * sizeof(char *)); }
        lines[count++] = strdup(buf);
    }
    fclose(f);

    if (count == 0) { free(lines); return; }

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

/* Recursively remove a directory (replaces system("rm -rf")). */
void rmrf(const char *path) {
    DIR *d = opendir(path);
    if (!d) { unlink(path); return; }
    struct dirent *e;
    while ((e = readdir(d))) {
        if (e->d_name[0] == '.' && (e->d_name[1] == '\0' ||
            (e->d_name[1] == '.' && e->d_name[2] == '\0'))) continue;
        char child[PATH_MAX];
        snprintf(child, sizeof(child), "%s/%s", path, e->d_name);
        struct stat st;
        if (lstat(child, &st) == 0 && S_ISDIR(st.st_mode))
            rmrf(child);
        else
            unlink(child);
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
    size_t raw_len = hdr->value_len;
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
            size_t cap = (size_t)(f->size > 8 ? f->size : 8);
            key_buf = malloc(cap);
            typed_field_to_index_key(ic->ts, (const uint8_t *)raw, fidx, key_buf, &key_len);
            if (key_len == 0) { free(key_buf); key_buf = NULL; }
        }
    }

    if (key_buf && key_len > 0) {
        pthread_mutex_lock(&ic->lock);
        if (ic->pair_count >= ic->pair_cap) {
            ic->pair_cap *= 2;
            ic->pairs = realloc(ic->pairs, ic->pair_cap * sizeof(BtEntry));
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

int cmd_add_index(const char *db_root, const char *object,
                         const char *field, int force) {
    Schema sch = load_schema(db_root, object);
    char conf_path[PATH_MAX], idx_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);

    if (!force) {
        FILE *cf = fopen(conf_path, "r");
        if (cf) {
            char line[256];
            while (fgets(line, sizeof(line), cf)) {
                line[strcspn(line, "\n")] = '\0';
                if (strcmp(line, field) == 0) {
                    OUT("{\"status\":\"exists\",\"field\":\"%s\"}\n", field);
                    fclose(cf); return 0;
                }
            }
            fclose(cf);
        }
    }
    if (force) unlink(idx_path);

    TypedSchema *ts = load_typed_schema(db_root, object);

    IndexScanCtx ic;
    memset(&ic, 0, sizeof(ic));
    ic.field = field;
    ic.ts = ts;
    ic.pair_cap = 4096;
    ic.pairs = malloc(ic.pair_cap * sizeof(BtEntry));
    ic.is_composite = (strchr(field, '+') != NULL);
    pthread_mutex_init(&ic.lock, NULL);

    if (ic.is_composite) {
        char fbuf[256]; strncpy(fbuf, field, 255); fbuf[255] = '\0';
        char *_t_save = NULL; char *t = strtok_r(fbuf, "+", &_t_save);
        while (t && ic.field_index_count < 16) {
            ic.field_indices[ic.field_index_count++] = typed_field_index(ts, t);
            t = strtok_r(NULL, "+", &_t_save);
        }
    } else {
        ic.field_indices[0] = typed_field_index(ts, field);
        ic.field_index_count = 1;
    }

    /* Parallel shard scan — collects all (value, hash) pairs */
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    mkdirp(dirname_of(idx_path));
    scan_shards(data_dir, sch.slot_size, index_scan_cb, &ic);

    /* Sort and bulk build B+ tree */
    qsort(ic.pairs, ic.pair_count, sizeof(BtEntry), cmp_btentry_fn);
    btree_bulk_build(idx_path, ic.pairs, ic.pair_count);

    for (size_t i = 0; i < ic.pair_count; i++) free((char *)ic.pairs[i].value);
    free(ic.pairs);
    pthread_mutex_destroy(&ic.lock);

    /* Add to index.conf */
    mkdirp(dirname_of(conf_path));
    int already = 0;
    FILE *cf = fopen(conf_path, "r");
    if (cf) {
        char line[256];
        while (fgets(line, sizeof(line), cf)) {
            line[strcspn(line, "\n")] = '\0';
            if (strcmp(line, field) == 0) { already = 1; break; }
        }
        fclose(cf);
    }
    if (!already) {
        FILE *af = fopen(conf_path, "a");
        if (af) { fprintf(af, "%s\n", field); fclose(af); }
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
    size_t raw_len = hdr->value_len;

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
                size_t cap = (size_t)(f->size > 8 ? f->size : 8);
                key_buf = malloc(cap);
                typed_field_to_index_key(mc->ts, (const uint8_t *)raw, fidx, key_buf, &key_len);
                if (key_len == 0) { free(key_buf); key_buf = NULL; }
            }
        }

        if (key_buf && key_len > 0) {
            pthread_mutex_lock(&mc->lock[fi]);
            if (mc->pair_count[fi] >= mc->pair_cap[fi]) {
                mc->pair_cap[fi] *= 2;
                mc->pairs[fi] = realloc(mc->pairs[fi], mc->pair_cap[fi] * sizeof(BtEntry));
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
    char conf_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);

    /* Filter out already-existing indexes (unless force) */
    char actual_fields[MAX_FIELDS][256];
    int actual_count = 0;
    for (int i = 0; i < nfields; i++) {
        char idx_path[PATH_MAX];
        snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, fields[i]);
        if (force) { unlink(idx_path); }
        else {
            struct stat ist;
            if (stat(idx_path, &ist) == 0 && ist.st_size > 0) continue; /* skip existing */
        }
        memcpy(actual_fields[actual_count], fields[i], 256);
        actual_count++;
    }
    if (actual_count == 0) { OUT("{\"status\":\"all_exist\"}\n"); return 0; }

    TypedSchema *ts = load_typed_schema(db_root, object);

    MultiIndexCtx mc;
    memset(&mc, 0, sizeof(mc));
    mc.nfields = actual_count;
    mc.ts = ts;

    for (int fi = 0; fi < actual_count; fi++) {
        memcpy(mc.fields[fi], actual_fields[fi], 256);
        mc.is_composite[fi] = (strchr(actual_fields[fi], '+') != NULL);
        mc.pair_cap[fi] = 4096;
        mc.pairs[fi] = malloc(mc.pair_cap[fi] * sizeof(BtEntry));
        pthread_mutex_init(&mc.lock[fi], NULL);

        if (mc.is_composite[fi]) {
            char fb[256]; strncpy(fb, actual_fields[fi], 255); fb[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fb, "+", &_tok_save);
            while (tok && mc.field_index_count[fi] < 16) {
                mc.field_indices[fi][mc.field_index_count[fi]++] = typed_field_index(ts, tok);
                tok = strtok_r(NULL, "+", &_tok_save);
            }
        } else {
            mc.field_indices[fi][0] = typed_field_index(ts, actual_fields[fi]);
            mc.field_index_count[fi] = 1;
        }
    }

    /* Single parallel shard scan — extracts ALL index fields per record */
    char data_dir[PATH_MAX];
    snprintf(data_dir, sizeof(data_dir), "%s/%s/data", db_root, object);
    scan_shards(data_dir, sch.slot_size, multi_index_scan_cb, &mc);
    for (int fi = 0; fi < actual_count; fi++) pthread_mutex_destroy(&mc.lock[fi]);

    /* Parallel sort + build — one thread per index */
    IdxBuildArgIdx *ib_args = malloc(actual_count * sizeof(IdxBuildArgIdx));
    int ib_count = 0;
    for (int fi = 0; fi < actual_count; fi++) {
        if (mc.pair_count[fi] == 0) { free(mc.pairs[fi]); continue; }
        snprintf(ib_args[ib_count].ipath, PATH_MAX, "%s/%s/indexes/%s.idx", db_root, object, mc.fields[fi]);
        mkdirp(dirname_of(ib_args[ib_count].ipath));
        ib_args[ib_count].pairs = mc.pairs[fi];
        ib_args[ib_count].pair_count = mc.pair_count[fi];
        ib_count++;
    }

    parallel_for(idx_build_worker_idx, ib_args, ib_count, sizeof(IdxBuildArgIdx));

    for (int i = 0; i < ib_count; i++) {
        for (size_t ei = 0; ei < ib_args[i].pair_count; ei++) free((char *)ib_args[i].pairs[ei].value);
        free(ib_args[i].pairs);
    }
    free(ib_args);

    /* Add to index.conf */
    mkdirp(dirname_of(conf_path));
    for (int fi = 0; fi < actual_count; fi++) {
        int already = 0;
        FILE *cf = fopen(conf_path, "r");
        if (cf) {
            char line[256];
            while (fgets(line, sizeof(line), cf)) {
                line[strcspn(line, "\n")] = '\0';
                if (strcmp(line, actual_fields[fi]) == 0) { already = 1; break; }
            }
            fclose(cf);
        }
        if (!already) {
            FILE *af = fopen(conf_path, "a");
            if (af) { fprintf(af, "%s\n", actual_fields[fi]); fclose(af); }
        }
    }

    invalidate_idx_cache(object);
    OUT("{\"status\":\"indexed\",\"count\":%d}\n", actual_count);
    return 0;
}

/* ========== remove-index ==========
   Drops a single index by exact name (matches whatever was passed to
   add-index, including composite "a+b" forms). Unlinks the .idx file,
   removes its line from index.conf, and invalidates caches. */

int cmd_remove_index(const char *db_root, const char *object, const char *field) {
    if (!field || !field[0]) {
        OUT("{\"error\":\"field is required\"}\n");
        return 1;
    }

    char conf_path[PATH_MAX], idx_path[PATH_MAX];
    snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);

    /* Rewrite index.conf without the target line. */
    int found = 0;
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
            if (strcmp(stripped, field) == 0) { found = 1; continue; }
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

    btree_cache_invalidate(idx_path);
    unlink(idx_path);
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

    int removed = 0, missing = 0;
    for (int i = 0; i < nfields; i++) {
        char conf_path[PATH_MAX], idx_path[PATH_MAX];
        snprintf(conf_path, sizeof(conf_path), "%s/%s/indexes/index.conf", db_root, object);
        snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, fields[i]);

        int found = 0;
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
                if (strcmp(stripped, fields[i]) == 0) { found = 1; continue; }
                fprintf(nf, "%s", line);
            }
            fclose(cf);
            fclose(nf);
            if (rename(tmp_path, conf_path) != 0) { unlink(tmp_path); continue; }
        }

        if (found) {
            btree_cache_invalidate(idx_path);
            unlink(idx_path);
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
   Per-object progress is logged at info level only. */

int cmd_reindex(const char *db_root, const char *dir_filter, const char *obj_filter) {
    char scpath[PATH_MAX];
    snprintf(scpath, sizeof(scpath), "%s/schema.conf", db_root);
    FILE *sf = fopen(scpath, "r");
    if (!sf) {
        OUT("{\"error\":\"cannot open schema.conf\"}\n");
        return 1;
    }

    FILE *devnull = fopen("/dev/null", "w");
    FILE *saved_out = g_out;

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

        char icpath[PATH_MAX];
        snprintf(icpath, sizeof(icpath), "%s/%s/indexes/index.conf", eff_root, obj);
        FILE *ic = fopen(icpath, "r");
        if (!ic) { objects_skipped++; continue; }

        /* Build a JSON fields array from index.conf lines. */
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

        if (nf == 0) { objects_skipped++; continue; }

        /* force=1 drops existing .idx files first, then rebuilds. */
        g_out = devnull ? devnull : saved_out;
        cmd_add_indexes(eff_root, obj, fields_json, 1);
        g_out = saved_out;

        objects_rebuilt++;
        indexes_rebuilt += nf;
        log_msg(3, "REINDEX %s/%s: rebuilt %d indexes", dir, obj, nf);
    }
    fclose(sf);
    if (devnull) fclose(devnull);

    uint64_t t1 = now_ms_coarse();
    OUT("{\"status\":\"reindexed\",\"objects\":%d,\"skipped\":%d,\"indexes\":%d,\"duration_ms\":%llu}\n",
        objects_rebuilt, objects_skipped, indexes_rebuilt,
        (unsigned long long)(t1 - t0));
    return 0;
}
