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
                              const char *field, const char *attr_val,
                              const uint8_t hash16[16]) {
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);
    mkdirp(dirname_of(idx_path));
    btree_insert(idx_path, attr_val, strlen(attr_val), hash16);
}

/* Delete from index — uses B+ tree */
void delete_index_entry(const char *db_root, const char *object,
                               const char *field, const char *attr_val,
                               const uint8_t hash16[16]) {
    char idx_path[PATH_MAX];
    snprintf(idx_path, sizeof(idx_path), "%s/%s/indexes/%s.idx", db_root, object, field);
    btree_delete(idx_path, attr_val, strlen(attr_val), hash16);
}

/* ========== Parallel indexing ========== */

typedef struct {
    const char *db_root;
    const char *object;
    const char *field;
    const char *attr_val;
    const uint8_t *hash16;
} IndexThreadArg;

void *index_thread_fn(void *arg) {
    IndexThreadArg *a = (IndexThreadArg *)arg;
    write_index_entry(a->db_root, a->object, a->field, a->attr_val, a->hash16);
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
    return json_get_raw(json, field_name);
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

    /* Collect all unique sub-field names from single + composite indexes */
    const char *unique_keys[MAX_FIELDS * 4];
    int unique_count = 0;
    for (int i = 0; i < nfields; i++) {
        if (strchr(fields[i], '+')) {
            /* Composite — split and add sub-fields */
            char fbuf[256];
            strncpy(fbuf, fields[i], 255); fbuf[255] = '\0';
            char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
            while (tok) {
                /* Check if already in unique list */
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
                unique_keys[unique_count++] = fields[i]; /* no strdup needed — points to fields */
        }
    }

    /* Single-pass extraction of ALL unique sub-fields */
    char *extracted[MAX_FIELDS * 4];
    json_get_fields(value, unique_keys, unique_count, extracted);

    /* Now build index values and insert */
    pthread_t threads[MAX_FIELDS];
    IndexThreadArg args[MAX_FIELDS];
    int tcount = 0;

    /* Temporary storage for composite values */
    char *composite_vals[MAX_FIELDS];
    memset(composite_vals, 0, sizeof(composite_vals));

    for (int i = 0; i < nfields; i++) {
        char *idx_val = NULL;

        if (strchr(fields[i], '+')) {
            /* Composite index — concatenate sub-field values */
            char fbuf[256];
            strncpy(fbuf, fields[i], 255); fbuf[255] = '\0';
            char result[4096];
            int pos = 0;
            int all_present = 1;
            char *_tok_save = NULL; char *tok = strtok_r(fbuf, "+", &_tok_save);
            while (tok) {
                /* Find this sub-field in extracted values */
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
            result[pos] = '\0';
            if (all_present && pos > 0) {
                composite_vals[i] = strdup(result);
                idx_val = composite_vals[i];
            }
        } else {
            /* Single field — find in extracted */
            for (int j = 0; j < unique_count; j++) {
                if (strcmp(unique_keys[j], fields[i]) == 0) {
                    idx_val = extracted[j];
                    break;
                }
            }
        }

        if (!idx_val || idx_val[0] == '\0') continue;

        args[tcount].db_root = db_root;
        args[tcount].object = object;
        args[tcount].field = fields[i];
        args[tcount].attr_val = idx_val;
        args[tcount].hash16 = hash16;

        pthread_create(&threads[tcount], NULL, index_thread_fn, &args[tcount]);
        tcount++;
    }

    for (int i = 0; i < tcount; i++)
        pthread_join(threads[i], NULL);

    /* Free extracted values and composite values */
    for (int i = 0; i < unique_count; i++) free(extracted[i]);
    for (int i = 0; i < nfields; i++) free(composite_vals[i]);
    /* Free strdup'd unique keys (only those from composite splits) */
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

/* Comparators for raw structs (used by add-index sort) */
int cmp_btentry_fn(const void *a, const void *b) {
    return strcmp(((const BtEntry *)a)->value, ((const BtEntry *)b)->value);
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
} IndexScanCtx;

static int index_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    IndexScanCtx *ic = (IndexScanCtx *)ctx;
    const char *raw = (const char *)(block + hdr->key_len);
    size_t raw_len = hdr->value_len;
    char *attr = NULL;

    if (ic->is_composite) {
        char cat[4096]; int cpos = 0; int ok = 1;
        for (int i = 0; i < ic->field_index_count; i++) {
            char *v = typed_get_field_str(ic->ts, (const uint8_t *)raw, ic->field_indices[i]);
            if (v) { int sl = strlen(v); memcpy(cat + cpos, v, sl); cpos += sl; free(v); }
            else { ok = 0; break; }
        }
        cat[cpos] = '\0';
        if (ok && cpos > 0) attr = strdup(cat);
    } else {
        attr = typed_get_field_str(ic->ts, (const uint8_t *)raw, ic->field_indices[0]);
    }

    if (attr && attr[0]) {
        if (ic->pair_count >= ic->pair_cap) {
            ic->pair_cap *= 2;
            ic->pairs = realloc(ic->pairs, ic->pair_cap * sizeof(BtEntry));
        }
        ic->pairs[ic->pair_count].value = attr;
        ic->pairs[ic->pair_count].vlen = strlen(attr);
        memcpy(ic->pairs[ic->pair_count].hash, hdr->hash, 16);
        ic->pair_count++;
    } else {
        free(attr);
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
} MultiIndexCtx;

static int multi_index_scan_cb(const SlotHeader *hdr, const uint8_t *block, void *ctx) {
    MultiIndexCtx *mc = (MultiIndexCtx *)ctx;
    const char *raw = (const char *)(block + hdr->key_len);
    size_t raw_len = hdr->value_len;

    for (int fi = 0; fi < mc->nfields; fi++) {
        char *attr = NULL;
        if (mc->is_composite[fi]) {
            char cat[4096]; int cpos = 0; int ok = 1;
            for (int si = 0; si < mc->field_index_count[fi]; si++) {
                char *v = typed_get_field_str(mc->ts, (const uint8_t *)raw, mc->field_indices[fi][si]);
                if (v) { int sl = strlen(v); memcpy(cat + cpos, v, sl); cpos += sl; free(v); }
                else { ok = 0; break; }
            }
            cat[cpos] = '\0';
            if (ok && cpos > 0) attr = strdup(cat);
        } else {
            attr = typed_get_field_str(mc->ts, (const uint8_t *)raw, mc->field_indices[fi][0]);
        }

        if (attr && attr[0]) {
            if (mc->pair_count[fi] >= mc->pair_cap[fi]) {
                mc->pair_cap[fi] *= 2;
                mc->pairs[fi] = realloc(mc->pairs[fi], mc->pair_cap[fi] * sizeof(BtEntry));
            }
            mc->pairs[fi][mc->pair_count[fi]].value = attr;
            mc->pairs[fi][mc->pair_count[fi]].vlen = strlen(attr);
            memcpy(mc->pairs[fi][mc->pair_count[fi]].hash, hdr->hash, 16);
            mc->pair_count[fi]++;
        } else {
            free(attr);
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

    int nt = parallel_threads();
    if (nt > ib_count) nt = ib_count;
    if (nt <= 1) {
        for (int i = 0; i < ib_count; i++) idx_build_worker_idx(&ib_args[i]);
    } else {
        pthread_t *it = malloc(nt * sizeof(pthread_t));
        for (int b = 0; b < ib_count; b += nt) {
            int n = ib_count - b; if (n > nt) n = nt;
            for (int t = 0; t < n; t++) pthread_create(&it[t], NULL, idx_build_worker_idx, &ib_args[b + t]);
            for (int t = 0; t < n; t++) pthread_join(it[t], NULL);
        }
        free(it);
    }

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

