#include "types.h"
#include "slotcask.h"
#include "simd.h"
#include "bitmap.h"
#include "trigram.h"
#include "io_direct.h"
#include "query_internal.h"
#include <math.h>
#include <dirent.h>
#include <stdarg.h>
/* ========== Joins (find-side) ==========
 * Parse, resolve, lookup and emit join results. Joined queries always return
 * tabular {"columns":[...],"rows":[[...]]} with fully-namespaced column names.
 */

/* typed_field_to_buf_raw and decode_idx_to_buf are declared in query_internal.h */

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
int resolve_joins(JoinSpec *joins, int n, const char *db_root,
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
int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                             size_t driver_len, const TypedSchema *driver_ts,
                             char *buf, size_t bufsz) {
    if (j->local_is_composite) {
        char fb[256]; strncpy(fb, j->local_field, 255); fb[255] = '\0';
        int pos = 0;
        char *save = NULL;
        char *tok = strtok_r(fb, "+", &save);
        while (tok) {
            int idx = driver_ts ? typed_field_index(driver_ts, tok) : -1;
            if (idx < 0) { tok = strtok_r(NULL, "+", &save); continue; }
            size_t blen = 0;
            typed_field_to_index_key(driver_ts, driver_raw, driver_len, idx,
                                      (uint8_t *)buf + pos, &blen);
            if (blen == 0) { tok = strtok_r(NULL, "+", &save); continue; }
            if (pos + (int)blen < (int)bufsz) { pos += (int)blen; }
            else break;
            tok = strtok_r(NULL, "+", &save);
        }
        return pos;
    }
    if (j->local_tf) {
        const TypedField *tf = j->local_tf;
        const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
            ? g_zero_field_65537
            : driver_raw + tf->offset;
        return typed_field_to_buf_raw(tf, fp,
                                      buf, bufsz);
    }
    return 0;
}

/* Forward decl — definition lives near btree_dispatch below. */
const TypedField *resolve_idx_field(const TypedSchema *ts, const char *field);



/* Btree search callback — captures the first hash hit. _Atomic on `found`
   because btree_idx_search fans out across idx_shards in parallel and
   multiple shard workers can race to invoke this cb on the shared hit
   struct. Compare-exchange ensures exactly one winner records its hash. */
typedef struct { uint8_t hash[16]; _Atomic int found; } JoinBtHit;
static int join_bt_first_cb(const char *val, size_t vlen, const uint8_t *hash16, void *ctx) {
    (void)val; (void)vlen;
    JoinBtHit *h = (JoinBtHit *)ctx;
    int expected = 0;
    if (!atomic_compare_exchange_strong_explicit(
            &h->found, &expected, 1,
            memory_order_acq_rel, memory_order_relaxed)) {
        return -1;  /* another worker already recorded the hash */
    }
    memcpy(h->hash, hash16, 16);
    return -1;  /* stop after first match */
}

/* Look up the remote record for one join. Returns 1 if found, 0 otherwise.
   On success, *out_rr is populated; caller must release_record_ref(out_rr).
   Storage-version-agnostic: read_record_ref dispatches v1/v2 internally. */
int lookup_remote(const JoinSpec *j, const char *db_root,
                         const char *local_key, size_t local_len,
                         RecordRef *out_rr) {
    memset(out_rr, 0, sizeof(*out_rr));
    if (local_len == 0) return 0;

    uint8_t hash[16];
    if (j->remote_is_key) {
        compute_hash_raw(local_key, local_len, hash);
    } else {
        JoinBtHit hit = {{0}, 0};
        /* Encode local_key as an index key for the REMOTE field's type —
           that's how the remote .idx stores its values. */
        const TypedField *rem_tf = resolve_idx_field(j->remote_fs.ts, j->remote_field);
        uint8_t keybuf[1032];
        size_t keylen;
        if (rem_tf && !j->local_is_composite) {
            encode_field_for_index(rem_tf, local_key, (int)local_len, keybuf, &keylen);
            btree_idx_search(db_root, j->object, j->remote_field, j->remote_sch.splits,
                             (const char *)keybuf, keylen, join_bt_first_cb, &hit);
        } else {
            /* Composite remote field or untyped — raw passthrough. */
            btree_idx_search(db_root, j->object, j->remote_field, j->remote_sch.splits,
                             local_key, local_len, join_bt_first_cb, &hit);
        }
        if (!atomic_load_explicit(&hit.found, memory_order_acquire)) return 0;
        memcpy(hash, hit.hash, 16);
    }

    if (read_record_ref(db_root, j->object, &j->remote_sch, hash, out_rr) != 0)
        return 0;

    /* For primary-key joins, guard against 16-byte hash collisions by
       verifying the stored key bytes match. The indexed-field path already
       verified equality via the btree. */
    if (j->remote_is_key) {
        if (out_rr->klen != local_len ||
            memcmp(out_rr->key, local_key, local_len) != 0) {
            release_record_ref(out_rr);
            return 0;
        }
    }
    return 1;
}

/* snprintf() reports the length it *would* write even when the
   destination is too small to hold it. Accumulating that raw return
   value into a running buffer offset (`pos += snprintf(buf+pos,
   bufsz-pos, ...)`) lets a single truncation permanently desync pos
   from the buffer: the next call in the chain computes `buf + pos`
   past the end of the buffer, and `bufsz - pos` (size_t - int, with
   pos > bufsz) wraps around to a huge unsigned size — both feed an
   out-of-bounds write to the following snprintf. This wrapper clamps
   to what was actually written / would fit, so pos can never exceed
   bufsz - 1 (CID 1696463, CID 1696458). */
static int snprintf_bounded(char *buf, size_t bufsz, const char *fmt, ...) {
    if (bufsz == 0) return 0;
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, bufsz, fmt, ap);
    va_end(ap);
    if (n < 0) return 0;
    return (n >= (int)bufsz) ? (int)bufsz - 1 : n;
}

/* Write one field value as a JSON token (number/"string"/null) into buf.
   Returns bytes written; always < bufsz, so accumulating this into a
   running offset can never overrun the caller's buffer (see
   snprintf_bounded above). Safe for worker threads. */
static int buf_field_value(const TypedField *tf, const uint8_t *field_ptr,
                           char *buf, size_t bufsz) {
    if (!tf) return snprintf_bounded(buf, bufsz, "null");
    char tmp[512];
    int n;
    switch (tf->type) {
    case FT_VARCHAR: {
        int len = varchar_eff_len(field_ptr, tf->size);
        if (len == 0) return snprintf_bounded(buf, bufsz, "\"\"");
        /* Escape per RFC 8259 — varchar content is opaque bytes,
           may contain " \ or control chars. Caller sizes buf for
           up to 6 * len + 2 worst case. NUL-terminate so callers
           that pass the buffer to printf-%s read a bounded string.
           Returns 0 (not -1) when it doesn't fit, so a caller doing
           pos += buf_field_value(...) never has pos move backwards. */
        if (bufsz < 4) return 0;
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, bufsz - 3,
                                    (const char *)(field_ptr + 2),
                                    (size_t)len);
        if (esc < 0) return 0;
        buf[1 + esc] = '"';
        buf[2 + esc] = '\0';
        return 2 + esc;
    }
    case FT_DATE:
    case FT_DATETIME:
    case FT_DATETIMEMS:
    case FT_IPV4:
    case FT_IPV6:
    case FT_ENUM:
        /* Enum's display string is a JSON string (quoted, escaped).
           DATE/DATETIME are also strings on the wire. */
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf_bounded(buf, bufsz, "null");
        return snprintf_bounded(buf, bufsz, "\"%s\"", tmp);
    default:
        n = typed_field_to_buf_raw(tf, field_ptr, tmp, sizeof(tmp));
        if (n <= 0) return snprintf_bounded(buf, bufsz, "null");
        return snprintf_bounded(buf, bufsz, "%s", tmp);
    }
}

/* Write one join's contribution (,val,val,...) to buf. remote_raw NULL → nulls.
   Returns bytes written; always < bufsz (see snprintf_bounded above). */
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw, size_t remote_len,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        else {
            const TypedField *tf = j->proj_tfs[k];
            const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > remote_len)
                ? g_zero_field_65537
                : remote_raw + tf->offset;
            pos += buf_field_value(tf, fp,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}

/* Write driver-side row values (,val,val,...) after the key.
   Returns bytes written; always < bufsz (see snprintf_bounded above). */
int buf_driver_values(const uint8_t *driver_raw, size_t driver_len, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    const TypedField *tf = &driver_fs->ts->fields[idx];
                    const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
                        ? g_zero_field_65537
                        : driver_raw + tf->offset;
                    pos += buf_field_value(tf, fp,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            const TypedField *tf = &driver_fs->ts->fields[i];
            const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
                ? g_zero_field_65537
                : driver_raw + tf->offset;
            pos += buf_field_value(tf, fp,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}

/* CSV-cell quoting to a buffer (mirrors csv_emit_cell but doesn't OUT —
   safe for worker threads). Returns chars written (excluding NUL). */
static size_t csv_cell_to_buf(const char *val, char delim, char *out, size_t out_sz) {
    if (!out_sz) return 0;
    if (!val || !val[0]) { out[0] = '\0'; return 0; }
    size_t len = strlen(val);
    int needs_quote = 0;
    for (size_t i = 0; i < len; i++) {
        char c = val[i];
        if (c == delim || c == '"' || c == '\n' || c == '\r') { needs_quote = 1; break; }
    }
    size_t pos = 0;
    if (!needs_quote) {
        for (size_t i = 0; i < len && pos < out_sz - 1; i++) out[pos++] = val[i];
    } else {
        if (pos < out_sz - 1) out[pos++] = '"';
        for (size_t i = 0; i < len && pos < out_sz - 1; i++) {
            char c = val[i];
            if (c == '\n' || c == '\r') c = ' ';
            if (c == '"') {
                if (pos + 1 < out_sz - 1) { out[pos++] = '"'; out[pos++] = '"'; }
                else break;
            } else out[pos++] = c;
        }
        if (pos < out_sz - 1) out[pos++] = '"';
    }
    out[pos] = '\0';
    return pos;
}

/* Build one joined-query CSV row "<key><d>v1<d>v2<d>j1.a<d>...\n" into buf.
   Joins with no remote match emit empty cells. Returns chars written. */
size_t build_joined_csv_row(const char *key,
                                   const uint8_t *driver_raw, size_t driver_len,
                                   FieldSchema *driver_fs,
                                   const char **proj_fields, int proj_count,
                                   const JoinSpec *joins, int njoins,
                                   const RecordRef *jrefs,
                                   char csv_delim,
                                   char *buf, size_t bufsz) {
    size_t pos = 0;
    pos += csv_cell_to_buf(key, csv_delim, buf + pos, bufsz - pos);
    char tmp[1024];

    /* Driver fields */
    if (proj_count > 0 && driver_fs && driver_fs->ts) {
        for (int i = 0; i < proj_count; i++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            int idx = typed_field_index(driver_fs->ts, proj_fields[i]);
            char *v = (idx >= 0)
                ? typed_get_field_str(driver_fs->ts, driver_raw, (int)driver_len, idx)
                : NULL;
            (void)tmp;
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            char *v = typed_get_field_str(driver_fs->ts, driver_raw, (int)driver_len, i);
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    }

    /* Joined fields — one column per (join.proj_field), prefixed with as_name in the header. */
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        const uint8_t *rraw = jrefs ? jrefs[i].val : NULL;
        size_t rlen = jrefs ? jrefs[i].vlen : 0;
        if (j->include_remote_key) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            /* Empty cell — local field carries the value (matches JSON's null). */
        }
        for (int k = 0; k < j->proj_count; k++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            if (!rraw || !j->proj_tfs[k]) continue;
            const TypedField *rtf = j->proj_tfs[k];
            const uint8_t *rfp = ((size_t)rtf->offset + (size_t)rtf->size > rlen)
                ? g_zero_field_65537
                : rraw + rtf->offset;
            int n = typed_field_to_buf_raw(rtf, rfp,
                                           tmp, sizeof(tmp));
            if (n > 0) pos += csv_cell_to_buf(tmp, csv_delim, buf + pos, bufsz - pos);
        }
    }

    if (pos < bufsz - 1) buf[pos++] = '\n';
    buf[pos] = '\0';
    return pos;
}

/* Emit the CSV header line for a joined query (driver + joins, prefixed columns). */
void emit_joined_csv_header(const char *driver_object,
                                   FieldSchema *driver_fs,
                                   const JoinSpec *joins, int njoins,
                                   const char **driver_proj, int driver_proj_count,
                                   char delim) {
    OUT("%s.key", driver_object);
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, driver_object, driver_proj[i]);
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, driver_object, driver_fs->ts->fields[i].name);
        }
    }
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        if (j->include_remote_key) { char d[2] = { delim, '\0' }; OUT("%s%s.key", d, j->as_name); }
        for (int k = 0; k < j->proj_count; k++) {
            char d[2] = { delim, '\0' }; OUT("%s%s.%s", d, j->as_name, j->proj_fields[k]);
        }
    }
    OUT("\n");
}

/* Emit the columns header for a joined query (main thread only). */
void emit_joined_columns(const char *driver_object,
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

    /* Render key per the object's auto_key mode so the wire-form
       comparison + emit work for binary keys (AK_UUID/SEQ). Heap-alloc
       to preserve the existing free() lifecycle in this function. */
    char *key = malloc(1100);
    if (!key) return 1;
    {
        const Schema *sc_p = (sc->fs && sc->fs->auto_key != AK_NONE)
                              ? &sc->fs->auto_key_schema_snapshot : NULL;
        format_wire_key(sc_p, (const char *)block, hdr->key_len, key, 1100);
    }

    /* Check excluded keys first */
    if (is_excluded(&sc->excluded, key)) { free(key); return 0; }

    const char *raw = (const char *)block + hdr->key_len;

    /* Criteria match + join resolution are thread-local reads, lock-free. */
    int match = sc->fast_cc
        ? match_typed((const uint8_t *)raw, sc->fast_cc, sc->fs)
        : criteria_match_tree((const uint8_t *)raw, sc->tree, sc->fs);

    if (match) {
        RecordRef     *join_refs = NULL;
        const uint8_t **join_raws = NULL;  /* parallel array of value ptrs for emit helpers */
        int dropped = 0;
        if (sc->njoins > 0) {
            join_refs = calloc(sc->njoins, sizeof(RecordRef));
            join_raws = calloc(sc->njoins, sizeof(const uint8_t *));
            for (int i = 0; i < sc->njoins; i++) {
                char lk[1024];
                int llen = extract_local_key(&sc->joins[i], (const uint8_t *)raw,
                                             (size_t)hdr->value_len,
                                             sc->fs ? sc->fs->ts : NULL, lk, sizeof(lk));
                int found = 0;
                if (llen > 0) {
                    found = lookup_remote(&sc->joins[i], sc->db_root, lk, (size_t)llen,
                                          &join_refs[i]);
                    if (found) join_raws[i] = join_refs[i].val;
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
                    for (int i = 0; i < sc->njoins; i++) release_record_ref(&join_refs[i]);
                    free(join_refs); free(join_raws);
                }
                free(key);
                return 1;
            }
            sc->count++;
            if (sc->count > sc->offset) {
                if (sc->njoins > 0 && sc->csv_delim) {
                    /* CSV joined row: <key><delim>v1<delim>...<delim>j1.a<delim>...\n */
                    char row[16384];
                    size_t n = build_joined_csv_row(
                        key, (const uint8_t *)raw, (size_t)hdr->value_len, sc->fs,
                        sc->proj_count > 0 ? sc->proj_fields : NULL, sc->proj_count,
                        sc->joins, sc->njoins, join_refs, sc->csv_delim,
                        row, sizeof(row));
                    OUT("%.*s", (int)n, row);
                } else if (sc->njoins > 0) {
                    /* Tabular JSON row: [driver.key, driver fields..., join1 fields..., ...] */
                    char row[16384];
                    int pos = snprintf_bounded(row, sizeof(row), "%s[\"%s\"",
                                       sc->printed ? "," : "", key);
                    pos += buf_driver_values((const uint8_t *)raw, (size_t)hdr->value_len, sc->fs,
                                             sc->proj_count > 0 ? sc->proj_fields : NULL,
                                             sc->proj_count,
                                             row + pos, sizeof(row) - pos);
                    for (int i = 0; i < sc->njoins && pos < (int)sizeof(row) - 2; i++)
                        pos += buf_join_values(&sc->joins[i], join_raws[i], join_refs[i].vlen,
                                               row + pos, sizeof(row) - pos);
                    snprintf_bounded(row + pos, sizeof(row) - pos, "]");
                    OUT("%s", row);
                } else if (sc->csv_delim) {
                    csv_emit_row(key, (const uint8_t *)raw, hdr->value_len,
                                 sc->proj_count > 0 ? sc->proj_fields : NULL,
                                 sc->proj_count, sc->fs, sc->csv_delim);
                } else if (sc->rows_fmt) {
                    OUT("%s[\"%s\"", sc->printed ? "," : "", key);
                    if (sc->proj_count > 0) {
                        for (int i = 0; i < sc->proj_count; i++) {
                            char *pv = json_projected_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            OUT(",%s", pv ? pv : "\"\"");
                            free(pv);
                        }
                    } else if (sc->fs && sc->fs->ts) {
                        for (int i = 0; i < sc->fs->ts->nfields; i++) {
                            if (sc->fs->ts->fields[i].removed) continue;
                            char *pv = json_projected_value(
                                typed_get_field_str(sc->fs->ts, (const uint8_t *)raw, (int)hdr->value_len, i),
                                &sc->fs->ts->fields[i]);
                            OUT(",%s", pv ? pv : "\"\"");
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
                            char *pv = json_projected_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                            if (!pv) continue;
                            OUT("%s\"%s\":%s", first ? "" : ",", sc->proj_fields[i], pv);
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
                        char *pv = json_projected_field(raw, hdr->value_len, sc->proj_fields[i], sc->fs);
                        if (!pv) continue;
                        OUT("%s\"%s\":%s", first ? "" : ",", sc->proj_fields[i], pv);
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
            for (int i = 0; i < sc->njoins; i++) release_record_ref(&join_refs[i]);
            free(join_refs);
            free(join_raws);
        }
    }
    free(key);
    return 0;
}
