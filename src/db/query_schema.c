#include "types.h"
#include "type_desc.h"
#include "slotcask.h"
#include "bitmap.h"
#include "query_internal.h"
#include <dirent.h>

/* ========== edit-field ==========
 *
 * Same-type, in-place field edits (varchar grow/shrink, integer
 * widen/narrow, numeric scale change, float→double widen). Wire shape:
 *   {"mode":"edit-field","dir":..,"object":..,"fields":["name:varchar:200", ..]}
 *
 * v2 only — v1 is refused with a pointer to the historical 2026.05.4
 * migration path. Cross-type changes
 * are refused with a hint to use add-field + remove-field + bulk-update.
 *
 * Strategy:
 *   1. Parse each edit spec, refuse tombstoned/unknown/duplicate/cross-type.
 *   2. Build new_ts by overlaying each edit's TypedField onto a clone of
 *      old_ts. Field positions, names, and active-or-tombstoned status
 *      stay the same — only size, offset, numeric_scale, default
 *      modifiers move.
 *   3. If no field's encoding actually changes (field_needs_transform == 0
 *      for every edit), skip the rebuild entirely and just rewrite
 *      fields.conf — the only user-visible change is default modifiers,
 *      which affects future inserts, not existing records.
 *   4. Otherwise pre-flight scan: open the live slotcask, walk every
 *      live record, decode each edited field per record, refuse the
 *      whole edit if any record's value won't fit the new shape.
 *   5. All clear — call rebuild_object_v2 (n_added=0, drop_tombstoned=0).
 *      The enhanced v2_rebuild_walk_cb already routes edited fields
 *      through transform_field_value. After the rebuild succeeds,
 *      rewrite fields.conf in place and rebuild every index in
 *      index.conf (affected indexes have stale leaf bytes; we wipe
 *      and rebuild all to keep the code simple — acceptable for v1,
 *      optimise later if it becomes a hot path).
 *
 * Caller holds objlock_wrlock on the object. */

typedef struct {
    const TypedSchema *old_ts;
    const TypedSchema *new_ts;
    const int *edited_old_idx;  /* old TypedSchema indices being edited */
    int n_edits;
    char fail_field[128];
    char fail_reason[192];
    int failed;
} EditPreflightCtx;

/* Accumulate BE bytes unsigned, then sign-extend: left-shifting the
   seeded negative accumulator was UB (strict UBSan gate, 2026-08-28 —
   edit-field int→long aborts the daemon on negative values). The
   arithmetic >> on the re-signed value is the canonical extension. */
static int64_t decode_be_signed(const uint8_t *src, int sz) {
    uint64_t u = 0;
    for (int i = 0; i < sz; i++) u = (u << 8) | src[i];
    unsigned sext = (unsigned)(64 - sz * 8);
    return (int64_t)(u << sext) >> sext;
}

/* Return 1 if value will fit the new field's bounds; 0 otherwise. Writes
   the human-readable reason into reason[reason_cap] on failure. */
static int field_value_fits_new(const TypedField *old_f,
                                const TypedField *new_f,
                                const uint8_t *src,
                                char *reason, size_t reason_cap) {
    switch (old_f->type) {
    case FT_VARCHAR: {
        uint16_t old_clen = ((uint16_t)src[0] << 8) | (uint16_t)src[1];
        int new_cap = new_f->size - 2;
        if (new_cap < 0) new_cap = 0;
        if ((int)old_clen > new_cap) {
            snprintf(reason, reason_cap,
                "varchar shrink: value of length %u exceeds new cap %d",
                (unsigned)old_clen, new_cap);
            return 0;
        }
        return 1;
    }
    case FT_INT:
    case FT_LONG:
    case FT_SHORT: {
        if (new_f->size >= old_f->size) return 1;  /* widen always fits */
        int64_t v = decode_be_signed(src, old_f->size);
        int64_t lo = -(1LL << (new_f->size * 8 - 1));
        int64_t hi =  (1LL << (new_f->size * 8 - 1)) - 1;
        if (v < lo || v > hi) {
            snprintf(reason, reason_cap,
                "integer narrow: value %lld out of range [%lld, %lld]",
                (long long)v, (long long)lo, (long long)hi);
            return 0;
        }
        return 1;
    }
    case FT_NUMERIC: {
        int delta = new_f->numeric_scale - old_f->numeric_scale;
        if (delta <= 0) return 1;  /* scale-down truncates, never overflows */
        int64_t v = decode_be_signed(src, old_f->size);
        int64_t mult = 1;
        for (int i = 0; i < delta; i++) {
            /* Multiply with overflow detection. INT64_MAX/10 = 922337203685477580. */
            if (v > 922337203685477580LL || v < -922337203685477580LL) {
                snprintf(reason, reason_cap,
                    "numeric scale-up: value %lld overflows int64 after ×10^%d",
                    (long long)v, delta);
                return 0;
            }
            mult *= 10;
            (void)mult;  /* mult itself can't overflow at delta<=18 */
        }
        /* Final check — v * 10^delta */
        int64_t scaled = v;
        for (int i = 0; i < delta; i++) scaled *= 10;
        (void)scaled;
        return 1;
    }
    default:
        /* Same-size paths are handled by field_needs_transform short-circuit;
           any other type that reaches here has the same byte layout so it fits. */
        return 1;
    }
}

static int edit_preflight_walk_cb(const uint8_t hash16[16],
                                  const void *key, size_t klen,
                                  const void *value, size_t vlen,
                                  void *ctx_v) {
    (void)hash16; (void)key; (void)klen;
    EditPreflightCtx *ctx = (EditPreflightCtx *)ctx_v;
    if (ctx->failed) return 1;
    for (int e = 0; e < ctx->n_edits; e++) {
        int oi = ctx->edited_old_idx[e];
        const TypedField *of = &ctx->old_ts->fields[oi];
        const TypedField *nf = &ctx->new_ts->fields[oi];
        if (of->offset + (size_t)of->size > vlen) continue;  /* defensive */
        char reason[192];
        if (!field_value_fits_new(of, nf, (const uint8_t *)value + of->offset,
                                   reason, sizeof(reason))) {
            strncpy(ctx->fail_field, of->name, sizeof(ctx->fail_field) - 1);
            ctx->fail_field[sizeof(ctx->fail_field) - 1] = '\0';
            strncpy(ctx->fail_reason, reason, sizeof(ctx->fail_reason) - 1);
            ctx->fail_reason[sizeof(ctx->fail_reason) - 1] = '\0';
            ctx->failed = 1;
            return 1;  /* stop walk */
        }
    }
    return 0;
}

/* Lightweight name validator, mirrors valid_field_name in config.c. */
static int edit_valid_name(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n >= 128) return 0;
    for (size_t i = 0; i < n; i++) {
        char c = name[i];
        if (c == ':' || c == '+' || c == '/' || c == '\n' ||
            c == '\r' || c == ' ' || c == '\t')
            return 0;
    }
    return 1;
}

/* Extract the trailing default-modifier suffix from a fields.conf line.
   Modifiers recognised: :default=<…>, :auto_create, :auto_update.
   The suffix is everything from the first such modifier to end-of-line.
   Returns 1 and copies the suffix into out_buf when a modifier is found
   AND fits in out_cap (NUL-terminator included). Returns 0 in every other
   case: no modifier present, or the modifier wouldn't fit. Refusing to
   truncate is intentional — a half-modifier written into fields.conf
   would corrupt the schema on reload, so callers that get 0 here just
   drop the carry rather than write a wrong value. */
static int default_modifiers_for_line(const char *line, char *out_buf, size_t out_cap) {
    if (out_cap == 0) return 0;
    out_buf[0] = '\0';
    static const char *mods[] = { ":default=", ":auto_create", ":auto_update", NULL };
    const char *earliest = NULL;
    for (int i = 0; mods[i]; i++) {
        const char *p = strstr(line, mods[i]);
        if (p && (!earliest || p < earliest)) earliest = p;
    }
    if (!earliest) return 0;
    size_t need = strlen(earliest);
    if (need >= out_cap) return 0;   /* refuse to truncate — safer than half-modifier */
    memcpy(out_buf, earliest, need);
    out_buf[need] = '\0';
    return 1;
}

/* Rewrite fields.conf in place, replacing each edited line with its new
   spec. Edited lines preserve the trailing :removed marker if present
   (edit-field rejects tombstoned fields up front, so this is defensive).
   Lines unaffected by the edit pass through unchanged. */
static int rewrite_fields_conf_for_edit(const char *obj_dir,
                                         char edit_lines[][256], int n_edits) {
    char fpath[PATH_MAX], fpath_new[PATH_MAX];
    snprintf(fpath,     sizeof(fpath),     "%s/fields.conf", obj_dir);
    snprintf(fpath_new, sizeof(fpath_new), "%s/fields.conf.new", obj_dir);

    /* Extract just the field name from each edit_line (text up to first ':'). */
    char edit_names[MAX_FIELDS][128];
    for (int e = 0; e < n_edits; e++) {
        const char *colon = strchr(edit_lines[e], ':');
        size_t nlen = colon ? (size_t)(colon - edit_lines[e]) : strlen(edit_lines[e]);
        if (nlen >= sizeof(edit_names[0])) nlen = sizeof(edit_names[0]) - 1;
        memcpy(edit_names[e], edit_lines[e], nlen);
        edit_names[e][nlen] = '\0';
    }

    FILE *fin = fopen(fpath, "r");
    int fin_errno = errno;
    FILE *fout = fopen(fpath_new, "w");
    int fout_errno = errno;
    if (!fin || !fout) {
        if (!fin) LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath, strerror(fin_errno));
        if (!fout) LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: fopen %s failed: %s", fpath_new, strerror(fout_errno));
        if (fin) fclose(fin);
        if (fout) fclose(fout);
        return -1;
    }
    char line[512];
    while (fgets(line, sizeof(line), fin)) {
        char stripped[512];
        strncpy(stripped, line, sizeof(stripped) - 1);
        stripped[sizeof(stripped) - 1] = '\0';
        stripped[strcspn(stripped, "\n")] = '\0';
        if (stripped[0] == '\0' || stripped[0] == '#') {
            fputs(line, fout);
            continue;
        }
        const char *colon = strchr(stripped, ':');
        size_t nlen = colon ? (size_t)(colon - stripped) : strlen(stripped);
        int matched = -1;
        for (int e = 0; e < n_edits; e++) {
            size_t elen = strlen(edit_names[e]);
            if (nlen == elen && memcmp(stripped, edit_names[e], elen) == 0) {
                matched = e;
                break;
            }
        }
        if (matched < 0) {
            fputs(line, fout);
            continue;
        }
        /* Carry default modifiers (:default=…, :auto_create, :auto_update)
           from the OLD line to the new line when the user's new spec omits
           them. Lets `edit-field age:long` change the type without wiping
           an existing `:default=42`. */
        char old_mods[256] = "";
        default_modifiers_for_line(stripped, old_mods, sizeof(old_mods));
        char new_mods[256] = "";
        int new_has = default_modifiers_for_line(edit_lines[matched], new_mods, sizeof(new_mods));
        if (!new_has && old_mods[0])
            fprintf(fout, "%s%s\n", edit_lines[matched], old_mods);
        else
            fprintf(fout, "%s\n", edit_lines[matched]);
    }
    fclose(fin);
    fclose(fout);
    if (rename(fpath_new, fpath) != 0) {
        LOG_ERROR(LOG_SUB_CONFIG, "rewrite_fields_conf_for_edit: rename %s -> %s failed: %s", fpath_new, fpath, strerror(errno));
        unlink(fpath_new);
        return -1;
    }
    return 0;
}

/* Selective reindex driver — walks index.conf and only rebuilds indexes
   whose referenced fields appear in dirty_names[]. Returns (rebuilt,
   skipped) via out params. */
static int selective_reindex_dirty(const char *db_root, const char *object,
                                   char dirty_names[][128], int n_dirty,
                                   int *out_rebuilt, int *out_skipped) {
    *out_rebuilt = 0; *out_skipped = 0;
    if (n_dirty <= 0) return 0;
    char ic_path[PATH_MAX];
    snprintf(ic_path, sizeof(ic_path), "%s/%s/indexes/index.conf", db_root, object);
    FILE *ic = fopen(ic_path, "r");
    if (!ic) return errno == ENOENT ? 0 : -1;

    char affected_specs[MAX_FIELDS][256];
    int n_aff = 0;
    char line[512];
    while (fgets(line, sizeof(line), ic)) {
        line[strcspn(line, "\n")] = '\0';
        if (!line[0]) continue;
        /* Index spec form: <fname>[+<fname>...][':' index-type-tail].
           Match any '+'-separated token against dirty_names[]. */
        int hit = 0;
        const char *p = line;
        while (*p && !hit) {
            const char *next = p;
            while (*next && *next != '+' && *next != ':') next++;
            size_t toklen = (size_t)(next - p);
            for (int d = 0; d < n_dirty; d++) {
                size_t dn = strlen(dirty_names[d]);
                if (toklen == dn && memcmp(p, dirty_names[d], dn) == 0) { hit = 1; break; }
            }
            if (*next == '+') p = next + 1;
            else break;
        }
        if (hit && n_aff < MAX_FIELDS) {
            strncpy(affected_specs[n_aff], line, sizeof(affected_specs[0]) - 1);
            affected_specs[n_aff][sizeof(affected_specs[0]) - 1] = '\0';
            n_aff++;
        } else (*out_skipped)++;
    }
    fclose(ic);

    if (n_aff == 0) return 0;

    Schema sch = load_schema(db_root, object);
    for (int i = 0; i < n_aff; i++) {
        /* Strip ':<type>' tail to get the field-only name for unlink. */
        char field_only[256];
        strncpy(field_only, affected_specs[i], sizeof(field_only) - 1);
        field_only[sizeof(field_only) - 1] = '\0';
        char *col = strchr(field_only, ':');
        if (col) *col = '\0';
        btree_idx_unlink_all(db_root, object, field_only, sch.splits);
    }

    /* Hand the affected list to cmd_add_indexes(force=1). snprintf returns
       the *would-have-written* length even on truncation, so the running
       pos can outrun the buffer. Clamp after every accumulation so the
       final "]" snprintf can't see an underflowed remaining-size. */
    char fields_json[8192];
    size_t cap = sizeof(fields_json);
    size_t pos = (size_t)snprintf(fields_json, cap, "[");
    if (pos >= cap) pos = cap - 1;
    for (int i = 0; i < n_aff; i++) {
        size_t rem = cap - pos;
        int n = snprintf(fields_json + pos, rem, "%s\"%s\"",
                         i ? "," : "", affected_specs[i]);
        if (n < 0) break;
        if ((size_t)n >= rem) { pos = cap - 1; break; }
        pos += (size_t)n;
    }
    if (pos < cap - 1) snprintf(fields_json + pos, cap - pos, "]");
    else fields_json[cap - 1] = '\0';

    FILE *saved_out = g_out;
    FILE *devnull = fopen("/dev/null", "w");
    g_out = devnull ? devnull : saved_out;
    int rc = cmd_add_indexes(db_root, object, fields_json, 1);
    g_out = saved_out;
    if (devnull) fclose(devnull);
    if (rc == 0) *out_rebuilt = n_aff;
    return rc;
}

typedef struct {
    const char *db_root;
    const char *object;
    const char *obj_dir;
    char (*edit_lines)[256];
    int n_edits;
    char (*dirty_names)[128];
    int n_dirty;
    int idx_rebuilt;
    int idx_skipped;
} EditFinalizeCtx;

static int edit_finalize_metadata(void *ctx_) {
    EditFinalizeCtx *ctx = (EditFinalizeCtx *)ctx_;
    return rewrite_fields_conf_for_edit(ctx->obj_dir, ctx->edit_lines,
                                        ctx->n_edits);
}

static int edit_finalize_indexes(void *ctx_, int *out_rebuilt) {
    EditFinalizeCtx *ctx = (EditFinalizeCtx *)ctx_;
    int rc = selective_reindex_dirty(ctx->db_root, ctx->object,
                                     ctx->dirty_names, ctx->n_dirty,
                                     &ctx->idx_rebuilt, &ctx->idx_skipped);
    *out_rebuilt = ctx->idx_rebuilt;
    return rc;
}

int cmd_edit_fields(const char *db_root, const char *object,
                    char lines[][256], int n_edits,
                    int allow_rename, int dry_run) {
    if (n_edits <= 0) {
        OUT("{\"error\":\"No fields specified\"}\n");
        return 1;
    }
    if (n_edits > MAX_FIELDS) {
        OUT("{\"error\":\"Too many fields in one edit (max %d)\"}\n", MAX_FIELDS);
        return 1;
    }

    Schema old_sch = load_schema(db_root, object);
    if (old_sch.splits <= 0) {
        OUT("{\"error\":\"Object [%s] not found\"}\n", object);
        return 1;
    }
    TypedSchema *old_ts = load_typed_schema(db_root, object);
    if (!old_ts) {
        OUT("{\"error\":\"fields.conf missing for [%s]\"}\n", object);
        return 1;
    }

    /* Parse each edit line, validate name/type, look up target field. */
    TypedField parsed[MAX_FIELDS];
    int edited_old_idx[MAX_FIELDS];

    for (int e = 0; e < n_edits; e++) {
        memset(&parsed[e], 0, sizeof(parsed[e]));
        if (strstr(lines[e], ":removed")) {
            OUT("{\"error\":\"Cannot edit with ':removed' marker; use remove-field\"}\n");
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        if (!parse_field_line(lines[e], &parsed[e])) {
            OUT("{\"error\":\"Invalid field line: %s\"}\n", lines[e]);
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        if (!edit_valid_name(parsed[e].name)) {
            OUT("{\"error\":\"Invalid field name: %s\"}\n", parsed[e].name);
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        /* Duplicate-edit-in-request check. */
        for (int b = 0; b < e; b++) {
            if (strcmp(parsed[b].name, parsed[e].name) == 0) {
                OUT("{\"error\":\"Duplicate edit for field [%s] in request\"}\n",
                    parsed[e].name);
                for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
                return 1;
            }
        }
        /* Resolve to old TypedSchema index. Refuse unknown / tombstoned. */
        int found = -1;
        for (int i = 0; i < old_ts->nfields; i++) {
            if (strcmp(old_ts->fields[i].name, parsed[e].name) == 0) {
                found = i;
                break;
            }
        }
        if (found < 0) {
            OUT("{\"error\":\"Field [%s] not found\"}\n", parsed[e].name);
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        if (old_ts->fields[found].removed) {
            OUT("{\"error\":\"Field [%s] is tombstoned; cannot edit\"}\n",
                parsed[e].name);
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }
        /* Cross-type refusal. Allowed cross-type edits:
             - integer family (FT_SHORT/INT/LONG) — same signed-BE encoding,
               just different storage widths. transform_field_value handles
               sign-extend on widen and truncates on narrow (with pre-flight
               range check).
             - float → double widen. */
        const TypedField *oldf = &old_ts->fields[found];
        int same_type = (oldf->type == parsed[e].type);
        int int_family_old = (oldf->type == FT_SHORT || oldf->type == FT_INT ||
                               oldf->type == FT_LONG);
        int int_family_new = (parsed[e].type == FT_SHORT || parsed[e].type == FT_INT ||
                               parsed[e].type == FT_LONG);
        int int_family = int_family_old && int_family_new;
        int float_widen = (oldf->type == FT_FLOAT && parsed[e].type == FT_DOUBLE);
        if (!same_type && !int_family && !float_widen) {
            OUT("{\"error\":\"Cross-type edit refused for [%s]; "
                 "use add-field <new> + remove-field <old> + bulk-update\"}\n",
                 parsed[e].name);
            for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
            return 1;
        }

        /* FT_ENUM diff: new value list must be a strict prefix of the old
           (append), optionally with renames at existing positions when the
           caller passed `allow_rename`. Anything else (remove, reorder,
           length shrink) corrupts existing records, so refuse loudly.
           Width transitions: 1B → 2B is allowed when the new list count
           exceeds 256 — the rebuild path handles the record re-encoding
           via transform_field_value. 2B → 1B not supported (would lose
           record data if any record holds an index ≥ 256). */
        if (oldf->type == FT_ENUM && parsed[e].type == FT_ENUM) {
            int oldn = oldf->n_enum_values;
            int newn = parsed[e].n_enum_values;
            if (newn < oldn) {
                OUT("{\"error\":\"enum edit refused for [%s]: cannot remove or shrink the value list (records reference values by position — removing would corrupt them). Append-only edits supported.\"}\n",
                    parsed[e].name);
                for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
                return 1;
            }
            if (oldf->enum_width == 2 && parsed[e].enum_width == 1) {
                OUT("{\"error\":\"enum edit refused for [%s]: cannot narrow 2-byte → 1-byte enum.\"}\n",
                    parsed[e].name);
                for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
                return 1;
            }
            int had_rename = 0;
            for (int i = 0; i < oldn; i++) {
                const char *ov = oldf->enum_values[i];
                const char *nv = parsed[e].enum_values[i];
                if (!ov || !nv) {
                    OUT("{\"error\":\"enum edit for [%s]: internal value list corrupt at position %d\"}\n",
                        parsed[e].name, i);
                    for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
                    return 1;
                }
                if (strcmp(ov, nv) != 0) had_rename = 1;
            }
            if (had_rename && !allow_rename) {
                    OUT("{\"error\":\"enum edit refused for [%s]: at least one value at an existing position changed — that's a rename. Re-issue with allow_rename:true to confirm (existing records keep their byte index but the displayed value changes).\"}\n",
                        parsed[e].name);
                    for (int _i = 0; _i <= e; _i++) free_enum_values(&parsed[_i]);
                    return 1;
            }
        }

        edited_old_idx[e] = found;
    }

    /* Build new_ts: clone old_ts, overlay each edit, recompute offsets. */
    TypedSchema new_ts;
    memset(&new_ts, 0, sizeof(new_ts));
    new_ts.typed = 1;
    new_ts.nfields = old_ts->nfields;
    int new_to_old[MAX_FIELDS];
    int noff = 0;
    for (int i = 0; i < old_ts->nfields; i++) {
        new_ts.fields[i] = old_ts->fields[i];
        int edit_for_i = -1;
        for (int e = 0; e < n_edits; e++) {
            if (edited_old_idx[e] == i) { edit_for_i = e; break; }
        }
        if (edit_for_i >= 0) {
            /* Overlay the edit's parsed type info; keep name + removed flag. */
            new_ts.fields[i].type             = parsed[edit_for_i].type;
            new_ts.fields[i].size             = parsed[edit_for_i].size;
            new_ts.fields[i].numeric_scale    = parsed[edit_for_i].numeric_scale;
            new_ts.fields[i].numeric_scale_mult = parsed[edit_for_i].numeric_scale_mult;
            /* FT_ENUM: overlay the new value list + width. Pointer is
               shared with parsed[edit_for_i] which lives on this stack
               frame; that's safe because new_ts is also stack-local and
               doesn't outlive parsed[]. The on-disk fields.conf rewrite
               (lines[]) carries the canonical spec; load_typed_schema
               will re-allocate enum_values from disk on next read. */
            new_ts.fields[i].enum_values    = parsed[edit_for_i].enum_values;
            new_ts.fields[i].n_enum_values  = parsed[edit_for_i].n_enum_values;
            new_ts.fields[i].enum_width     = parsed[edit_for_i].enum_width;
            /* default_kind / default_val: as of PR #66 parse_field_line
               strips the default-modifier suffix into parsed[].default_kind
               / default_val, so we can carry the edit's new default through
               into the in-memory new_ts. The on-disk fields.conf rewrite
               replaces the whole line below, so load_typed_schema will
               re-read the same default on the next request. */
            new_ts.fields[i].default_kind = parsed[edit_for_i].default_kind;
            memcpy(new_ts.fields[i].default_val,
                   parsed[edit_for_i].default_val,
                   sizeof(new_ts.fields[i].default_val));
        }
        new_ts.fields[i].offset = noff;
        noff += new_ts.fields[i].size;
        new_to_old[i] = i;
    }
    new_ts.total_size = noff;

    /* Per-edit dirty tracking. A field is dirty when its encoding actually
       changed; only those fields' indexes need to be rebuilt. */
    char dirty_names[MAX_FIELDS][128];
    int n_dirty = 0;
    for (int e = 0; e < n_edits; e++) {
        int oi = edited_old_idx[e];
        if (field_needs_transform(&old_ts->fields[oi], &new_ts.fields[oi])) {
            size_t nlen = strlen(parsed[e].name);
            if (nlen >= sizeof(dirty_names[0])) nlen = sizeof(dirty_names[0]) - 1;
            memcpy(dirty_names[n_dirty], parsed[e].name, nlen);
            dirty_names[n_dirty][nlen] = '\0';
            n_dirty++;
        }
    }
    int needs_rebuild = (n_dirty > 0);

    char obj_dir[PATH_MAX];
    snprintf(obj_dir, sizeof(obj_dir), "%s/%s", db_root, object);

    if (!needs_rebuild) {
        if (dry_run) {
            OUT("{\"status\":\"ok\",\"dry_run\":true,\"fields\":%d,"
                "\"would_rebuild\":false}\n", n_edits);
            for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
            return 0;
        }
        if (rewrite_fields_conf_for_edit(obj_dir, lines, n_edits) != 0) {
            OUT("{\"error\":\"Failed to rewrite fields.conf\"}\n");
            return 1;
        }
        invalidate_schema_caches(db_root, object);
        LOG_AUDIT(LOG_SUB_CONFIG, "EDIT-FIELD %s/%s: %d fields edited (no-op encoding, fields.conf only)",
                db_root, object, n_edits);
        OUT("{\"status\":\"edited\",\"fields\":%d,\"rebuilt\":false}\n", n_edits);
        for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
        return 0;
    }

    /* Pre-flight: open live slotcask and walk to verify each edited
       field's value fits the new shape on every live record. */
    SlotcaskSchemaInfo info = {
        .splits = old_sch.splits, .slot_size = old_sch.slot_size,
        .streams = old_sch.streams,
    };
    SlotcaskDb *sdb SDB_REG_REF = slotcask_registry_get(db_root, object, &info);
    if (!sdb) {
        OUT("{\"error\":\"Failed to open slotcask for pre-flight\"}\n");
        return 1;
    }
    EditPreflightCtx pf = {0};
    pf.old_ts        = old_ts;
    pf.new_ts        = &new_ts;
    pf.edited_old_idx = edited_old_idx;
    pf.n_edits       = n_edits;
    slotcask_walk_live(sdb, edit_preflight_walk_cb, &pf);
    if (pf.failed) {
        OUT("{\"error\":\"Pre-flight failed on field [%s]: %s\"}\n",
            pf.fail_field, pf.fail_reason);
        return 1;
    }

    /* Dry-run short-circuit: all validation + pre-flight scan passed, but
       the caller only wanted a preview. Don't run the rebuild, don't
       rewrite fields.conf, don't touch indexes. Report what *would* have
       happened so operators can preview a same-type narrow / widen before
       committing to it. */
    if (dry_run) {
        OUT("{\"status\":\"ok\",\"dry_run\":true,\"fields\":%d,"
            "\"would_rebuild\":true}\n", n_edits);
        for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
        return 0;
    }

    /* Compute the new slot_size and run the v2 rebuild. */
    Schema new_sch = old_sch;
    new_sch.max_value = new_ts.total_size;
    new_sch.slot_size = (24 + new_sch.max_key + new_sch.max_value + 7) & ~7;
    if (new_sch.slot_size < 32) new_sch.slot_size = 32;

    /* v2_rebuild_walk_cb takes the verbatim re-insert path when
       slot_changed=0 — which would skip transform_field_value for
       numeric-scale-only edits where slot_size happens to be unchanged.
       For edit-field we always need the recompose path when any field's
       encoding changed (needs_rebuild==1 here). */
    EditFinalizeCtx finalize_ctx = {
        .db_root = db_root, .object = object, .obj_dir = obj_dir,
        .edit_lines = lines, .n_edits = n_edits,
        .dirty_names = dirty_names, .n_dirty = n_dirty,
    };
    RebuildFinalizeOps finalize = {
        .apply_metadata = edit_finalize_metadata,
        .rebuild_indexes = edit_finalize_indexes,
        .ctx = &finalize_ctx,
        .indexes_may_change = n_dirty > 0,
    };
    int rc = rebuild_object_v2(db_root, object, &old_sch, old_ts,
                                &new_sch, &new_ts, new_to_old,
                                1 /* slot_changed → force recompose */,
                                0 /* splits_changed */,
                                0 /* drop_tombstoned */,
                                NULL /* added_lines */, 0 /* n_added */,
                                &finalize);
    if (rc != 0) {
        /* rebuild_object_v2 already emitted an {"error":..} response. */
        return rc;
    }

    LOG_AUDIT(LOG_SUB_CONFIG, "EDIT-FIELD %s/%s: %d fields edited, slot_size=%d→%d, "
               "idx_rebuilt=%d, idx_skipped=%d",
            db_root, object, n_edits, old_sch.slot_size, new_sch.slot_size,
            finalize_ctx.idx_rebuilt, finalize_ctx.idx_skipped);
    OUT("{\"status\":\"edited\",\"fields\":%d,\"rebuilt\":true,"
        "\"slot_size\":%d,\"indexes_rebuilt\":%d,\"indexes_skipped\":%d}\n",
        n_edits, new_sch.slot_size, finalize_ctx.idx_rebuilt,
        finalize_ctx.idx_skipped);
    for (int _i = 0; _i < n_edits; _i++) free_enum_values(&parsed[_i]);
    return 0;
}

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
    if (strcmp(type, "float") == 0)  return 4;
    if (strcmp(type, "bool") == 0)   return 1;
    if (strcmp(type, "byte") == 0)   return 1;
    if (strcmp(type, "date") == 0)   return 4;
    if (strcmp(type, "datetime") == 0) return 6;
    if (strcmp(type, "datetimems") == 0) return 8;
    if (strcmp(type, "time") == 0)    return 3;
    if (strcmp(type, "timestamp") == 0) return 8;
    if (strcmp(type, "uuid") == 0)    return 16;
    if (strcmp(type, "ipv4") == 0)    return 4;
    if (strcmp(type, "ipv6") == 0)    return 16;
    if (strcmp(type, "currency") == 0) return 8;
    if (strncmp(type, "numeric:", 8) == 0) return 8;
    if (strncmp(type, "enum(", 5) == 0) {
        /* Validate enum(v1,v2,...) — empty list, duplicates, missing
           close paren, and over-ceiling all → invalid (return 0).
           Returns the storage byte width: 1 for ≤256 values, 2 for
           257..65535. create-object is cold path, simple alloc is fine. */
        const char *open  = type + 5;
        const char *close = strrchr(open, ')');
        if (!close || close <= open) return 0;

        /* Pass 1: count + duplicate-check via a heap-allocated value list.
           A worst-case 65535-entry list of avg ~8B = ~640 KB peak — fine. */
        char **vals = calloc(65536, sizeof(char *));
        if (!vals) return 0;
        int n = 0;
        const char *p = open;
        while (p < close) {
            const char *next = memchr(p, ',', (size_t)(close - p));
            if (!next) next = close;
            size_t vl = (size_t)(next - p);
            if (vl == 0) goto enum_invalid;            /* empty value */
            if (n >= 65535)            goto enum_invalid;
            char *v = malloc(vl + 1);
            if (!v) goto enum_invalid;
            memcpy(v, p, vl); v[vl] = '\0';
            for (int i = 0; i < n; i++) {
                if (strcmp(vals[i], v) == 0) {  /* duplicate */
                    free(v);
                    goto enum_invalid;
                }
            }
            vals[n++] = v;
            p = (next < close) ? next + 1 : close;
        }
        if (n == 0) goto enum_invalid;
        int width = (n <= 256) ? 1 : 2;
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return width;

enum_invalid:
        for (int i = 0; i < n; i++) free(vals[i]);
        free(vals);
        return 0;
    }
    return 0;
}

/* Validate + parse the auto_key spec. Returns 0 on AK_NONE, 1 on
   AK_UUID, 2 on AK_SEQ (with seq_name filled). On invalid input writes
   the {"error":...} JSON to OUT and returns -1. Empty/NULL → 0. */
static int parse_auto_key_spec(const char *spec, char *out_seq_name, size_t seq_cap) {
    if (!spec || !spec[0]) return 0;
    if (strcmp(spec, "uuid") == 0) return 1;
    if (strncmp(spec, "seq(", 4) == 0) {
        size_t slen = strlen(spec);
        if (slen < 6 || spec[slen - 1] != ')') {
            OUT("{\"error\":\"Invalid auto_key=seq(...) form; expected seq(<name>)\"}\n");
            return -1;
        }
        size_t nlen = slen - 5;  /* "seq(" + ")" */
        if (nlen >= seq_cap) {
            OUT("{\"error\":\"auto_key=seq(<name>): name too long (max %zu)\"}\n", seq_cap - 1);
            return -1;
        }
        /* Validate name characters: no `:`, `/`, `+`, spaces, `)` etc. */
        for (size_t i = 0; i < nlen; i++) {
            char c = spec[4 + i];
            if (c == ':' || c == '/' || c == '+' || c == '\\' || c == ' ' ||
                c == '\t' || c == '\n' || c == '\r' || c == '(' || c == ')') {
                OUT("{\"error\":\"Invalid sequence name in auto_key=seq(...)\"}\n");
                return -1;
            }
        }
        if (nlen == 0) {
            OUT("{\"error\":\"auto_key=seq(): sequence name is empty\"}\n");
            return -1;
        }
        memcpy(out_seq_name, spec + 4, nlen);
        out_seq_name[nlen] = '\0';
        return 2;
    }
    OUT("{\"error\":\"Unknown auto_key mode '%s'; expected 'uuid' or 'seq(<name>)'\"}\n", spec);
    return -1;
}

/* Worker for parallel bitmap-shard materialisation inside cmd_create_object.
   Each task creates one (field, shard) bitmap file; tasks are independent so
   no locking is needed beyond what bm_open already provides internally. */
typedef struct {
    char    path[PATH_MAX];
    int     slots_per_shard;
    int     is_bool;
    uint32_t max_values;
} CreateBmArg;

static void *create_bm_worker(void *raw) {
    CreateBmArg *a = (CreateBmArg *)raw;
    BitmapShard *bm = bm_open(a->path, a->slots_per_shard, 1, a->is_bool,
                               a->max_values, 1 /* writer: materialise file */);
    if (bm) bm_close(bm);
    /* Best-effort — same policy as the old serial loop. */
    return NULL;
}

int cmd_create_object(const char *db_root, const char *dir, const char *object,
                      const char *fields_json, const char *indexes_json,
                      int splits, int max_key, int if_not_exists,
                      const char *auto_key_spec) {
    if (!dir || !dir[0]) {
        OUT("{\"error\":\"dir is required\"}\n");
        return 1;
    }
    if (!object || !object[0]) {
        OUT("{\"error\":\"object is required\"}\n");
        return 1;
    }
    if (!is_valid_object(object)) {
        OUT("{\"error\":\"invalid object name (no /,\\\\, leading dot, control chars; max 255 bytes)\"}\n");
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
            OUT("{\"error\":\"invalid field type: \\\"%s\\\" — valid types: varchar:N, int, long, short, double, float, bool, byte, date, datetime, datetimems, time, timestamp, uuid, ipv4, ipv6, currency, numeric:P,S, enum(v1,v2,...)\"}\n",
                   field_specs[nfields]);
            return 1;
        }

        /* enum + :default=X — refuse at create-object if X isn't in the
           declared value list. The spec layout puts :default= AFTER the
           enum's closing paren, so the strstr lookup is unambiguous. */
        const char *spec = field_specs[nfields];
        const char *enum_open = strstr(spec, ":enum(");
        const char *dflt = strstr(spec, ":default=");
        if (enum_open && dflt && dflt > enum_open) {
            /* Default literal runs from after ":default=" to end of spec
               (or to the next ':' modifier — but none follow :default in
               the current grammar). */
            const char *dval = dflt + 9;
            size_t dlen = strlen(dval);
            const char *close = strchr(enum_open + 6, ')');
            if (!close) close = dval - 1;  /* defensive */
            int in_list = 0;
            const char *p = enum_open + 6;
            while (p < close) {
                const char *next = memchr(p, ',', (size_t)(close - p));
                if (!next) next = close;
                size_t vl = (size_t)(next - p);
                if (vl == dlen && memcmp(p, dval, dlen) == 0) {
                    in_list = 1;
                    break;
                }
                p = (next < close) ? next + 1 : close;
            }
            if (!in_list) {
                OUT("{\"error\":\"enum field [%s]: default value \\\"%.*s\\\" is not in the declared value list\"}\n",
                    field_specs[nfields], (int)dlen, dval);
                return 1;
            }
        }

        total_value_size += field_size;
        nfields++;
    }

    if (nfields == 0) {
        OUT("{\"error\":\"fields array is empty — at least one typed field required\"}\n");
        return 1;
    }

    /* Defaults + strict validation. As of 2026.05.1, splits must be a
       power of 2 in [8, 4096]. The per-shard index layout
       (index_splits_for(splits) — see types.h for the curve) relies on
       this regularity. */
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

    /* Parse + validate `indexes` into an in-memory list of (name, type) so
       we can:
         1. Reject unknown types / type-field mismatches upfront.
         2. Write `<obj>/indexes/index.conf` from the canonicalised list
            rather than re-parsing the JSON.

       Line format on disk is `name` (legacy → IT_BTREE) or `name:type`.
       Only user-declared indexes are created — there is no auto-default.
       (Bare `bool`/`enum` names still promote to bitmap via
       idx_should_auto_bitmap: a declared index with an automatic type.)
       Composite indexes (`f1+f2`) are btree-only in 2026.05.7. */
    struct ParsedIdx {
        char name[256];
        enum IndexType type;
        uint32_t max_values;   /* bitmap-only: 0 = default (BM_DEFAULT_MAX_VALUES) */
    };
    struct ParsedIdx pidx[MAX_FIELDS];
    int npidx = 0;

    /* Type token (the substring after the first ':') of the first field
       named fname, or NULL. First-name-match-wins: a duplicate field
       name resolves to its first declaration. */
    #define FIELD_TYPE_TOKEN(fname, fnlen)                                    \
        ({                                                                    \
            const char *_tok = NULL;                                          \
            for (int _i = 0; _i < nfields; _i++) {                            \
                const char *_c = strchr(field_specs[_i], ':');                \
                if (!_c) continue;                                            \
                int _nlen = (int)(_c - field_specs[_i]);                      \
                if (_nlen != (fnlen)) continue;                               \
                if (memcmp(field_specs[_i], (fname), _nlen) != 0) continue;   \
                _tok = _c + 1;                                                \
                break;                                                        \
            }                                                                 \
            _tok;                                                             \
        })
    /* The delimiter tail check is what keeps "timestamp" from matching
       "time"; an enum spec continues with '(' after the token, so it can
       never pass this check — FIELD_TYPE_PREFIX_IS covers it. */
    #define FIELD_TYPE_IS(fname, fnlen, expected_tname)                       \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = 0;                                                 \
            if (_tok) {                                                       \
                size_t _elen = strlen(expected_tname);                        \
                if (strncmp(_tok, (expected_tname), _elen) == 0 &&            \
                    (_tok[_elen] == '\0' || _tok[_elen] == ':'))              \
                    _matched = 1;                                             \
            }                                                                 \
            _matched;                                                         \
        })
    #define FIELD_TYPE_PREFIX_IS(fname, fnlen, prefix)                        \
        ({                                                                    \
            const char *_tok = FIELD_TYPE_TOKEN((fname), (fnlen));            \
            int _matched = _tok != NULL &&                                    \
                           strncmp(_tok, (prefix), strlen(prefix)) == 0;      \
            _matched;                                                         \
        })

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

                    if (ilen <= 0 || ilen >= 256) {
                        OUT("{\"error\":\"invalid index spec (empty or >255 chars)\"}\n");
                        return 1;
                    }
                    if (npidx >= MAX_FIELDS) {
                        OUT("{\"error\":\"too many index specs (max %d)\"}\n", MAX_FIELDS);
                        return 1;
                    }

                    char raw_spec[256];
                    memcpy(raw_spec, istart, ilen);
                    raw_spec[ilen] = '\0';

                    /* Canonical syntactic parse — same helper reindex uses
                       (config.c::parse_index_spec). Errors out on
                       unparsable `bitmap(N)`. */
                    ParsedIndexSpec ps;
                    if (parse_index_spec(raw_spec, &ps) != 0) {
                        OUT("{\"error\":\"invalid index spec \\\"%s\\\"\"}\n", raw_spec);
                        return 1;
                    }
                    /* Cap range check (semantic, not syntactic). */
                    if (ps.type == IT_BITMAP && ps.max_values != 0 &&
                        (ps.max_values < 2 || ps.max_values > BM_HARD_CEILING)) {
                        OUT("{\"error\":\"bitmap cap %u out of range [2, %u] in \\\"%s\\\"\"}\n",
                            ps.max_values, BM_HARD_CEILING, raw_spec);
                        return 1;
                    }
                    /* Composite indexes are btree-only in 2026.05.7. */
                    if (ps.is_composite && ps.type != IT_BTREE) {
                        OUT("{\"error\":\"composite indexes are btree-only (got \\\"%s\\\"); declare each field separately if you need bitmap/trigram\"}\n",
                            raw_spec);
                        return 1;
                    }

                    /* Walk the composite — validate every part exists and
                       that the field's storage type matches the index
                       type's contract. */
                    char check[256];
                    strncpy(check, ps.name, 255); check[255] = '\0';
                    char *_tok_save = NULL; char *tok = strtok_r(check, "+", &_tok_save);
                    while (tok) {
                        int tok_len = (int)strlen(tok);
                        int found = 0;
                        for (int i = 0; i < nfields; i++) {
                            const char *c = strchr(field_specs[i], ':');
                            int nlen = c ? (int)(c - field_specs[i]) : (int)strlen(field_specs[i]);
                            if (tok_len == nlen && memcmp(tok, field_specs[i], nlen) == 0) {
                                found = 1; break;
                            }
                        }
                        if (!found) {
                            OUT("{\"error\":\"index field \\\"%s\\\" not found in fields\"}\n", tok);
                            return 1;
                        }
                        if (!ps.is_composite) {
                            if (ps.type == IT_BITMAP) {
                                if (!FIELD_TYPE_IS(tok, tok_len, "bool") &&
                                    !FIELD_TYPE_IS(tok, tok_len, "varchar") &&
                                    !FIELD_TYPE_PREFIX_IS(tok, tok_len, "enum(")) {
                                    OUT("{\"error\":\"bitmap index requires bool, varchar or enum field (got \\\"%s\\\")\"}\n", tok);
                                    return 1;
                                }
                            } else if (ps.type == IT_TRIGRAM) {
                                if (!FIELD_TYPE_IS(tok, tok_len, "varchar")) {
                                    OUT("{\"error\":\"trigram index requires varchar field (got \\\"%s\\\")\"}\n", tok);
                                    return 1;
                                }
                            }
                        }
                        tok = strtok_r(NULL, "+", &_tok_save);
                    }

                    /* Auto-promote (single source of truth for the rule —
                       config.c::idx_should_auto_bitmap). Find this field's
                       FieldType from field_specs[], pass to the rule. For
                       FT_ENUM also pick the right bitmap cap: 1-byte enum
                       gets the default (256), 2-byte enum needs 65535 so
                       the dict can grow to the enum's full domain. */
                    if (!ps.is_composite) {
                        int fnlen2 = (int)strlen(ps.name);
                        for (int i = 0; i < nfields; i++) {
                            const char *c = strchr(field_specs[i], ':');
                            if (!c) continue;
                            int nl = (int)(c - field_specs[i]);
                            if (nl != fnlen2) continue;
                            if (memcmp(field_specs[i], ps.name, nl) != 0) continue;
                            TypedField tf = {0};
                            parse_field_type(c + 1, &tf);
                            if (idx_should_auto_bitmap(ps.had_explicit_type, tf.type)) {
                                ps.type = IT_BITMAP;
                            }
                            /* 2-byte enums need the full-domain cap whether the
                               bitmap came from a bare promote or an explicit
                               name:bitmap (explicit bitmap(N) overrides). */
                            if (ps.type == IT_BITMAP && tf.type == FT_ENUM &&
                                tf.enum_width == 2 && ps.max_values == 0) {
                                ps.max_values = 65535;
                            }
                            free_enum_values(&tf);
                            break;
                        }
                    }

                    strncpy(pidx[npidx].name, ps.name, sizeof(pidx[npidx].name) - 1);
                    pidx[npidx].name[sizeof(pidx[npidx].name) - 1] = '\0';
                    pidx[npidx].type = ps.type;
                    pidx[npidx].max_values = ps.max_values;
                    npidx++;
                } else p++;
            }
        }
    }

    #undef FIELD_TYPE_IS
    #undef FIELD_TYPE_PREFIX_IS
    #undef FIELD_TYPE_TOKEN

    /* All validation passed — invalidate caches (in case object is being recreated) */
    invalidate_idx_cache(db_root, object);
    char inv_path[PATH_MAX];
    snprintf(inv_path, sizeof(inv_path), "%s/%s/%s", db_root, dir, object);

    /* Now create on disk */
    char eff_root[PATH_MAX];
    snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);

    char path[PATH_MAX];
    /* Slotcask uses keyfile_*.kf + stream_NNN/ created lazily by
       slotcask_open() below. */
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
    /* Streams count for slotcask: hardcoded by nproc at create time so subsequent
       opens use the same count (stream_id is on-disk in the keyfile entry). */
    int streams = slotcask_streams_for_nproc();

    /* Parse + validate auto_key_spec. Refuses cross-version invariants
       (uuid needs max_key>=16, seq needs max_key>=8). On AK_SEQ,
       pre-initialise the sequence file so the first `next` returns 1. */
    char auto_seq_name[128] = {0};
    int auto_key_kind = parse_auto_key_spec(auto_key_spec, auto_seq_name, sizeof(auto_seq_name));
    if (auto_key_kind < 0) return 1;  /* error already emitted */
    if (auto_key_kind == 1 && max_key < 16) {
        OUT("{\"error\":\"auto_key=uuid requires max_key>=16 (got %d)\"}\n", max_key);
        return 1;
    }
    if (auto_key_kind == 2 && max_key < 8) {
        OUT("{\"error\":\"auto_key=seq(...) requires max_key>=8 (got %d)\"}\n", max_key);
        return 1;
    }
    if (auto_key_kind == 2) {
        /* Pre-initialise the sequence file at 0 — first `next` returns 1.
           Use O_CREAT|O_EXCL so create-if-absent is one atomic syscall —
           the previous stat() + fopen("w") pattern was a TOCTOU race
           (CodeQL #89): an attacker who could win the gap between the
           two calls could swap the path target for a symlink and
           redirect the write. EEXIST is the silent no-op we want; any
           other errno is logged but non-fatal (sequence file is
           recoverable via reindex). */
        char seq_dir[PATH_MAX], seq_path[PATH_MAX];
        snprintf(seq_dir,  sizeof(seq_dir),  "%s/%s/%s/metadata/sequences", db_root, dir, object);
        snprintf(seq_path, sizeof(seq_path), "%s/%s", seq_dir, auto_seq_name);
        mkdirp(seq_dir);
        int sfd = open(seq_path, O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (sfd >= 0) {
            (void)write(sfd, "0\n", 2);
            close(sfd);
        } else if (errno != EEXIST) {
            LOG_ERROR(LOG_SUB_CONFIG, "auto_key seq init: open(%s) failed: %s",
                    seq_path, strerror(errno));
        }
    }

    if (!exists) {
        f = fopen(schema_path, "a");
        if (f) {
            /* Schema line: dir:object:splits:max_key:2:streams[:auto_key=...].
               The literal `2` is the storage-version slot — kept in the on-disk
               format for forward compatibility with future engine versions,
               even though every object the daemon writes is v2. */
            if (auto_key_kind == 1) {
                fprintf(f, "%s:%s:%d:%d:2:%d:auto_key=uuid\n",
                        dir, object, splits, max_key, streams);
            } else if (auto_key_kind == 2) {
                fprintf(f, "%s:%s:%d:%d:2:%d:auto_key=seq(%s)\n",
                        dir, object, splits, max_key, streams, auto_seq_name);
            } else {
                fprintf(f, "%s:%s:%d:%d:2:%d\n",
                        dir, object, splits, max_key, streams);
            }
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

    /* Create the on-disk layout (keyfile_*.kf + stream_NNN/) so the object is
       immediately usable. slotcask_open is idempotent — recovery semantics
       handle a re-open if we crash partway. */
    {
        char obj_data_dir[PATH_MAX];
        snprintf(obj_data_dir, sizeof(obj_data_dir), "%s/%s", eff_root, object);
        /* slot_size includes the 24B per-record inline header. */
        int slot_size = (24 + max_key + total_value_size + 7) & ~7;
        if (slot_size < 32) slot_size = 32;
        SlotcaskDb sdb;
        if (slotcask_open(&sdb, obj_data_dir, splits, streams, slot_size) != 0) {
            OUT("{\"error\":\"slotcask_open failed for %s/%s\"}\n", dir, object);
            return 1;
        }
        slotcask_close(&sdb);
    }

    /* Write index.conf from the parsed list — exactly the user-declared
       indexes, nothing else. Line format is `name`
       for btree (back-compat with pre-2026.05.7 readers), `name:type` for
       trigram, or `name:bitmap[(N)]` where the (N) is only emitted when a
       non-default cap was declared. */
    if (npidx > 0) {
        snprintf(path, sizeof(path), "%s/%s/indexes/index.conf", eff_root, object);
        f = fopen(path, "w");
        if (f) {
            for (int i = 0; i < npidx; i++) {
                switch (pidx[i].type) {
                    case IT_BTREE:
                        fprintf(f, "%s\n", pidx[i].name);
                        break;
                    case IT_BITMAP:
                        if (pidx[i].max_values && pidx[i].max_values != BM_DEFAULT_MAX_VALUES) {
                            fprintf(f, "%s:bitmap(%u)\n", pidx[i].name, pidx[i].max_values);
                        } else {
                            fprintf(f, "%s:bitmap\n", pidx[i].name);
                        }
                        break;
                    case IT_TRIGRAM:
                        fprintf(f, "%s:trigram\n", pidx[i].name);
                        break;
                }
            }
            fclose(f);
        }
    }

    /* Materialize bitmap shard files NOW so each file's header carries
       the declared cap (default or override). Empty at create-time;
       CRUD maintains bits on insert/update/delete, reindex backfills
       any records that pre-existed the field. Per declared bitmap field,
       create `splits` per-data-shard files.

       Fan out via parallel_for: flatten (bitmap-field × shard) into one
       task array, then let the thread pool do the ftruncate+mmap calls
       concurrently.  At 256 splits × 1 bool field this is 256 independent
       file-creates that previously ran serially (~2.4 s cold). */
    {
        int slots_per_shard = (int)slotcask_default_slots_for_splits(splits);

        /* First pass: count tasks so we can size the array exactly. */
        int total_bm_tasks = 0;
        for (int i = 0; i < npidx; i++) {
            if (pidx[i].type != IT_BITMAP) continue;
            if (strchr(pidx[i].name, '+')) continue;
            total_bm_tasks += splits;
        }

        if (total_bm_tasks > 0) {
            CreateBmArg *bm_args = malloc((size_t)total_bm_tasks * sizeof(CreateBmArg));
            if (!bm_args) {
                OUT("{\"error\":\"OOM building bitmap prealloc task list\"}\n");
                return 1;
            }
            int idx = 0;
            for (int i = 0; i < npidx; i++) {
                if (pidx[i].type != IT_BITMAP) continue;
                if (strchr(pidx[i].name, '+')) continue;

                /* Find field's storage type to set the bool fast-path flag. */
                int is_bool = 0;
                int fnlen = (int)strlen(pidx[i].name);
                for (int j = 0; j < nfields; j++) {
                    const char *fc = strchr(field_specs[j], ':');
                    if (!fc) continue;
                    int jlen = (int)(fc - field_specs[j]);
                    if (jlen != fnlen) continue;
                    if (memcmp(field_specs[j], pidx[i].name, fnlen) != 0) continue;
                    if (strncmp(fc + 1, "bool", 4) == 0 &&
                        (fc[5] == '\0' || fc[5] == ':')) {
                        is_bool = 1;
                    }
                    break;
                }

                for (int s = 0; s < splits; s++) {
                    bm_build_path(bm_args[idx].path, sizeof(bm_args[idx].path),
                                  eff_root, object, pidx[i].name, s);
                    bm_args[idx].slots_per_shard = slots_per_shard;
                    bm_args[idx].is_bool         = is_bool;
                    bm_args[idx].max_values      = pidx[i].max_values;
                    idx++;
                }
            }
            parallel_for(create_bm_worker, bm_args, total_bm_tasks, sizeof(CreateBmArg));
            free(bm_args);
        }
    }

    LOG_AUDIT(LOG_SUB_CONFIG, "CREATE-OBJECT %s/%s: splits=%d max_key=%d fields=%d streams=%d",
            dir, object, splits, max_key, nfields, streams);
    OUT("{\"status\":\"created\",\"object\":\"%s\",\"dir\":\"%s\",\"splits\":%d,\"max_key\":%d,\"value_size\":%d,\"fields\":%d,\"storage_version\":2,\"streams\":%d}\n",
        object, dir, splits, max_key, total_value_size, nfields, streams);
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

    char eff_obj[PATH_MAX];
    snprintf(eff_obj, sizeof(eff_obj), "%s/%s", dir, object);
    char eff_root[PATH_MAX];
    snprintf(eff_root, sizeof(eff_root), "%s/%s", db_root, dir);

    slotcask_registry_invalidate(eff_root, object);
    invalidate_idx_cache(db_root, object);
    invalidate_schema_caches(db_root, object);
    counts_invalidate(eff_root, object);

    /* Nuke the on-disk object tree. */
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
                LOG_ERROR(LOG_SUB_CONFIG, "drop_object: rename(%s → %s): %s",
                        tmp_path, schema_path, strerror(errno));
                unlink(tmp_path);
            }
        } else {
            fclose(in);
        }
    }

    LOG_AUDIT(LOG_SUB_CONFIG, "DROP-OBJECT %s/%s", dir, object);
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
    const TypeDescriptor *d = type_desc(t);
    return d ? d->name : "unknown";
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
