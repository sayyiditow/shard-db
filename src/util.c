#include "types.h"

/* ========== Utilities ========== */

void mkdirp(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') { *p = '\0'; mkdir(tmp, 0755); *p = '/'; }
    }
    mkdir(tmp, 0755);
}

char *dirname_of(const char *path) {
    static char buf[PATH_MAX];
    snprintf(buf, sizeof(buf), "%s", path);
    char *last = strrchr(buf, '/');
    if (last) *last = '\0';
    return buf;
}

char *read_file(const char *path, size_t *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) return NULL;
    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);
    char *buf = malloc(len + 1);
    fread(buf, 1, len, f);
    buf[len] = '\0';
    fclose(f);
    if (out_len) *out_len = len;
    return buf;
}

/* ========== Minimal JSON helpers ========== */

const char *json_skip(const char *p) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

const char *json_skip_value(const char *p) {
    p = json_skip(p);
    if (*p == '"') {
        p++;
        while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
        if (*p == '"') p++;
        return p;
    }
    if (*p == '{') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            else if (*p == '"') { p++; while (*p && !(*p == '"' && *(p-1) != '\\')) p++; }
            p++;
        }
        return p;
    }
    if (*p == '[') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '[') depth++;
            else if (*p == ']') depth--;
            else if (*p == '"') { p++; while (*p && !(*p == '"' && *(p-1) != '\\')) p++; }
            p++;
        }
        return p;
    }
    while (*p && *p != ',' && *p != '}' && *p != ']' && *p != ' ' && *p != '\n') p++;
    return p;
}

/* Single-pass JSON field extraction.
   Parses the top-level object once, returns value for the requested key.
   Much faster than strstr per field — O(n) single scan vs O(n*k) for k fields. */
char *json_get_field(const char *json, const char *key, int strip_quotes) {
    const char *p = json_skip(json);
    if (*p != '{') return NULL;
    p++;
    size_t keylen = strlen(key);

    while (*p) {
        p = json_skip(p);
        if (*p == '}') return NULL;
        if (*p == ',') { p++; continue; }

        /* Parse field name */
        if (*p != '"') return NULL;
        p++;
        const char *fname = p;
        while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
        size_t flen = p - fname;
        if (*p == '"') p++;

        p = json_skip(p);
        if (*p != ':') return NULL;
        p = json_skip(p + 1);

        /* Check if this is our key */
        if (flen == keylen && memcmp(fname, key, keylen) == 0) {
            const char *vstart = p;
            const char *vend = json_skip_value(p);
            size_t vlen = vend - vstart;
            char *out = malloc(vlen + 1);
            memcpy(out, vstart, vlen);
            out[vlen] = '\0';
            if (strip_quotes && out[0] == '"' && vlen > 1 && out[vlen-1] == '"') {
                memmove(out, out + 1, vlen - 2);
                out[vlen - 2] = '\0';
            }
            return out;
        }

        /* Skip this value */
        p = json_skip_value(p);
    }
    return NULL;
}

char *json_get_string(const char *json, const char *key) {
    return json_get_field(json, key, 1);
}

char *json_get_raw(const char *json, const char *key) {
    return json_get_field(json, key, 1);
}

/* Parse a JSON field that can be a string or array. Returns comma-separated string.
   Input: "name,city" or ["name","city"] → returns "name,city" (malloc'd) */
char *json_get_string_or_array(const char *json, const char *key) {
    char *raw = json_get_field(json, key, 0);
    if (!raw) return NULL;
    if (raw[0] != '[') {
        /* Already a plain string — strip quotes if present */
        if (raw[0] == '"') {
            size_t len = strlen(raw);
            if (len > 1 && raw[len-1] == '"') {
                memmove(raw, raw + 1, len - 2);
                raw[len - 2] = '\0';
            }
        }
        return raw;
    }
    /* Parse JSON array → comma-separated */
    char result[MAX_LINE];
    int pos = 0;
    const char *p = raw + 1; /* skip [ */
    int first = 1;
    while (*p) {
        while (*p == ' ' || *p == ',') p++;
        if (*p == ']') break;
        if (*p == '"') {
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            size_t len = p - start;
            if (!first && pos < MAX_LINE - 1) result[pos++] = ',';
            if (pos + (int)len < MAX_LINE - 1) {
                memcpy(result + pos, start, len);
                pos += len;
            }
            first = 0;
            if (*p == '"') p++;
        } else p++;
    }
    result[pos] = '\0';
    free(raw);
    return strdup(result);
}

/* Extract multiple fields in a single pass. keys[i] -> out_values[i] (caller frees).
   Returns number of fields found. */
int json_get_fields(const char *json, const char **keys, int nkeys, char **out_values) {
    for (int i = 0; i < nkeys; i++) out_values[i] = NULL;

    const char *p = json_skip(json);
    if (*p != '{') return 0;
    p++;
    int found = 0;

    while (*p && found < nkeys) {
        p = json_skip(p);
        if (*p == '}') break;
        if (*p == ',') { p++; continue; }

        if (*p != '"') break;
        p++;
        const char *fname = p;
        while (*p && !(*p == '"' && *(p-1) != '\\')) p++;
        size_t flen = p - fname;
        if (*p == '"') p++;

        p = json_skip(p);
        if (*p != ':') break;
        p = json_skip(p + 1);

        /* Check against all requested keys */
        for (int i = 0; i < nkeys; i++) {
            if (out_values[i]) continue; /* already found */
            size_t klen = strlen(keys[i]);
            if (flen == klen && memcmp(fname, keys[i], klen) == 0) {
                const char *vstart = p;
                const char *vend = json_skip_value(p);
                size_t vlen = vend - vstart;
                char *out = malloc(vlen + 1);
                memcpy(out, vstart, vlen);
                out[vlen] = '\0';
                if (out[0] == '"' && vlen > 1 && out[vlen-1] == '"') {
                    memmove(out, out + 1, vlen - 2);
                    out[vlen - 2] = '\0';
                }
                out_values[i] = out;
                found++;
                break;
            }
        }

        p = json_skip_value(p);
    }
    return found;
}

/* ========== Base64 (RFC 4648) ========== */

static const char B64_ENC[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/* 0..63 for valid base64 alphabet chars; 64 for '='; 0xFF for invalid; 0xFE for whitespace */
static uint8_t b64_dec_table[256];
static int b64_dec_table_init = 0;

static void b64_init_table(void) {
    if (b64_dec_table_init) return;
    for (int i = 0; i < 256; i++) b64_dec_table[i] = 0xFF;
    for (int i = 0; i < 64; i++) b64_dec_table[(uint8_t)B64_ENC[i]] = (uint8_t)i;
    b64_dec_table[(uint8_t)'='] = 64;
    b64_dec_table[(uint8_t)' ']  = 0xFE;
    b64_dec_table[(uint8_t)'\t'] = 0xFE;
    b64_dec_table[(uint8_t)'\r'] = 0xFE;
    b64_dec_table[(uint8_t)'\n'] = 0xFE;
    b64_dec_table_init = 1;
}

size_t b64_encoded_size(size_t raw_len) {
    return ((raw_len + 2) / 3) * 4;
}

/* out must be at least b64_encoded_size(raw_len) + 1 bytes (for NUL). */
void b64_encode(const uint8_t *raw, size_t raw_len, char *out) {
    size_t o = 0;
    size_t i = 0;
    while (i + 3 <= raw_len) {
        uint32_t v = ((uint32_t)raw[i] << 16) | ((uint32_t)raw[i+1] << 8) | (uint32_t)raw[i+2];
        out[o++] = B64_ENC[(v >> 18) & 0x3F];
        out[o++] = B64_ENC[(v >> 12) & 0x3F];
        out[o++] = B64_ENC[(v >>  6) & 0x3F];
        out[o++] = B64_ENC[v         & 0x3F];
        i += 3;
    }
    size_t rem = raw_len - i;
    if (rem == 1) {
        uint32_t v = (uint32_t)raw[i] << 16;
        out[o++] = B64_ENC[(v >> 18) & 0x3F];
        out[o++] = B64_ENC[(v >> 12) & 0x3F];
        out[o++] = '=';
        out[o++] = '=';
    } else if (rem == 2) {
        uint32_t v = ((uint32_t)raw[i] << 16) | ((uint32_t)raw[i+1] << 8);
        out[o++] = B64_ENC[(v >> 18) & 0x3F];
        out[o++] = B64_ENC[(v >> 12) & 0x3F];
        out[o++] = B64_ENC[(v >>  6) & 0x3F];
        out[o++] = '=';
    }
    out[o] = '\0';
}

/* Upper bound for decode output (ignoring whitespace). Real length returned via out_len. */
size_t b64_decoded_maxsize(size_t b64_len) {
    return (b64_len / 4) * 3 + 3;
}

/* Decode b64 input (ignoring whitespace). Returns 0 on success, -1 on invalid char or bad padding.
   out must be at least b64_decoded_maxsize(b64_len) bytes. *out_len set to actual decoded size. */
int b64_decode(const char *b64, size_t b64_len, uint8_t *out, size_t *out_len) {
    b64_init_table();
    uint32_t v = 0;
    int bits = 0;
    int pad = 0;
    size_t o = 0;
    for (size_t i = 0; i < b64_len; i++) {
        uint8_t c = (uint8_t)b64[i];
        uint8_t d = b64_dec_table[c];
        if (d == 0xFE) continue;          /* whitespace */
        if (d == 0xFF) return -1;         /* invalid char */
        if (d == 64) { pad++; continue; } /* '=' — track padding, do not emit */
        if (pad) return -1;               /* non-pad after pad */
        v = (v << 6) | d;
        bits += 6;
        if (bits >= 8) {
            bits -= 8;
            out[o++] = (uint8_t)((v >> bits) & 0xFF);
        }
    }
    if (pad > 2) return -1;
    /* Padding sanity: total sextets (alphabet + pad) must be % 4 == 0.
       We can check by: o computed is correct, but we ensure no spurious bits remain. */
    if (bits >= 6) return -1;
    *out_len = o;
    return 0;
}

/* ========== Filename validation ========== */

/* Reject empty, oversized, absolute, traversal, or control-char names. */
int valid_filename(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 255) return 0;
    if (name[0] == '.' && (n == 1 || (n == 2 && name[1] == '.'))) return 0;
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c < 0x20 || c == 0x7F) return 0;
    }
    /* No component may be "..". Since we disallow '/', the whole name is one component;
       we already rejected "..". Done. */
    return 1;
}

/* ========== Single-pass JSON object parser ==========
   Walks `s` exactly once and records every top-level {"name": value, ...}
   field as a (name, value) span in `out`. Values include surrounding quotes
   for strings and brackets for nested arrays / objects — callers that want
   an unquoted string use json_obj_unquoted(). Input must be NUL-terminated
   (the per-value walkers json_skip / json_skip_value rely on it as a
   secondary bound alongside the span length). */
int json_parse_object(const char *s, size_t slen, JsonObj *out) {
    if (!out) return -1;
    out->n = 0;
    if (!s) return -1;
    const char *p = json_skip(s);
    if (*p != '{') return -1;
    p++;
    const char *end = s + slen;
    while (p < end && *p) {
        p = json_skip(p);
        if (*p == '}') return out->n;
        if (*p == ',') { p++; continue; }
        if (*p != '"') return -1;
        p++;
        const char *name_start = p;
        while (p < end && *p && !(*p == '"' && *(p - 1) != '\\')) p++;
        if (*p != '"') return -1;
        size_t nlen = p - name_start;
        p++;  /* closing " */
        p = json_skip(p);
        if (*p != ':') return -1;
        p = json_skip(p + 1);
        const char *val_start = p;
        p = json_skip_value(p);
        size_t vlen = p - val_start;
        if (out->n >= JSON_OBJ_MAX_FIELDS) {
            /* Too many fields for our fixed-size bucket. Abort; caller can
               still fall back to the legacy per-field walker if this ever
               fires in practice. */
            return -1;
        }
        out->f[out->n].name = name_start;
        out->f[out->n].nlen = nlen;
        out->f[out->n].val  = val_start;
        out->f[out->n].vlen = vlen;
        out->n++;
    }
    return out->n;
}

int json_obj_get(const JsonObj *o, const char *key, const char **val, size_t *vlen) {
    if (!o || !key) return 0;
    size_t klen = strlen(key);
    for (int i = 0; i < o->n; i++) {
        if (o->f[i].nlen == klen && memcmp(o->f[i].name, key, klen) == 0) {
            if (val)  *val  = o->f[i].val;
            if (vlen) *vlen = o->f[i].vlen;
            return 1;
        }
    }
    return 0;
}

int json_obj_unquoted(const JsonObj *o, const char *key, const char **val, size_t *vlen) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return 0;
    if (vl >= 2 && v[0] == '"' && v[vl - 1] == '"') {
        v++;
        vl -= 2;
    }
    if (val)  *val  = v;
    if (vlen) *vlen = vl;
    return 1;
}

int json_obj_int(const JsonObj *o, const char *key, int fallback) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl) || vl == 0) return fallback;
    char buf[32];
    size_t cl = vl < sizeof(buf) - 1 ? vl : sizeof(buf) - 1;
    memcpy(buf, v, cl); buf[cl] = '\0';
    return atoi(buf);
}

int json_obj_copy(const JsonObj *o, const char *key, char *buf, size_t bufsz) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl) || vl == 0 || bufsz == 0) {
        if (bufsz) buf[0] = '\0';
        return 0;
    }
    size_t cl = vl < bufsz - 1 ? vl : bufsz - 1;
    memcpy(buf, v, cl); buf[cl] = '\0';
    return (int)cl;
}

char *json_obj_strdup(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) return NULL;
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}

char *json_obj_strdup_raw(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) return NULL;
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}
