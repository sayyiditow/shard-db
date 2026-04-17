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

