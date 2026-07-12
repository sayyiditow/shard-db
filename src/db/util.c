#include "types.h"

#include <errno.h>
#include <fcntl.h>
#include <sys/random.h>
#include <unistd.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

/* Fill buf with n cryptographically-secure random bytes. Primary source is
   getentropy(2) — no fd, so it cannot fail from fd exhaustion and works in
   chroot/sandbox; chunked at 256 bytes (the getentropy per-call cap).
   Fallback is a /dev/urandom read loop for libcs without getentropy.
   Returns 0 on success, -1 if no random source is available. Callers MUST
   check the return: on -1 the buffer contents are unspecified and must not
   be used. */
int fill_random(void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        size_t chunk = (n - off > 256) ? 256 : (n - off);
        if (getentropy(p + off, chunk) != 0) break;
        off += chunk;
    }
    if (off == n) return 0;

    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        fprintf(stderr, "fill_random: getentropy exhausted and open(/dev/urandom) failed: %s\n", strerror(errno));
        return -1;
    }
    off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) {
            fprintf(stderr, "fill_random: read(/dev/urandom) failed or hit EOF: %s\n", r < 0 ? strerror(errno) : "EOF");
            close(fd); return -1;
        }
        off += (size_t)r;
    }
    close(fd);
    return 0;
}

/* ========== Hashing ==========
 * Single source of truth for the engine's primary-key hash. Used by:
 *   - storage.c (cmd_insert/get/delete shard routing via compute_record_shard)
 *   - query.c (btree index entries, RecordRef hash lookups)
 *   - slotcask.c (keyfile entry hash, slotcask_lookup_by_hash)
 * The canonical (big-endian) form makes the hash byte layout host-endian-
 * independent, so identical bytes appear in btree index entries and
 * slotcask keyfile entries on every architecture. Drift here corrupts
 * cross-component lookups silently — all callers MUST go through this
 * function, not roll their own. */
void compute_hash_raw(const char *key, size_t key_len, uint8_t hash_out[16]) {
    XXH128_hash_t h = XXH3_128bits(key, key_len);
    XXH128_canonical_t c;
    XXH128_canonicalFromHash(&c, h);
    memcpy(hash_out, c.digest, 16);
}

/* ========== Utilities ========== */

void mkdirp(const char *path) {
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof(tmp), "%s", path);
    /* mkdir() may fail with EEXIST on intermediate components or the leaf —
       that's the expected case for any path where ancestors already exist.
       Other failures (EACCES, ENOSPC, ENOTDIR) are treated as best-effort:
       callers will hit them again on the open()/write() that follows and
       report a precise error there. Errors emitted to stderr (not log_msg)
       to keep util.c self-contained for the fuzz harness, which links
       util.c standalone without the daemon's logging facility. */
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST)
                fprintf(stderr, "mkdirp: %s: %s\n", tmp, strerror(errno));
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST)
        fprintf(stderr, "mkdirp: %s: %s\n", tmp, strerror(errno));
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
    if (fseek(f, 0, SEEK_END) != 0) {
        fprintf(stderr, "read_file: fseek(SEEK_END) failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    long len = ftell(f);
    if (len < 0) {
        fprintf(stderr, "read_file: ftell failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fprintf(stderr, "read_file: fseek(SEEK_SET) failed on '%s': %s\n", path, strerror(errno));
        fclose(f); return NULL;
    }
    char *buf = malloc((size_t)len + 1);
    if (!buf) {
        fprintf(stderr, "read_file: malloc(%zu) failed for '%s'\n", (size_t)len + 1, path);
        fclose(f); return NULL;
    }
    size_t got = fread(buf, 1, (size_t)len, f);
    buf[got] = '\0';
    fclose(f);
    if (out_len) *out_len = got;
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
    /* The inner string-skip (the `"` branch inside) walks until the closing
       quote OR end-of-buffer. If it ran out at end-of-buffer without finding
       a close, we MUST NOT then `p++` past the NUL terminator — that's a
       heap-buffer-overflow on the next loop check. The `if (!*p) break;`
       guard covers that. Found by libFuzzer; see fuzz/fuzz_json.c. */
    if (*p == '{') {
        int depth = 1; p++;
        while (*p && depth > 0) {
            if (*p == '{') depth++;
            else if (*p == '}') depth--;
            else if (*p == '"') { p++; while (*p && !(*p == '"' && *(p-1) != '\\')) p++; }
            if (!*p) break;
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
            if (!*p) break;
            p++;
        }
        return p;
    }
    while (*p && *p != ',' && *p != '}' && *p != ']' && *p != ' ' && *p != '\n') p++;
    return p;
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
        if (c == '/' || c == '\\' || c == '"' || c < 0x20 || c == 0x7F) return 0;
    }
    /* No component may be "..". Since we disallow '/', the whole name is one component;
       we already rejected "..". Done. */
    return 1;
}

/* Validate an object name arriving from a request. Object names are a single
   path component under $DB_ROOT/<dir>/; they are interpolated directly into
   filesystem paths, so they must never contain a separator or traversal.
   Rejects: empty, > 255 bytes, "/" or "\", control chars, leading '.',
   and the literal "." / "..". Mirrors valid_filename's contract but is named
   for the call sites so the intent is unambiguous. */
int is_valid_object(const char *name) {
    if (!name || !name[0]) return 0;
    size_t n = strlen(name);
    if (n > 255) return 0;
    if (name[0] == '.') return 0;  /* rejects ".", "..", and dotfiles */
    for (size_t i = 0; i < n; i++) {
        unsigned char c = (unsigned char)name[i];
        if (c == '/' || c == '\\' || c < 0x20 || c == 0x7F) return 0;
    }
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
            fprintf(stderr, "json_parse_object: field count exceeds JSON_OBJ_MAX_FIELDS=%d, aborting parse\n", JSON_OBJ_MAX_FIELDS);
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

static int hex_nibble_u8(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
    return -1;
}

int json_unescape_string(const char *in, size_t in_len,
                         char **out_buf, size_t *out_len) {
    if (!out_buf || !out_len) return -1;
    /* Worst case: every escape decodes to fewer bytes than its source,
       so in_len + 1 is a safe upper bound for the output buffer. */
    char *out = malloc(in_len + 1);
    if (!out) {
        fprintf(stderr, "json_unescape_string: malloc(%zu) failed\n", in_len + 1);
        return -1;
    }
    size_t op = 0;
    for (size_t i = 0; i < in_len; i++) {
        char c = in[i];
        if (c != '\\') {
            out[op++] = c;
            continue;
        }
        if (i + 1 >= in_len) { free(out); return -1; }
        char e = in[++i];
        switch (e) {
            case '"':  out[op++] = '"';  break;
            case '\\': out[op++] = '\\'; break;
            case '/':  out[op++] = '/';  break;
            case 'b':  out[op++] = '\b'; break;
            case 'f':  out[op++] = '\f'; break;
            case 'n':  out[op++] = '\n'; break;
            case 'r':  out[op++] = '\r'; break;
            case 't':  out[op++] = '\t'; break;
            case 'u': {
                if (i + 4 >= in_len) { free(out); return -1; }
                int h1 = hex_nibble_u8(in[i + 1]);
                int h2 = hex_nibble_u8(in[i + 2]);
                int h3 = hex_nibble_u8(in[i + 3]);
                int h4 = hex_nibble_u8(in[i + 4]);
                if (h1 < 0 || h2 < 0 || h3 < 0 || h4 < 0) { free(out); return -1; }
                unsigned cp = (h1 << 12) | (h2 << 8) | (h3 << 4) | h4;
                i += 4;
                /* UTF-8 encode the codepoint. Surrogate pairs intentionally
                   pass through as separate 3-byte sequences — JSON callers
                   that need full astral plane support should already be
                   sending UTF-8 directly. */
                if (cp < 0x80) {
                    out[op++] = (char)cp;
                } else if (cp < 0x800) {
                    out[op++] = (char)(0xC0 | (cp >> 6));
                    out[op++] = (char)(0x80 | (cp & 0x3F));
                } else {
                    out[op++] = (char)(0xE0 | (cp >> 12));
                    out[op++] = (char)(0x80 | ((cp >> 6) & 0x3F));
                    out[op++] = (char)(0x80 | (cp & 0x3F));
                }
                break;
            }
            default:
                /* Unknown escape — refuse rather than silently pass through. */
                free(out);
                return -1;
        }
    }
    out[op] = '\0';
    *out_buf = out;
    *out_len = op;
    return 0;
}

char *json_obj_strdup_unescaped(const JsonObj *o, const char *key, size_t *out_len) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl)) return NULL;
    char *buf = NULL; size_t bl = 0;
    if (json_unescape_string(v, vl, &buf, &bl) != 0) return NULL;
    if (out_len) *out_len = bl;
    return buf;
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

/* Returns 1 if the named field is the JSON boolean true OR the string "true";
   0 for false, "false", absent, or any other value.  Uses json_obj_get (raw
   span) so it works for both quoted string values and unquoted JSON booleans.
   No allocation — safe to call from any hot path. */
int json_obj_is_true(const JsonObj *o, const char *key)
{
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return 0;
    /* JSON boolean: true (4 bytes, no quotes) */
    if (vl == 4 && memcmp(v, "true", 4) == 0) return 1;
    /* JSON string: "true" (6 bytes including surrounding quotes) */
    if (vl == 6 && memcmp(v, "\"true\"", 6) == 0) return 1;
    return 0;
}

char *json_obj_strdup(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_unquoted(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) {
        fprintf(stderr, "json_obj_strdup: malloc(%zu) failed for key '%s'\n", vl + 1, key);
        return NULL;
    }
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}

char *json_obj_strdup_raw(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl)) return NULL;
    char *s = malloc(vl + 1);
    if (!s) {
        fprintf(stderr, "json_obj_strdup_raw: malloc(%zu) failed for key '%s'\n", vl + 1, key);
        return NULL;
    }
    memcpy(s, v, vl); s[vl] = '\0';
    return s;
}

/* Flatten a string-or-array field into a malloc'd comma-separated string.
   Matches json_get_string_or_array's legacy semantics but operates on the
   pre-parsed span, avoiding the full json_get_field walk. */
char *json_obj_string_or_array(const JsonObj *o, const char *key) {
    const char *v; size_t vl;
    if (!json_obj_get(o, key, &v, &vl) || vl == 0) return NULL;

    /* Plain string: strip surrounding quotes if present. */
    if (vl >= 2 && v[0] == '"' && v[vl - 1] == '"') {
        char *out = malloc(vl - 1);
        if (!out) {
            fprintf(stderr, "json_obj_string_or_array: malloc(%zu) failed for key '%s'\n", vl - 1, key);
            return NULL;
        }
        memcpy(out, v + 1, vl - 2);
        out[vl - 2] = '\0';
        return out;
    }
    if (v[0] != '[') {
        char *out = malloc(vl + 1);
        if (!out) {
            fprintf(stderr, "json_obj_string_or_array: malloc(%zu) failed for key '%s'\n", vl + 1, key);
            return NULL;
        }
        memcpy(out, v, vl); out[vl] = '\0';
        return out;
    }

    /* JSON array of strings → comma-separated. */
    char buf[MAX_LINE];
    int pos = 0;
    const char *p = v + 1;                    /* skip [ */
    const char *end = v + vl;
    int first = 1;
    while (p < end) {
        while (p < end && (*p == ' ' || *p == ',')) p++;
        if (p >= end || *p == ']') break;
        if (*p == '"') {
            p++;
            const char *start = p;
            while (p < end && *p != '"') p++;
            size_t len = p - start;
            if (!first && pos < MAX_LINE - 1) buf[pos++] = ',';
            if (pos + (int)len < MAX_LINE - 1) {
                memcpy(buf + pos, start, len);
                pos += len;
            }
            first = 0;
            if (p < end && *p == '"') p++;
        } else p++;
    }
    buf[pos] = '\0';
    return strdup(buf);
}

/* Write src[0..slen) to dst[0..dst_cap) as JSON-escaped string contents
   (without the surrounding quotes — caller adds them). Escapes ", \, and
   all C0 control characters (U+0000..U+001F) per RFC 8259. Bytes >= 0x20
   (including UTF-8 multi-byte sequences) pass through unchanged.

   Returns the number of bytes written, or -1 if dst_cap is too small for
   the expanded output. Worst-case expansion is 6x (every byte becomes
   \u00XX); caller sizing dst as 6*slen+1 guarantees success. */
int json_escape_into(char *dst, size_t dst_cap,
                     const char *src, size_t slen) {
    static const char hex[] = "0123456789abcdef";
    size_t out = 0;
    for (size_t i = 0; i < slen; i++) {
        unsigned char c = (unsigned char)src[i];
        const char *e = NULL;
        switch (c) {
        case '"':  e = "\\\""; break;
        case '\\': e = "\\\\"; break;
        case '\b': e = "\\b";  break;
        case '\f': e = "\\f";  break;
        case '\n': e = "\\n";  break;
        case '\r': e = "\\r";  break;
        case '\t': e = "\\t";  break;
        default: break;
        }
        if (e) {
            if (out + 2 > dst_cap) return -1;
            dst[out++] = e[0];
            dst[out++] = e[1];
        } else if (c < 0x20) {
            if (out + 6 > dst_cap) return -1;
            dst[out++] = '\\';
            dst[out++] = 'u';
            dst[out++] = '0';
            dst[out++] = '0';
            dst[out++] = hex[c >> 4];
            dst[out++] = hex[c & 0xF];
        } else {
            if (out + 1 > dst_cap) return -1;
            dst[out++] = (char)c;
        }
    }
    return (int)out;
}

/* ========== FT_UUID helpers ==========
 * Shared by config.c (decode_field_to_buf, typed_get_field_str) and
 * query.c (typed_field_to_buf_raw, decode_idx_to_buf). The all-zero
 * sentinel is the on-disk "unset" marker for a UUID column. */

int uuid_is_zero(const uint8_t b[16]) {
    return (b[0]|b[1]|b[2]|b[3]|b[4]|b[5]|b[6]|b[7]
          | b[8]|b[9]|b[10]|b[11]|b[12]|b[13]|b[14]|b[15]) == 0;
}

int uuid_format_canonical(char *buf, size_t buflen, const uint8_t b[16]) {
    return snprintf(buf, buflen,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0], b[1], b[2],  b[3],  b[4],  b[5],  b[6],  b[7],
        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}
