#include "packed_record.h"
#include <limits.h>
#include <string.h>

static uint32_t get32(const uint8_t *p) {
    return (uint32_t)p[0] << 24 | (uint32_t)p[1] << 16 |
           (uint32_t)p[2] << 8 | p[3];
}
static void put32(uint8_t *p, uint32_t n) {
    p[0] = (uint8_t)(n >> 24); p[1] = (uint8_t)(n >> 16);
    p[2] = (uint8_t)(n >> 8); p[3] = (uint8_t)n;
}
static int uint_token(const char **s, uint32_t limit, uint32_t *out) {
    const char *p = *s;
    if (*p < '0' || *p > '9') return 0;
    uint32_t n = 0;
    do {
        uint32_t d = (uint32_t)(*p - '0');
        if (n > limit / 10 || (n == limit / 10 && d > limit % 10)) return 0;
        n = n * 10 + d; p++;
    } while (*p >= '0' && *p <= '9');
    *s = p; *out = n; return 1;
}
static uint32_t numeric_cap(int32_t precision) {
    /* Canonical coefficient strips zero padding even for negative S or S>P;
       a constrained value needs at most P significant coefficient digits. */
    return precision < 0 ? DEC_PAYLOAD_MAX :
           DEC_PAYLOAD_HEADER + ((uint32_t)precision + 1) / 2;
}
static int field_ok(const PkrField *f) {
    if (!f || f->bare > 1 || f->currency > 1) return 0;
    if (f->kind == PKR_FIXED)
        return f->width >= 1 && f->width <= 16 && !f->payload_max &&
               !f->precision && !f->scale && !f->bare && !f->currency;
    if (f->width != PKR_DESCRIPTOR_BYTES) return 0;
    if (f->kind == PKR_VARCHAR)
        return f->payload_max >= 1 && f->payload_max <= PKR_VARCHAR_MAX &&
               (!f->bare || f->payload_max == PKR_VARCHAR_MAX) &&
               !f->precision && !f->scale && !f->currency;
    if (f->kind != PKR_NUMERIC) return 0;
    if (f->bare)
        return f->precision == -1 && !f->scale && !f->currency &&
               f->payload_max == DEC_PAYLOAD_MAX;
    return f->precision >= 1 && f->precision <= DEC_MAX_PRECISION &&
           f->scale >= DEC_MIN_SCALE && f->scale <= DEC_MAX_SCALE &&
           (!f->currency || (f->precision == 19 && f->scale == 4)) &&
           f->payload_max == numeric_cap(f->precision);
}

dec_status pkr_field_fixed(uint32_t width, PkrField *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!width || width > 16) return DEC_ERR_INVALID;
    out->kind = PKR_FIXED; out->width = width; return DEC_OK;
}
dec_status pkr_field_parse(const char *type, PkrField *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!type) return DEC_ERR_INVALID;
    PkrField f = {0}; f.width = PKR_DESCRIPTOR_BYTES;
    if (!strcmp(type, "varchar")) {
        f.kind = PKR_VARCHAR; f.bare = 1; f.payload_max = PKR_VARCHAR_MAX;
    } else if (!strncmp(type, "varchar:", 8)) {
        const char *p = type + 8; uint32_t n;
        if (!uint_token(&p, PKR_VARCHAR_MAX, &n) || *p || !n)
            return DEC_ERR_INVALID;
        f.kind = PKR_VARCHAR; f.payload_max = n;
    } else if (!strcmp(type, "numeric")) {
        f.kind = PKR_NUMERIC; f.bare = 1; f.precision = -1;
        f.payload_max = DEC_PAYLOAD_MAX;
    } else if (!strcmp(type, "currency")) {
        f.kind = PKR_NUMERIC; f.precision = 19; f.scale = 4; f.currency = 1;
        f.payload_max = numeric_cap(f.precision);
    } else if (!strncmp(type, "numeric:", 8)) {
        const char *p = type + 8; uint32_t precision, scale = 0; int neg = 0;
        if (!uint_token(&p, DEC_MAX_PRECISION, &precision) || !precision)
            return DEC_ERR_INVALID;
        if (*p == ',') {
            p++;
            if (*p == '-' || *p == '+') { neg = *p == '-'; p++; }
            if (!uint_token(&p, DEC_MAX_SCALE, &scale)) return DEC_ERR_INVALID;
        }
        if (*p) return DEC_ERR_INVALID;
        f.kind = PKR_NUMERIC; f.precision = (int32_t)precision;
        f.scale = neg ? -(int32_t)scale : (int32_t)scale;
        f.payload_max = numeric_cap(f.precision);
    } else return DEC_ERR_INVALID;
    *out = f; return DEC_OK;
}

dec_status pkr_schema_build(const PkrField *fields, size_t n, PkrSchema *out) {
    if (!out) return DEC_ERR_INVALID;
    /* Local assembly permits rebuilding from out->fields without mutation
       while validation is in progress. No field owns dynamic memory. */
    PkrSchema next = {0};
    if (!fields || !n || n > PKR_MAX_FIELDS) {
        memset(out, 0, sizeof(*out)); return DEC_ERR_INVALID;
    }
    uint64_t directory = 0, payloads = 0;
    for (size_t i = 0; i < n; i++) {
        if (!field_ok(&fields[i])) {
            memset(out, 0, sizeof(*out)); return DEC_ERR_INVALID;
        }
        next.fields[i] = fields[i];
        next.fields[i].offset = (uint32_t)directory;
        directory += fields[i].width; payloads += fields[i].payload_max;
    }
    uint64_t prefix = 1 + (n + 7) / 8, maximum = prefix + directory + payloads;
    if (maximum > INT32_MAX) {
        memset(out, 0, sizeof(*out)); return DEC_ERR_RANGE;
    }
    next.nfields = (uint32_t)n; next.prefix = (uint32_t)prefix;
    next.directory = (uint32_t)directory; next.maximum = (uint32_t)maximum;
    *out = next; return DEC_OK;
}
static int schema_ok(const PkrSchema *s) {
    if (!s || !s->nfields || s->nfields > PKR_MAX_FIELDS ||
        s->prefix != 1 + (s->nfields + 7) / 8) return 0;
    uint64_t dir = 0, payload = 0;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        if (!field_ok(f) || f->offset != dir) return 0;
        dir += f->width; payload += f->payload_max;
    }
    return dir == s->directory && s->prefix + dir + payload <= INT32_MAX &&
           s->maximum == s->prefix + dir + payload;
}

dec_status pkr_schema_validate(const PkrSchema *schema) {
    return schema_ok(schema)?DEC_OK:DEC_ERR_INVALID;
}

dec_status pkr_numeric_encode(const PkrField *f, const char *text, size_t n,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz) {
    if (err && errsz) err[0] = '\0';
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!field_ok(f) || f->kind != PKR_NUMERIC || !text || !out)
        return DEC_ERR_INVALID;
    dec_t parsed, rounded; dec_init(&parsed); dec_init(&rounded);
    dec_status st = dec_parse(text, n, &parsed, err, errsz);
    const dec_t *value = &parsed;
    if (st == DEC_OK && !f->bare) {
        st = dec_apply_precision(&parsed, f->precision, f->scale,
                                 &rounded, err, errsz);
        value = &rounded;
    }
    if (st == DEC_OK && dec_payload_len(value) > f->payload_max) st = DEC_ERR_RANGE;
    if (st == DEC_OK) st = dec_pack(value, out, cap, used);
    dec_free(&rounded); dec_free(&parsed); return st;
}
static dec_status numeric_payload(const PkrField *f, const uint8_t *p, size_t n) {
    dec_t d, r; dec_init(&d); dec_init(&r);
    dec_status st = dec_unpack(p, n, &d);
    if (st == DEC_OK && !f->bare) {
        st = dec_apply_precision(&d, f->precision, f->scale, &r, NULL, 0);
        if (st == DEC_OK && (dec_cmp(&d, &r) || d.dscale != r.dscale))
            st = DEC_ERR_PAYLOAD;
    }
    dec_free(&r); dec_free(&d);
    if (st == DEC_OK || st == DEC_ERR_NOMEM) return st;
    return DEC_ERR_PAYLOAD;
}
static dec_status cell_ok(const PkrField *f, const PkrCell *c) {
    if (c->is_null > 1) return DEC_ERR_INVALID;
    if (c->is_null) return (!c->data && !c->len) ? DEC_OK : DEC_ERR_INVALID;
    if (f->kind == PKR_FIXED)
        return (c->data && c->len == f->width) ? DEC_OK : DEC_ERR_PAYLOAD;
    if (c->len > f->payload_max) return DEC_ERR_RANGE;
    if (c->len && !c->data) return DEC_ERR_INVALID;
    if (f->kind == PKR_NUMERIC) return numeric_payload(f, c->data, c->len);
    return DEC_OK;
}
dec_status pkr_measure(const PkrSchema *s, const PkrCell *cells, size_t n,
                       size_t *used) {
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!schema_ok(s) || !cells || n != s->nfields) return DEC_ERR_INVALID;
    size_t total = s->prefix + s->directory;
    for (size_t i = 0; i < n; i++) {
        dec_status st = cell_ok(&s->fields[i], &cells[i]);
        if (st != DEC_OK) return st;
        if (!cells[i].is_null && s->fields[i].kind != PKR_FIXED)
            total += cells[i].len;
    }
    if (total > s->maximum) return DEC_ERR_RANGE;
    *used = total; return DEC_OK;
}
static int overlap(const void *a, size_t an, const void *b, size_t bn) {
    if (!an || !bn) return 0;
    uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
    if (an > UINTPTR_MAX - x || bn > UINTPTR_MAX - y) return 1;
    return x < y + bn && y < x + an;
}
dec_status pkr_compose(const PkrSchema *s, const PkrCell *cells, size_t n,
                       uint8_t *out, size_t cap, size_t *used) {
    if (!used) return DEC_ERR_INVALID;
    *used = 0;
    if (!out) return DEC_ERR_INVALID;
    size_t need = 0; dec_status st = pkr_measure(s, cells, n, &need);
    if (st != DEC_OK) return st;
    if (cap < need) return DEC_ERR_TRUNCATED;
    for (size_t i = 0; i < n; i++)
        if (overlap(out, need, cells[i].data, cells[i].len)) return DEC_ERR_INVALID;
    /* No operations after this point allocate or can fail. */
    memset(out, 0, s->prefix + s->directory);
    out[0] = (uint8_t)(s->prefix - 1);
    uint8_t *dir = out + s->prefix;
    size_t cursor = s->directory;
    for (size_t i = 0; i < n; i++) {
        const PkrField *f = &s->fields[i]; const PkrCell *c = &cells[i];
        if (c->is_null) { out[1 + i / 8] |= (uint8_t)(1u << (i % 8)); continue; }
        if (f->kind == PKR_FIXED) memcpy(dir + f->offset, c->data, f->width);
        else if (c->len) {
            put32(dir + f->offset, (uint32_t)cursor);
            put32(dir + f->offset + 4, (uint32_t)c->len);
            memcpy(dir + cursor, c->data, c->len); cursor += c->len;
        }
    }
    *used = need; return DEC_OK;
}

dec_status pkr_view(const PkrSchema *s, const uint8_t *v, size_t n,
                    uint8_t flags, PkrView *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!schema_ok(s) || !v) return DEC_ERR_INVALID;
    if (flags != PKR_FLAGS || n < (size_t)s->prefix + s->directory ||
        n > s->maximum || v[0] != s->prefix - 1) return DEC_ERR_PAYLOAD;
    unsigned rem = s->nfields % 8;
    if (rem && (v[s->prefix - 1] & (uint8_t)(0xFFu << rem))) return DEC_ERR_PAYLOAD;
    const uint8_t *dir = v + s->prefix;
    size_t cursor = s->directory, region = n - s->prefix;
    for (size_t i = 0; i < s->nfields; i++) {
        const PkrField *f = &s->fields[i];
        int is_null = !!(v[1 + i / 8] & (1u << (i % 8)));
        const uint8_t *entry = dir + f->offset;
        if (f->kind == PKR_FIXED) {
            if (is_null) for (size_t j = 0; j < f->width; j++)
                if (entry[j]) return DEC_ERR_PAYLOAD;
            continue;
        }
        uint32_t off = get32(entry), len = get32(entry + 4);
        if (!len) {
            if (off || (!is_null && f->kind == PKR_NUMERIC)) return DEC_ERR_PAYLOAD;
            continue;
        }
        if (is_null || len > f->payload_max || off != cursor ||
            cursor > region || len > region - cursor) return DEC_ERR_PAYLOAD;
        if (f->kind == PKR_NUMERIC) {
            dec_status st = numeric_payload(f, dir + off, len);
            if (st != DEC_OK) return st;
        }
        cursor += len;
    }
    if (cursor != region) return DEC_ERR_PAYLOAD;
    out->schema = s; out->value = v; out->len = n; return DEC_OK;
}
dec_status pkr_span(const PkrView *v, size_t index, PkrSpan *out) {
    if (!out) return DEC_ERR_INVALID;
    memset(out, 0, sizeof(*out));
    if (!v || !schema_ok(v->schema) || !v->value || index >= v->schema->nfields)
        return DEC_ERR_INVALID;
    const PkrSchema *s = v->schema; const PkrField *f = &s->fields[index];
    if (v->value[1 + index / 8] & (1u << (index % 8))) {
        out->state = PKR_NULL; return DEC_OK;
    }
    const uint8_t *dir = v->value + s->prefix;
    const uint8_t *entry = dir + f->offset;
    out->state = PKR_PRESENT;
    if (f->kind == PKR_FIXED) { out->data = entry; out->len = f->width; }
    else {
        out->len = get32(entry + 4);
        out->data = dir + (out->len ? get32(entry) : s->directory);
    }
    return DEC_OK;
}
static dec_status record_bytes(size_t value, size_t key, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!key || key > 1024 || value > SIZE_MAX - 24 - key - 7) return DEC_ERR_RANGE;
    size_t bytes = (24 + key + value + 7) & ~(size_t)7;
    *out = bytes < 32 ? 32 : bytes; return DEC_OK;
}
dec_status pkr_slot_bound(const PkrSchema *s, size_t key, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!schema_ok(s)) return DEC_ERR_INVALID;
    return record_bytes(s->maximum, key, out);
}
dec_status pkr_record_fits(const PkrSchema *s, size_t actual, size_t key,
                           size_t segment_max, size_t *out) {
    if (!out) return DEC_ERR_INVALID;
    *out = 0;
    if (!schema_ok(s)) return DEC_ERR_INVALID;
    if (actual < (size_t)s->prefix + s->directory || actual > s->maximum)
        return DEC_ERR_RANGE;
    size_t bytes = 0; dec_status st = record_bytes(actual, key, &bytes);
    if (st != DEC_OK) return st;
    if (bytes > segment_max) return DEC_ERR_RANGE;
    *out = bytes; return DEC_OK;
}
