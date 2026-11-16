#include "agg_exact.h"
#include "decimal_order.h"
#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef TEST_BUILD
static _Atomic int g_agg_fail_alloc;
void agg_exact_test_set_fail_alloc(int n) { atomic_store(&g_agg_fail_alloc, n); }
static int fault(void) {
    int n = atomic_load(&g_agg_fail_alloc);
    while (n > 0) {
        if (atomic_compare_exchange_weak(&g_agg_fail_alloc, &n, n - 1))
            return n == 1;
    }
    return 0;
}
#else
static int fault(void) { return 0; }
#endif

static const dec_t ZERO_DEC = {0};

static int fail(int e) { errno = e; return -1; }
static int dec_errno(dec_status st) {
    switch (st) {
    case DEC_OK: return 0;
    case DEC_ERR_NOMEM: return ENOMEM;
    case DEC_ERR_RANGE: case DEC_ERR_PRECISION: return ERANGE;
    default: return EILSEQ;
    }
}

int agg_num_spec_for(const TypedSchema *ts, size_t idx, AggNumSpec *out) {
    memset(out, 0, sizeof *out);
    if (!ts || idx >= (size_t)ts->nfields) return 0;
    const TypedField *f = &ts->fields[idx];
    out->index = idx;
    out->type = f->type;
    switch (f->type) {
    case FT_LONG: case FT_INT: case FT_SHORT: case FT_BYTE: case FT_BOOL:
    case FT_TIMESTAMP:
        out->cls = AGC_INT;
        return 1;
    case FT_DOUBLE: case FT_FLOAT:
        out->cls = AGC_FLT;
        return 1;
    case FT_NUMERIC:
        if (ts->packed.nfields) {
            const PkrField *p = &ts->packed.fields[idx];
            out->cls = AGC_DEC;
            out->bare = p->bare;
            out->dscale = (!p->bare && p->scale > 0) ? p->scale : 0;
            return 1;
        }
        out->cls = AGC_INT;
        out->scale = f->numeric_scale_mult > 0 ? f->numeric_scale : 0;
        return 1;
    default:
        return 0;
    }
}

int agg_index_exact(const AggNumSpec *sp) {
    return !(sp->cls == AGC_DEC && sp->bare);
}

void agg_env_init(AggEnv *e, _Atomic size_t *bytes, size_t limit) {
    memset(e, 0, sizeof *e);
    e->bytes = bytes;
    e->limit = limit;
}
void agg_env_free(AggEnv *e) { dec_free(&e->val); }

static int charge(AggEnv *e, size_t add) {
    if (!e->bytes || !add) return 0;
    size_t prev = atomic_fetch_add_explicit(e->bytes, add, memory_order_relaxed);
    if (prev + add > e->limit) {
        atomic_fetch_sub_explicit(e->bytes, add, memory_order_relaxed);
        return fail(ENOBUFS);
    }
    return 0;
}
static void uncharge(AggEnv *e, size_t n) {
    if (e->bytes && n) atomic_fetch_sub_explicit(e->bytes, n, memory_order_relaxed);
}
static size_t dec_bytes(const dec_t *d) { return (size_t)d->cap * sizeof(uint16_t); }

/* Move `fresh` into `slot`, charging only the storage growth; consumes fresh. */
static int slot_put(AggEnv *e, dec_t *slot, dec_t *fresh) {
    size_t nb = dec_bytes(fresh), ob = dec_bytes(slot);
    if (nb > ob && charge(e, nb - ob)) { dec_free(fresh); return -1; }
    if (nb < ob) uncharge(e, ob - nb);
    dec_free(slot);
    *slot = *fresh;
    dec_init(fresh);
    return 0;
}
static int slot_copy(AggEnv *e, dec_t *slot, const dec_t *src) {
    dec_t fresh; dec_init(&fresh);
    if (fault()) return fail(ENOMEM);
    dec_status st = dec_add(src, &ZERO_DEC, &fresh);   /* exact copy, keeps dscale */
    if (st != DEC_OK) { dec_free(&fresh); return fail(dec_errno(st)); }
    return slot_put(e, slot, &fresh);
}
static int slot_add(AggEnv *e, dec_t *slot, const dec_t *v) {
    dec_t fresh; dec_init(&fresh);
    if (fault()) return fail(ENOMEM);
    dec_status st = dec_add(slot, v, &fresh);
    if (st != DEC_OK) { dec_free(&fresh); return fail(dec_errno(st)); }
    return slot_put(e, slot, &fresh);
}

void agg_num_release(AggNum *n, AggClass cls, AggEnv *e) {
    if (cls == AGC_DEC) {
        uncharge(e, dec_bytes(&n->u.d.sum) + dec_bytes(&n->u.d.min) +
                    dec_bytes(&n->u.d.max));
        dec_free(&n->u.d.sum); dec_free(&n->u.d.min); dec_free(&n->u.d.max);
    }
    memset(n, 0, sizeof *n);
}

int agg_num_offer_int(AggNum *n, agg_i128 v) {
    agg_i128 s;
    if (__builtin_add_overflow(n->u.i.sum, v, &s)) return fail(ERANGE);
    n->u.i.sum = s;
    if (!n->has) { n->u.i.min = n->u.i.max = v; n->has = 1; }
    else { if (v < n->u.i.min) n->u.i.min = v; if (v > n->u.i.max) n->u.i.max = v; }
    return 0;
}
int agg_num_offer_double(AggNum *n, double v) {
    if (!isfinite(v)) return fail(ERANGE);
    if (!n->float_sum_overflow) {
        double s = n->u.f.sum + v;
        if (!isfinite(s)) n->float_sum_overflow = 1;
        else n->u.f.sum = s;
    }
    if (!n->has) { n->u.f.min = n->u.f.max = v; n->has = 1; }
    else { if (v < n->u.f.min) n->u.f.min = v; if (v > n->u.f.max) n->u.f.max = v; }
    return 0;
}
/* min/max with the commutative tie rule: equal value => larger display scale. */
static int dec_minmax(AggNum *n, AggEnv *e, const dec_t *vmin, const dec_t *vmax) {
    if (!n->has) {
        if (slot_copy(e, &n->u.d.min, vmin) || slot_copy(e, &n->u.d.max, vmax)) return -1;
        n->has = 1;
        return 0;
    }
    int c = dec_cmp(vmin, &n->u.d.min);
    if ((c < 0 || (c == 0 && vmin->dscale > n->u.d.min.dscale)) &&
        slot_copy(e, &n->u.d.min, vmin)) return -1;
    c = dec_cmp(vmax, &n->u.d.max);
    if ((c > 0 || (c == 0 && vmax->dscale > n->u.d.max.dscale)) &&
        slot_copy(e, &n->u.d.max, vmax)) return -1;
    return 0;
}
int agg_num_offer_decimal(AggNum *n, AggEnv *e, const dec_t *v) {
    if (slot_add(e, &n->u.d.sum, v)) return -1;
    return dec_minmax(n, e, v, v);
}

/* Big-endian signed reader (record bytes) and ordered-key readers (sign bit
   flipped / IEEE total-order transform). */
static agg_i128 rd_i(const uint8_t *p, int w) {
    uint64_t u = 0;
    for (int i = 0; i < w; i++) u = (u << 8) | p[i];
    return (agg_i128)((int64_t)(u << (64 - 8 * w)) >> (64 - 8 * w));
}
static agg_i128 key_i(const uint8_t *p, int w) {
    uint64_t u = 0;
    for (int i = 0; i < w; i++) u = (u << 8) | p[i];
    u ^= (uint64_t)1 << (8 * w - 1);
    return (agg_i128)((int64_t)(u << (64 - 8 * w)) >> (64 - 8 * w));
}
static double key_d(const uint8_t *p, int w) {
    if (w == 8) {
        uint64_t b = 0;
        for (int i = 0; i < 8; i++) b = (b << 8) | p[i];
        b = (b & ((uint64_t)1 << 63)) ? b ^ ((uint64_t)1 << 63) : ~b;
        double v; memcpy(&v, &b, 8); return v;
    }
    uint32_t b = 0;
    for (int i = 0; i < 4; i++) b = (b << 8) | p[i];
    b = (b & 0x80000000u) ? b ^ 0x80000000u : ~b;
    float f; memcpy(&f, &b, 4); return (double)f;
}

int agg_num_offer_row(const AggNumSpec *sp, const TypedRowView *row,
                      AggNum *n, AggEnv *e) {
    TypedRowSpan span;
    if (typed_row_span(row, sp->index, &span)) return -1;
    if (span.is_null || !span.complete) return 0;
    const uint8_t *p = span.data;
    switch (sp->cls) {
    case AGC_DEC: {
        dec_status st = dec_unpack(span.data, span.len, &e->val);
        if (st != DEC_OK) return fail(dec_errno(st));
        return agg_num_offer_decimal(n, e, &e->val) ? -1 : 1;
    }
    case AGC_FLT: {
        double v;
        if (sp->type == FT_FLOAT) { float f; memcpy(&f, p, 4); v = (double)f; }
        else memcpy(&v, p, 8);
        return agg_num_offer_double(n, v) ? -1 : 1;
    }
    case AGC_INT: {
        agg_i128 v;
        switch (sp->type) {
        case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC: v = rd_i(p, 8); break;
        case FT_INT:   v = rd_i(p, 4); break;
        case FT_SHORT: v = rd_i(p, 2); break;
        case FT_BOOL:  v = p[0] ? 1 : 0; break;
        case FT_BYTE:  v = p[0]; break;
        default: return 0;
        }
        return agg_num_offer_int(n, v) ? -1 : 1;
    }
    default: return 0;
    }
}

static int key_payload(const AggNumSpec *sp, const uint8_t *k, size_t kl,
                       const uint8_t **p, size_t *pl) {
    (void)sp;
    if (!k || kl == 0 || k[0] != 0x00) return 0;     /* NULL tier / foreign tier */
    *p = k + 1; *pl = kl - 1;
    return 1;
}

int agg_key_double(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                   double *out) {
    const uint8_t *p; size_t pl;
    if (sp->cls != AGC_FLT || !key_payload(sp, key, klen, &p, &pl)) return 0;
    int w = sp->type == FT_FLOAT ? 4 : 8;
    if (pl < (size_t)w) return 0;
    *out = key_d(p, w);
    return isfinite(*out) ? 1 : fail(ERANGE);
}

int agg_num_offer_key(const AggNumSpec *sp, const uint8_t *key, size_t klen,
                      AggNum *n, AggEnv *e) {
    const uint8_t *p; size_t pl;
    if (!key_payload(sp, key, klen, &p, &pl)) return 0;
    switch (sp->cls) {
    case AGC_FLT: {
        double v; int r = agg_key_double(sp, key, klen, &v);
        return r == 1 ? (agg_num_offer_double(n, v) ? -1 : 1) : r;
    }
    case AGC_INT: {
        int w;
        switch (sp->type) {
        case FT_LONG: case FT_TIMESTAMP: case FT_NUMERIC: w = 8; break;
        case FT_INT: w = 4; break;
        case FT_SHORT: w = 2; break;
        case FT_BOOL: case FT_BYTE:
            if (pl < 1) return 0;
            return agg_num_offer_int(n, sp->type == FT_BOOL ? (p[0] ? 1 : 0) : p[0]) ? -1 : 1;
        default: return 0;
        }
        if (pl < (size_t)w) return 0;
        return agg_num_offer_int(n, key_i(p, w)) ? -1 : 1;
    }
    case AGC_DEC: {
        if (!agg_index_exact(sp)) return fail(ENOTSUP);   /* caller must gate (D1) */
        if (decimal_order_decode(p, pl, &e->val)) return -1;
        if (e->val.dscale < sp->dscale) e->val.dscale = sp->dscale;
        return agg_num_offer_decimal(n, e, &e->val) ? -1 : 1;
    }
    default: return 0;
    }
}

int agg_num_merge(AggNum *dst, const AggNum *src, AggClass cls, AggEnv *e) {
    switch (cls) {
    case AGC_INT: {
        agg_i128 s;
        if (__builtin_add_overflow(dst->u.i.sum, src->u.i.sum, &s)) return fail(ERANGE);
        dst->u.i.sum = s;
        if (src->has) {
            if (!dst->has) { dst->u.i.min = src->u.i.min; dst->u.i.max = src->u.i.max; dst->has = 1; }
            else {
                if (src->u.i.min < dst->u.i.min) dst->u.i.min = src->u.i.min;
                if (src->u.i.max > dst->u.i.max) dst->u.i.max = src->u.i.max;
            }
        }
        return 0;
    }
    case AGC_FLT: {
        if (src->float_sum_overflow) dst->float_sum_overflow = 1;
        if (!dst->float_sum_overflow) {
            double s = dst->u.f.sum + src->u.f.sum;
            if (!isfinite(s)) dst->float_sum_overflow = 1;
            else dst->u.f.sum = s;
        }
        if (src->has) {
            if (!dst->has) { dst->u.f.min = src->u.f.min; dst->u.f.max = src->u.f.max; dst->has = 1; }
            else {
                if (src->u.f.min < dst->u.f.min) dst->u.f.min = src->u.f.min;
                if (src->u.f.max > dst->u.f.max) dst->u.f.max = src->u.f.max;
            }
        }
        return 0;
    }
    case AGC_DEC:
        if (slot_add(e, &dst->u.d.sum, &src->u.d.sum)) return -1;
        return src->has ? dec_minmax(dst, e, &src->u.d.min, &src->u.d.max) : 0;
    default: return 0;
    }
}

int agg_num_repeat(AggNum *n, AggClass cls, int64_t times, AggEnv *e) {
    if (!n || !n->has || times < 1) return fail(EINVAL);
    if (cls == AGC_FLT) return fail(EINVAL);
    if (times == 1) return 0;
    if (cls == AGC_INT) {
        agg_i128 s;
        if (__builtin_mul_overflow(n->u.i.sum, (agg_i128)times, &s)) return fail(ERANGE);
        n->u.i.sum = s;
        return 0;
    }
    if (cls != AGC_DEC) return fail(EINVAL);
    dec_t base, acc;
    dec_init(&base); dec_init(&acc);
    int rc = slot_copy(e, &base, &n->u.d.sum);
    int have = 0;
    uint64_t t = (uint64_t)times;
    while (!rc && t) {
        if (t & 1) {
            rc = have ? slot_add(e, &acc, &base) : slot_copy(e, &acc, &base);
            have = 1;
        }
        t >>= 1;
        if (!rc && t) rc = slot_add(e, &base, &base);
    }
    if (!rc) rc = slot_copy(e, &n->u.d.sum, &acc);
    int err = errno;
    uncharge(e, dec_bytes(&base) + dec_bytes(&acc));
    dec_free(&base); dec_free(&acc);
    if (rc) { errno = err; return -1; }
    return 0;
}

void agg_value_init(AggValue *v) { memset(v, 0, sizeof *v); }
void agg_value_release(AggValue *v) { dec_free(&v->own); memset(v, 0, sizeof *v); }
void agg_value_from_count(AggValue *v, int64_t c) {
    agg_value_init(v);
    v->cls = AGC_INT; v->present = 1; v->i = c;
}

/* ---- rendering ---- */
static char *render_i128(agg_i128 v, int scale) {
    char rev[48]; size_t nd = 0;
    int neg = v < 0;
    unsigned __int128 m = neg ? (unsigned __int128)0 - (unsigned __int128)v
                              : (unsigned __int128)v;
    do { rev[nd++] = (char)('0' + (int)(m % 10)); m /= 10; } while (m);
    while (nd < (size_t)scale + 1) rev[nd++] = '0';
    char *out = malloc(nd + 4);
    if (!out || fault()) { free(out); errno = ENOMEM; return NULL; }
    size_t o = 0, intd = nd - (size_t)scale;
    if (neg) out[o++] = '-';
    for (size_t i = 0; i < intd; i++) out[o++] = rev[nd - 1 - i];
    if (scale) {
        out[o++] = '.';
        for (size_t i = 0; i < (size_t)scale; i++) out[o++] = rev[scale - 1 - i];
    }
    out[o] = 0;
    return out;
}
char *agg_render_double(double v) {
    if (!isfinite(v)) { errno = ERANGE; return NULL; }
    char buf[40];
    for (int prec = 15; prec <= 17; prec++) {
        snprintf(buf, sizeof buf, "%.*g", prec, v);
        if (strtod(buf, NULL) == v || prec == 17) break;
    }
    char *o = fault() ? NULL : strdup(buf);
    if (!o) errno = ENOMEM;
    return o;
}
char *agg_value_render(const AggValue *v) {
    switch (v->cls) {
    case AGC_INT: return render_i128(v->i, v->scale);
    case AGC_FLT: return agg_render_double(v->f);
    case AGC_DEC: {
        size_t n = dec_text_len(v->dv), got = 0;
        char *o = fault() ? NULL : malloc(n);
        if (!o) { errno = ENOMEM; return NULL; }
        dec_status st = dec_to_text(v->dv, o, n, &got);
        if (st != DEC_OK) { free(o); errno = dec_errno(st); return NULL; }
        return o;
    }
    default: errno = EINVAL; return NULL;
    }
}

static int int_to_dec(agg_i128 v, int scale, dec_t *out) {
    char *t = render_i128(v, scale);
    if (!t) return -1;
    dec_status st = dec_parse(t, strlen(t), out, NULL, 0);
    free(t);
    return st == DEC_OK ? 0 : fail(dec_errno(st));
}

int agg_num_value(const AggNum *n, const AggNumSpec *sp, enum AggFn fn,
                  int64_t count, AggValue *out) {
    agg_value_init(out);
    out->cls = sp->cls;
    out->present = count > 0;
    if (sp->cls == AGC_FLT && n->float_sum_overflow &&
        (fn == AGG_SUM || fn == AGG_AVG)) return fail(ERANGE);
    if (fn == AGG_AVG) {
        if (sp->cls != AGC_FLT) out->cls = AGC_DEC;   /* integer averages are decimals */
        if (count <= 0) return 0;
        if (sp->cls == AGC_FLT) { out->f = n->u.f.sum / (double)count;
            return isfinite(out->f) ? 0 : fail(ERANGE); }
        dec_t sum; dec_init(&sum);
        const dec_t *sp_dec = &n->u.d.sum;
        if (sp->cls == AGC_INT) {
            if (int_to_dec(n->u.i.sum, sp->scale, &sum)) return -1;
            sp_dec = &sum;
        }
        dec_status st = dec_div_avg(sp_dec, (uint64_t)count, &out->own);
        dec_free(&sum);
        if (st != DEC_OK) return fail(dec_errno(st));
        out->cls = AGC_DEC; out->dv = &out->own;
        return 0;
    }
    switch (sp->cls) {
    case AGC_INT:
        out->scale = sp->scale;
        out->i = fn == AGG_SUM ? n->u.i.sum : fn == AGG_MIN ? n->u.i.min : n->u.i.max;
        break;
    case AGC_FLT:
        out->f = fn == AGG_SUM ? n->u.f.sum : fn == AGG_MIN ? n->u.f.min : n->u.f.max;
        break;
    case AGC_DEC:
        out->dv = fn == AGG_SUM ? &n->u.d.sum : fn == AGG_MIN ? &n->u.d.min : &n->u.d.max;
        break;
    default: break;
    }
    return 0;
}

int agg_value_from_label(const AggNumSpec *sp, const char *label, AggValue *out) {
    agg_value_init(out);
    out->cls = sp->cls == AGC_FLT ? AGC_FLT : AGC_DEC;
    out->present = label[0] != '\0';
    if (!out->present) return 0;
    if (sp->cls == AGC_FLT) {
        char *end; out->f = strtod(label, &end);
        return (*end || !isfinite(out->f)) ? fail(EILSEQ) : 0;
    }
    dec_status st = dec_parse(label, strlen(label), &out->own, NULL, 0);
    if (st != DEC_OK) return fail(dec_errno(st));
    out->dv = &out->own;
    return 0;
}

int agg_value_cmp(const AggValue *a, const AggValue *b) {
    if (a->cls == AGC_FLT) return a->f < b->f ? -1 : a->f > b->f;
    if (a->cls == AGC_INT && b->cls == AGC_INT && a->scale == b->scale)
        return a->i < b->i ? -1 : a->i > b->i;
    return dec_cmp(a->dv, b->dv);   /* callers materialise INT as DEC when scales differ */
}

int agg_value_cmp_text(const AggValue *a, const char *lit, int *cmp) {
    if (a->cls == AGC_FLT) {
        char *end; double d = strtod(lit, &end);
        if (end == lit || *end || !isfinite(d)) return fail(EINVAL);
        *cmp = a->f < d ? -1 : a->f > d;
        return 0;
    }
    dec_t l; dec_init(&l);
    dec_status st = dec_parse(lit, strlen(lit), &l, NULL, 0);
    if (st != DEC_OK) { dec_free(&l); return fail(st == DEC_ERR_NOMEM ? ENOMEM : EINVAL); }
    dec_t tmp; dec_init(&tmp);
    const dec_t *lhs = a->dv;
    if (a->cls == AGC_INT) {
        if (int_to_dec(a->i, a->scale, &tmp)) { dec_free(&l); return -1; }
        lhs = &tmp;
    }
    *cmp = dec_cmp(lhs, &l);
    dec_free(&tmp); dec_free(&l);
    return 0;
}

int agg_dec_label(const AggNumSpec *sp, const uint8_t *p, size_t plen,
                  int ordered, char *buf, size_t cap, AggEnv *e) {
    if (sp->cls != AGC_DEC) return fail(EINVAL);
    if (ordered) { if (decimal_order_decode(p, plen, &e->val)) return -1; }
    else {
        dec_status st = dec_unpack(p, plen, &e->val);
        if (st != DEC_OK) return fail(dec_errno(st));
    }
    int32_t md = decimal_min_dscale(&e->val);
    e->val.dscale = sp->bare ? md : (md > sp->dscale ? md : sp->dscale);
    size_t need = dec_text_len(&e->val), got = 0;
    if (need > cap) return fail(ENAMETOOLONG);
    if (dec_to_text(&e->val, buf, cap, &got) != DEC_OK) return fail(EILSEQ);
    return (int)got;   /* dec_to_text reports the length without the NUL */
}
