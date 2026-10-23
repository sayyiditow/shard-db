/* src/db/decnum.c
 *
 * Exact decimal core. See decnum.h for the representation contract.
 * STUB SKELETON: lifecycle is real (the test harness needs it); every
 * mathematical operation reports failure without producing a value.
 * Production bodies land task-by-task in the approved plan.
 */
#include "decnum.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

void dec_init(dec_t *d) {
    d->weight = 0;
    d->dscale = 0;
    d->sign = DEC_SIGN_ZERO;
    d->ndigits = 0;
    d->cap = 0;
    d->digits = NULL;
}

void dec_free(dec_t *d) {
    free(d->digits);
    dec_init(d);
}

dec_status dec_reserve(dec_t *d, uint16_t limbs) {
    if (limbs <= d->cap) return DEC_OK;
    if (limbs > DEC_MAX_LIMBS_ALLOC) return DEC_ERR_RANGE;
    uint16_t ncap = d->cap ? d->cap : 16;
    while (ncap < limbs) {
        if (ncap > UINT16_MAX / 2) { ncap = limbs; break; }
        ncap = (uint16_t)(ncap * 2);
    }
    uint16_t *grown = realloc(d->digits, (size_t)ncap * sizeof(uint16_t));
    if (!grown) return DEC_ERR_NOMEM;
    d->digits = grown;
    d->cap = ncap;
    return DEC_OK;
}

/* Same ASCII set as the scalar conversion boundary. */
static int dec_is_space(char c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static const uint16_t DEC_POW10[4] = { 1, 10, 100, 1000 };

/* Limb holding the given base-10000 place (0 when outside the value). */
static uint16_t dec_limb_at_place(const dec_t *d, int32_t place) {
    if (place > d->weight) return 0;
    int64_t idx = (int64_t)d->weight - (int64_t)place;
    if (idx >= (int64_t)d->ndigits) return 0;
    return d->digits[idx];
}

/* Digits left of the decimal point, upper-bounded and then adjusted for
   leading zero digits of the top limb — PostgreSQL 18 apply_typmod's
   ddigits. Can be <= 0 for values below 1. */
static int32_t dec_ddigits(const dec_t *d) {
    int32_t dd = (d->weight + 1) * DEC_LIMB_DIGITS;
    if (d->ndigits > 0) {
        uint16_t top = d->digits[0];
        if (top < 10) dd -= 3;
        else if (top < 100) dd -= 2;
        else if (top < 1000) dd -= 1;
    }
    return dd;
}


/* Build limbs for value = ±digits × 10^m; digits is canonical (no
   leading/trailing zero digit, ndig >= 1) and the caller has already
   enforced the integer/fraction digit limits, so the limb count is
   bounded by DEC_MAX_LIMBS. */
static dec_status dec_from_digits(dec_t *out, dec_sign sign,
                                  const char *dg, size_t ndig,
                                  long long m, int32_t dscale) {
    dec_init(out);
    out->dscale = dscale;
    out->sign = sign;

    if (m >= 0) {
        long long ivlen = (long long)ndig + m;
        size_t nlimbs = (size_t)((ivlen + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) {
            dec_free(out);   /* failure leaves the canonical zero */
            return st;
        }
        long long lead_pad = (long long)nlimbs * DEC_LIMB_DIGITS - ivlen;
        for (size_t L = 0; L < nlimbs; L++) {
            uint16_t v = 0;
            for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                long long p = (long long)L * DEC_LIMB_DIGITS + q;
                long long k = p - lead_pad;
                int d = (k >= 0 && k < (long long)ndig) ? dg[k] - '0' : 0;
                v = (uint16_t)(v * 10 + d);
            }
            out->digits[L] = v;
        }
        out->weight = (int32_t)nlimbs - 1;
        out->ndigits = (uint16_t)nlimbs;
    } else {
        long long f = -m;                    /* fractional digits */
        long long ip = (long long)ndig + m;  /* integer digits (may be <= 0) */
        size_t nli = ip > 0 ? (size_t)((ip + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS) : 0;
        size_t nlf = (size_t)((f + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
        size_t nlimbs = nli + nlf;
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) {
            dec_free(out);   /* failure leaves the canonical zero */
            return st;
        }
        if (ip > 0) {
            size_t lead_pad = (size_t)nli * DEC_LIMB_DIGITS - (size_t)ip;
            for (size_t L = 0; L < nli; L++) {
                uint16_t v = 0;
                for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                    long long p = (long long)(L * DEC_LIMB_DIGITS + q) -
                                  (long long)lead_pad;
                    int d = (p >= 0 && p < (long long)ndig) ? dg[p] - '0' : 0;
                    v = (uint16_t)(v * 10 + d);
                }
                out->digits[L] = v;
            }
        }
        for (size_t L = 0; L < nlf; L++) {
            uint16_t v = 0;
            for (int q = 0; q < DEC_LIMB_DIGITS; q++) {
                long long j = (long long)L * DEC_LIMB_DIGITS + q;
                long long idx = ip + j;   /* index into dg; <0 or >=ndig pads 0 */
                int d = (idx >= 0 && idx < (long long)ndig) ? dg[idx] - '0' : 0;
                v = (uint16_t)(v * 10 + d);
            }
            out->digits[nli + L] = v;
        }
        out->weight = ip > 0 ? (int32_t)nli - 1 : -1;
        out->ndigits = (uint16_t)nlimbs;
    }
    dec_normalize(out);
    return DEC_OK;
}

void dec_normalize(dec_t *d) {
    size_t lead = 0;
    while (lead < d->ndigits && d->digits[lead] == 0) lead++;
    if (lead > 0) {
        memmove(d->digits, d->digits + lead,
                (size_t)(d->ndigits - lead) * sizeof(uint16_t));
        d->ndigits = (uint16_t)(d->ndigits - lead);
        d->weight -= (int32_t)lead;
    }
    while (d->ndigits > 0 && d->digits[d->ndigits - 1] == 0)
        d->ndigits--;
    if (d->ndigits == 0) {
        d->sign = DEC_SIGN_ZERO;
        d->weight = 0;
    }
}

/* Digit at combined "[int].[frac]" run position p (skips the '.'). */
static char dec_run_digit(const char *val, size_t int_start, size_t int_len,
                          size_t p) {
    return p < int_len ? val[int_start + p]
                       : val[int_start + int_len + 1 + (p - int_len)];
}

dec_status dec_parse(const char *val, size_t vlen, dec_t *out,
                     char *err, size_t errsz) {
    if (err && errsz > 0) err[0] = '\0';
    if (!out) return DEC_ERR_INVALID;
    dec_free(out);   /* release any prior buffer; leaves the canonical zero */
    if (!val) return DEC_ERR_INVALID;
    if (vlen > 0 && memchr(val, 0, vlen) != NULL) {
        if (err && errsz > 0) snprintf(err, errsz, "embedded NUL byte");
        return DEC_ERR_SYNTAX;
    }
    while (vlen > 0 && dec_is_space(val[0])) { val++; vlen--; }
    while (vlen > 0 && dec_is_space(val[vlen - 1])) vlen--;

    size_t i = 0;
    int neg = 0;
    if (vlen > 0 && (val[0] == '+' || val[0] == '-')) {
        neg = (val[0] == '-');
        i = 1;
    }
    size_t int_start = i;
    while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
    size_t int_len = i - int_start;
    size_t frac_len = 0;
    if (i < vlen && val[i] == '.') {
        i++;
        size_t fs = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        frac_len = i - fs;
    }

    long long e_val = 0;
    if (i < vlen && (val[i] == 'e' || val[i] == 'E')) {
        i++;
        int e_neg = 0;
        if (i < vlen && (val[i] == '+' || val[i] == '-')) {
            e_neg = (val[i] == '-');
            i++;
        }
        size_t es = i;
        while (i < vlen && val[i] >= '0' && val[i] <= '9') i++;
        size_t el = i - es;
        if (el == 0) {
            if (err && errsz > 0) snprintf(err, errsz, "expected a decimal number");
            return DEC_ERR_SYNTAX;
        }
        /* Bounded-memory exponent policy: strip leading zeros, then cap
           the significant run. Scanning stays linear in the input with
           zero allocations and no value-proportional work. */
        size_t k = 0;
        while (k < el && val[es + k] == '0') k++;
        if (el - k > 7) {
            if (err && errsz > 0)
                snprintf(err, errsz, "exponent out of supported range");
            return DEC_ERR_EXPONENT;
        }
        for (; k < el; k++) e_val = e_val * 10 + (val[es + k] - '0');
        if (e_neg) e_val = -e_val;
    }

    if (i != vlen || (int_len + frac_len) == 0) {
        if (err && errsz > 0) snprintf(err, errsz, "expected a decimal number");
        return DEC_ERR_SYNTAX;
    }

    long long total = (long long)(int_len + frac_len);
    long long E = e_val;
    long long dscale = (long long)frac_len - E;
    if (dscale < 0) dscale = 0;
    if (dscale > DEC_MAX_DSCALE) {
        if (err && errsz > 0)
            snprintf(err, errsz, "more than %d fractional digits",
                     DEC_MAX_FRAC_DIGITS);
        return DEC_ERR_RANGE;
    }

    long long lz = 0;
    while (lz < total &&
           dec_run_digit(val, int_start, int_len, (size_t)lz) == '0')
        lz++;
    if (lz == total) {
        /* Exact zero: canonical sign and limbs; display scale keeps the
           input's fractional span (already bounded). */
        out->sign = DEC_SIGN_ZERO;
        out->dscale = (int32_t)dscale;
        return DEC_OK;
    }
    long long int_digits = (long long)int_len + E - lz;
    if (int_digits < 1) int_digits = 1;
    if (int_digits > DEC_MAX_INT_DIGITS) {
        if (err && errsz > 0)
            snprintf(err, errsz, "more than %d integer digits",
                     DEC_MAX_INT_DIGITS);
        return DEC_ERR_RANGE;
    }
    long long last = total - 1;
    while (last > lz &&
           dec_run_digit(val, int_start, int_len, (size_t)last) == '0')
        last--;
    long long ndig = last - lz + 1;
    long long trail = total - 1 - last;
    long long m = E - (long long)frac_len + trail;

    if (ndig < 1 || ndig > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)ndig;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
    for (long long p = 0; p < ndig; p++)
        dg[p] = dec_run_digit(val, int_start, int_len, (size_t)(lz + p));
    dec_status st = dec_from_digits(out, neg ? DEC_SIGN_NEG : DEC_SIGN_POS,
                                    dg, (size_t)ndig, m, (int32_t)dscale);
    free(dg);
    return st;
}

int32_t dec_int_digits(const dec_t *d) {
    if (d->ndigits == 0) return 0;
    int32_t dd = dec_ddigits(d);
    return dd < 1 ? 1 : dd;
}

size_t dec_text_len(const dec_t *d) {
    size_t n = 1;   /* NUL */
    if (d->ndigits == 0) {
        n += 1;
    } else {
        if (d->sign == DEC_SIGN_NEG) n += 1;
        n += (size_t)dec_int_digits(d);
    }
    if (d->dscale > 0) n += 1 + (size_t)d->dscale;
    return n;
}

dec_status dec_to_text(const dec_t *d, char *buf, size_t bufsz,
                       size_t *out_len) {
    if (!d || !buf || !out_len) return DEC_ERR_INVALID;
    size_t need = dec_text_len(d);
    if (bufsz < need) return DEC_ERR_TRUNCATED;
    char *p = buf;
    if (d->ndigits == 0) {
        *p++ = '0';
    } else {
        if (d->sign == DEC_SIGN_NEG) *p++ = '-';
        if (d->weight >= 0) {
            for (int32_t pl = d->weight; pl >= 0; pl--) {
                uint16_t v = dec_limb_at_place(d, pl);
                if (pl == d->weight) {
                    /* top limb: trim leading zeros */
                    char tmp[8];
                    int n = snprintf(tmp, sizeof(tmp), "%u", (unsigned)v);
                    memcpy(p, tmp, (size_t)n);
                    p += n;
                } else {
                    char tmp[8];
                    snprintf(tmp, sizeof(tmp), "%04u", (unsigned)v);
                    memcpy(p, tmp, 4);
                    p += 4;
                }
            }
        } else {
            *p++ = '0';
        }
    }
    if (d->dscale > 0) {
        *p++ = '.';
        for (int32_t j = 1; j <= d->dscale; j++) {
            int32_t q = -((j + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
            int32_t idx = 3 + 4 * q + j;
            uint16_t v = dec_limb_at_place(d, q);
            int digit = (v / DEC_POW10[3 - idx]) % 10;
            *p++ = (char)('0' + digit);
        }
    }
    *p = '\0';
    *out_len = need - 1;
    return DEC_OK;
}

static int dec_cmp_mag(const dec_t *a, const dec_t *b) {
    if (a->weight != b->weight) return a->weight > b->weight ? 1 : -1;
    size_t n = a->ndigits > b->ndigits ? a->ndigits : b->ndigits;
    for (size_t k = 0; k < n; k++) {
        int la = k < a->ndigits ? a->digits[k] : 0;
        int lb = k < b->ndigits ? b->digits[k] : 0;
        if (la != lb) return la > lb ? 1 : -1;
    }
    return 0;
}

int dec_cmp(const dec_t *a, const dec_t *b) {
    if (a->sign == DEC_SIGN_ZERO)
        return b->sign == DEC_SIGN_ZERO ? 0 :
               (b->sign == DEC_SIGN_POS ? -1 : 1);
    if (b->sign == DEC_SIGN_ZERO)
        return a->sign == DEC_SIGN_POS ? 1 : -1;
    if (a->sign != b->sign) return a->sign == DEC_SIGN_POS ? 1 : -1;
    int mag = dec_cmp_mag(a, b);
    return a->sign == DEC_SIGN_POS ? mag : -mag;
}

static uint64_t dec_hash_byte(uint64_t h, uint8_t b) {
    h ^= b;
    return h * 1099511628211ULL;   /* FNV-1a 64 */
}

uint64_t dec_hash(const dec_t *d) {
    uint64_t h = 14695981039346656037ULL;   /* standard FNV-1a 64 offset */
    h = dec_hash_byte(h, (uint8_t)d->sign);
    uint32_t w = (uint32_t)d->weight;
    h = dec_hash_byte(h, (uint8_t)(w >> 24));
    h = dec_hash_byte(h, (uint8_t)(w >> 16));
    h = dec_hash_byte(h, (uint8_t)(w >> 8));
    h = dec_hash_byte(h, (uint8_t)w);
    h = dec_hash_byte(h, (uint8_t)(d->ndigits >> 8));
    h = dec_hash_byte(h, (uint8_t)d->ndigits);
    for (size_t k = 0; k < d->ndigits; k++) {
        h = dec_hash_byte(h, (uint8_t)(d->digits[k] >> 8));
        h = dec_hash_byte(h, (uint8_t)d->digits[k]);
    }
    return h;
}



/* Shared aligned add/subtract. Magnitudes are limb arrays with a
   separate sign, so no signed-integer negation ever happens (an
   INT64_MIN-shaped magnitude is never negated in two's complement).
   Alignment runs over base-10000 places from each operand's most
   significant place (weight) down to its lowest (weight - ndigits +
   1); limbs accumulate least-significant-first (carry/borrow need
   that direction) and the result is reversed in place into the
   most-significant-first layout. */
static dec_status dec_add_signed(const dec_t *a, const dec_t *b,
                                 int b_negate, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    if (out == a || out == b) return DEC_ERR_INVALID;   /* never alias */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!a || !b) return DEC_ERR_INVALID;
    dec_sign sa = a->sign;
    dec_sign sb = b->sign;
    if (b_negate) {
        if (sb == DEC_SIGN_POS) sb = DEC_SIGN_NEG;
        else if (sb == DEC_SIGN_NEG) sb = DEC_SIGN_POS;
    }
    int32_t maxds = a->dscale > b->dscale ? a->dscale : b->dscale;

    if (sa == DEC_SIGN_ZERO && sb == DEC_SIGN_ZERO) {
        out->dscale = maxds;
        return DEC_OK;
    }
    /* Zero operands: dec_cmp_mag is a magnitude compare and must never
       see a zero (canonical zero's weight 0 would beat a negative
       weight). The result is the other operand, sign applied. */
    if (sa == DEC_SIGN_ZERO || sb == DEC_SIGN_ZERO) {
        const dec_t *nz = sa == DEC_SIGN_ZERO ? b : a;
        if (nz->ndigits > 0) {
            dec_status st = dec_reserve(out, nz->ndigits);
            if (st != DEC_OK) return st;
            memcpy(out->digits, nz->digits,
                   (size_t)nz->ndigits * sizeof(uint16_t));
            out->ndigits = nz->ndigits;
            out->weight = nz->weight;
            out->sign = sa == DEC_SIGN_ZERO ? sb : sa;
        } else {
            out->sign = DEC_SIGN_ZERO;   /* both effective operands zero */
        }
        out->dscale = maxds;
        return DEC_OK;
    }
    if (sa == sb) {
        /* Magnitudes add. Zero operands contribute nothing; at least
           one is nonzero here, so the result carries that sign. */
        int32_t hi_place = INT32_MIN, lo_place = INT32_MAX;
        if (a->ndigits) {
            if (a->weight > hi_place) hi_place = a->weight;
            if (a->weight - (int32_t)a->ndigits + 1 < lo_place)
                lo_place = a->weight - (int32_t)a->ndigits + 1;
        }
        if (b->ndigits) {
            if (b->weight > hi_place) hi_place = b->weight;
            if (b->weight - (int32_t)b->ndigits + 1 < lo_place)
                lo_place = b->weight - (int32_t)b->ndigits + 1;
        }
        int32_t nlimbs = hi_place - lo_place + 1;
        dec_status st = dec_reserve(out, (uint16_t)(nlimbs + 1));
        if (st != DEC_OK) return st;
        int32_t carry = 0;
        for (int32_t k = 0; k < nlimbs; k++) {
            int32_t place = lo_place + k;
            int32_t s = (int32_t)dec_limb_at_place(a, place) +
                        (int32_t)dec_limb_at_place(b, place) + carry;
            out->digits[k] = (uint16_t)(s % DEC_LIMB_BASE);
            carry = s / DEC_LIMB_BASE;
        }
        int32_t n = nlimbs;
        if (carry) { out->digits[n] = (uint16_t)carry; n++; }
        /* digits[k] holds place lo_place + k (least significant
           first); reverse into the most-significant-first layout. */
        for (int32_t lo = 0, hi = n - 1; lo < hi; lo++, hi--) {
            uint16_t t = out->digits[lo];
            out->digits[lo] = out->digits[hi];
            out->digits[hi] = t;
        }
        out->ndigits = (uint16_t)n;
        out->weight = lo_place + n - 1;
        out->sign = sa;
    } else {
        /* Opposite signs: subtract the smaller magnitude from the
           larger; equal magnitudes cancel to the canonical zero. */
        int mag = dec_cmp_mag(a, b);
        if (mag == 0) {
            out->dscale = maxds;   /* out is already the canonical zero */
            return DEC_OK;
        }
        const dec_t *big = mag > 0 ? a : b;
        const dec_t *small = mag > 0 ? b : a;
        int32_t lo_place =
            small->weight - (int32_t)small->ndigits + 1;
        if (big->weight - (int32_t)big->ndigits + 1 < lo_place)
            lo_place = big->weight - (int32_t)big->ndigits + 1;
        int32_t nlimbs = big->weight - lo_place + 1;
        dec_status st = dec_reserve(out, (uint16_t)nlimbs);
        if (st != DEC_OK) return st;
        int32_t borrow = 0;
        for (int32_t k = 0; k < nlimbs; k++) {
            int32_t place = lo_place + k;
            int32_t d = (int32_t)dec_limb_at_place(big, place) -
                        (int32_t)dec_limb_at_place(small, place) - borrow;
            if (d < 0) { d += DEC_LIMB_BASE; borrow = 1; }
            else borrow = 0;
            out->digits[k] = (uint16_t)d;
        }
        /* reverse into the most-significant-first layout */
        for (int32_t lo = 0, hi = nlimbs - 1; lo < hi; lo++, hi--) {
            uint16_t t = out->digits[lo];
            out->digits[lo] = out->digits[hi];
            out->digits[hi] = t;
        }
        out->ndigits = (uint16_t)nlimbs;
        out->weight = big->weight;
        out->sign = mag > 0 ? sa : sb;
    }
    out->dscale = maxds;
    dec_normalize(out);
    if (dec_int_digits(out) > DEC_MAX_INT_DIGITS) {
        dec_free(out);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}

dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out) {
    return dec_add_signed(a, b, 0, out);
}

dec_status dec_sub(const dec_t *a, const dec_t *b, dec_t *out) {
    return dec_add_signed(a, b, 1, out);
}


/* PostgreSQL 18 round_var: per-position powers for within-limb rounding. */
static const uint16_t DEC_ROUND_POWERS[4] = { 0, 1000, 100, 10 };

/* PostgreSQL 18 round_var, ported to this limb layout: truncate to the
   kept decimal position and round the dropped remainder half away from
   zero, propagating carry into a possible new leading limb. */
dec_status dec_round_scale(dec_t *d, int32_t rscale) {
    if (!d) return DEC_ERR_INVALID;
    /* The display scale is bounded by the frozen 16383 fractional-digit
       domain; a wider scale would pack but never decode. */
    if (rscale > DEC_MAX_DSCALE || rscale < -DEC_MAX_DSCALE) {
        dec_free(d);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    d->dscale = rscale;
    int64_t di = (int64_t)(d->weight + 1) * DEC_LIMB_DIGITS + rscale;
    if (di < 0) {
        d->ndigits = 0;
        d->weight = 0;
        d->sign = DEC_SIGN_ZERO;
        return DEC_OK;
    }
    int32_t keep = (int32_t)((di + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS);
    int32_t di4 = (int32_t)(di % DEC_LIMB_DIGITS);
    if (keep < (int32_t)d->ndigits ||
        (keep == (int32_t)d->ndigits && di4 > 0)) {
        int carry = 0;
        int32_t prop = keep;   /* first limb to receive carry */
        if (di4 == 0) {
            carry = d->digits[keep] >= DEC_LIMB_BASE / 2;
        } else {
            uint16_t pow10 = DEC_ROUND_POWERS[di4];
            int extra = d->digits[keep - 1] % pow10;
            d->digits[keep - 1] = (uint16_t)(d->digits[keep - 1] - extra);
            if (extra >= pow10 / 2) {
                int t = pow10 + d->digits[keep - 1];
                if (t >= DEC_LIMB_BASE) { t -= DEC_LIMB_BASE; carry = 1; }
                d->digits[keep - 1] = (uint16_t)t;
            }
            prop = keep - 1;
        }
        d->ndigits = (uint16_t)keep;
        while (carry) {
            if (prop == 0) {
                /* carry out of the top limb: prepend a new leading limb */
                dec_status st = dec_reserve(d, (uint16_t)(d->ndigits + 1));
                if (st != DEC_OK) {
                    dec_free(d);   /* failure leaves the canonical zero */
                    return st;
                }
                memmove(d->digits + 1, d->digits,
                        (size_t)d->ndigits * sizeof(uint16_t));
                d->digits[0] = 1;
                d->ndigits++;
                d->weight++;
                break;
            }
            prop--;
            int s = d->digits[prop] + carry;
            if (s >= DEC_LIMB_BASE) {
                d->digits[prop] = (uint16_t)(s - DEC_LIMB_BASE);
                carry = 1;
            } else {
                d->digits[prop] = (uint16_t)s;
                carry = 0;
            }
        }
    }
    /* Every publicly returned value meets the canonical invariants
       comparison, hashing and packing rely on: rounding can truncate
       to zero (kept limbs gone) or leave trailing zero limbs. */
    dec_normalize(d);
    /* The carry into a new leading limb can push a limit-respecting
       value one digit past the bare integer limit (a maximum-sized
       value rounded up at scale 0); re-check so a success status never
       escapes with a value the packed decoder would reject. */
    if (dec_int_digits(d) > DEC_MAX_INT_DIGITS) {
        dec_free(d);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}

dec_status dec_apply_precision(const dec_t *in, int32_t precision,
                               int32_t scale, dec_t *out,
                               char *err, size_t errsz) {
    if (err && errsz > 0) err[0] = '\0';
    if (!out) return DEC_ERR_INVALID;
    if (out == in) return DEC_ERR_INVALID;   /* never release aliased input */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!in) return DEC_ERR_INVALID;
    if (precision < 1 || precision > DEC_MAX_PRECISION ||
        scale < DEC_MIN_SCALE || scale > DEC_MAX_SCALE) {
        if (err && errsz > 0)
            snprintf(err, errsz,
                     "precision/scale outside 1..1000 / -1000..1000");
        return DEC_ERR_INVALID;
    }
    /* Clone into out (rounding mutates in place). */
    if (in->ndigits > 0) {
        dec_status st = dec_reserve(out, in->ndigits);
        if (st != DEC_OK) return st;
        memcpy(out->digits, in->digits,
               (size_t)in->ndigits * sizeof(uint16_t));
        out->ndigits = in->ndigits;
        out->weight = in->weight;
        out->sign = in->sign;
    }
    out->dscale = in->dscale;
    dec_status st = dec_round_scale(out, scale);
    if (st != DEC_OK) {
        dec_free(out);
        if (st == DEC_ERR_RANGE) {
            /* Rounding carried past the bare integer limit; with
               maxdigits <= 2000 for every declared (P,S) that is also
               a precision overflow, so constrained assignment keeps
               reporting DEC_ERR_PRECISION. */
            if (err && errsz > 0)
                snprintf(err, errsz,
                         "numeric field overflow: precision %d scale %d "
                         "holds an absolute value below 10^%d",
                         precision, scale, precision - scale);
            return DEC_ERR_PRECISION;
        }
        return st;
    }
    if (out->dscale < 0) out->dscale = 0;   /* never a negative display scale */
    dec_normalize(out);
    /* PostgreSQL 18 apply_typmod precision check, after rounding. */
    int32_t maxdigits = precision - scale;
    int32_t dd = (out->weight + 1) * DEC_LIMB_DIGITS;
    if (dd > maxdigits) {
        for (size_t k = 0; k < out->ndigits; k++) {
            uint16_t dig = out->digits[k];
            if (dig) {
                if (dig < 10) dd -= 3;
                else if (dig < 100) dd -= 2;
                else if (dig < 1000) dd -= 1;
                if (dd > maxdigits) {
                    if (err && errsz > 0)
                        snprintf(err, errsz,
                                 "numeric field overflow: precision %d scale %d "
                                 "holds an absolute value below 10^%d",
                                 precision, scale, maxdigits);
                    dec_free(out);
                    return DEC_ERR_PRECISION;
                }
                break;
            }
            dd -= DEC_LIMB_DIGITS;
        }
    }
    return DEC_OK;
}

/* PostgreSQL 18 select_div_scale (REL_18_0 numeric.c:10135), ported.
   Normalized base-10000 weight and first digit per operand (defaults 0
   for zero), quotient-weight decrement when the first digits order
   ambiguously, 16 significant digits, raw display scales as floors,
   final clamp 0..DEC_AVG_MAX_RSCALE — distinct from the stored
   16383-digit fractional limit. */

static int32_t dec_select_div_scale(const dec_t *v1, const dec_t *v2) {
    int32_t weight1 = 0, weight2 = 0;
    uint16_t firstdigit1 = 0, firstdigit2 = 0;
    for (size_t i = 0; i < v1->ndigits; i++) {
        firstdigit1 = v1->digits[i];
        if (firstdigit1 != 0) {
            weight1 = v1->weight - (int32_t)i;
            break;
        }
    }
    for (size_t i = 0; i < v2->ndigits; i++) {
        firstdigit2 = v2->digits[i];
        if (firstdigit2 != 0) {
            weight2 = v2->weight - (int32_t)i;
            break;
        }
    }
    int32_t qweight = weight1 - weight2;
    if (firstdigit1 <= firstdigit2) qweight--;
    int32_t rscale = DEC_MIN_SIG_DIGITS - qweight * DEC_LIMB_DIGITS;
    if (rscale < v1->dscale) rscale = v1->dscale;
    if (rscale < v2->dscale) rscale = v2->dscale;
    if (rscale < 0) rscale = 0;
    if (rscale > DEC_AVG_MAX_RSCALE) rscale = DEC_AVG_MAX_RSCALE;
    return rscale;
}

dec_status dec_div_avg(const dec_t *sum, uint64_t count, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    if (out == sum) return DEC_ERR_INVALID;   /* never alias */
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!sum) return DEC_ERR_INVALID;
    if (count == 0) return DEC_ERR_DIV_ZERO;

    /* Canonical divisor from the row count (<= 20 digits). */
    char dg[24];
    int nd = 0;
    uint64_t t = count;
    while (t > 0) {
        dg[nd++] = (char)('0' + (int)(t % 10));
        t /= 10;
    }
    for (int k = 0; k < nd / 2; k++) {
        char c = dg[k];
        dg[k] = dg[nd - 1 - k];
        dg[nd - 1 - k] = c;
    }
    dec_t dv;
    dec_status st = dec_from_digits(&dv, DEC_SIGN_POS, dg, (size_t)nd, 0, 0);
    if (st != DEC_OK) return st;

    int32_t rscale = dec_select_div_scale(sum, &dv);

    if (sum->ndigits == 0) {
        dec_free(&dv);
        out->dscale = rscale;   /* out is already the canonical zero */
        return DEC_OK;
    }

    /* PostgreSQL 18 div_var framing: one guard limb beyond the target
       scale makes the final half-away rounding decision exact. */
    int32_t res_weight = sum->weight - dv.weight + 1;
    int32_t res_ndigits =
        res_weight + 1 + (rscale + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS;
    if (res_ndigits < 1) res_ndigits = 1;
    res_ndigits += 1;
    if (res_ndigits > DEC_MAX_LIMBS_ALLOC) {
        dec_free(&dv);
        return DEC_ERR_RANGE;
    }
    st = dec_reserve(out, (uint16_t)res_ndigits);
    if (st != DEC_OK) {
        dec_free(&dv);
        return st;
    }

    /* Exact schoolbook division, one dividend limb at a time. The
       remainder stays below the divisor (< 10000^5 < 2^67), so
       rem*10^4 + limb stays far inside unsigned __int128, and the
       trial quotient (rem/divisor) is always a valid base-10000 limb
       (rem < divisor ⇒ quotient digit <= 10^4 - 1). */
    unsigned __int128 dvmag = 0;
    for (size_t k = 0; k < dv.ndigits; k++)
        dvmag = dvmag * DEC_LIMB_BASE + dv.digits[k];
    /* Scale the limb integer by the exponent of the divisor's lowest
       limb. Normalization strips trailing zero limbs, so that exponent
       (weight - ndigits + 1) is >= 0 for a count, and the product is
       the count itself (< 2^64). */
    int32_t shift = dv.weight - (int32_t)dv.ndigits + 1;
    for (int32_t k = 0; k < shift; k++)
        dvmag *= DEC_LIMB_BASE;

    /* Seed the remainder with the dividend limbs above the first
       quotient place. When the divisor's weight >= 1 the quotient's
       top place sits below the dividend's top, and those high limbs
       must enter the division. The seed is < divisor: it is at most
       10000^(w2-1) while the divisor is >= 10000^(w2-1)*1000, so every
       quotient digit stays a valid base-10000 limb. */
    unsigned __int128 rem = 0;
    for (int32_t p = sum->weight; p > res_weight; p--)
        rem = rem * DEC_LIMB_BASE + dec_limb_at_place(sum, p);
    for (int32_t k = 0; k < res_ndigits; k++) {
        int32_t place = res_weight - k;
        rem = rem * DEC_LIMB_BASE + dec_limb_at_place(sum, place);
        out->digits[k] = (uint16_t)(rem / dvmag);
        rem %= dvmag;
    }
    out->ndigits = (uint16_t)res_ndigits;
    out->weight = res_weight;
    out->sign = sum->sign == DEC_SIGN_NEG ? DEC_SIGN_NEG : DEC_SIGN_POS;
    out->dscale = rscale;
    dec_free(&dv);

    st = dec_round_scale(out, rscale);
    if (st != DEC_OK) return st;
    dec_normalize(out);
    /* RAW-01: rounding can carry the quotient past the bare integer
       limit (a maximum-sized sum rounded up at the selected scale);
       re-check after normalization so a success status never escapes
       with a value the packed decoder would reject. */
    if (dec_int_digits(out) > DEC_MAX_INT_DIGITS) {
        dec_free(out);   /* failure leaves the canonical zero */
        return DEC_ERR_RANGE;
    }
    return DEC_OK;
}

/* Decimal digits carried by one limb (canonical values only). */
static int32_t dec_limb_digits(uint16_t v) {
    return v >= 1000 ? 4 : v >= 100 ? 3 : v >= 10 ? 2 : 1;
}

/* Base-10 exponent of the coefficient's final limb digit. */
static long long dec_coeff_exp_raw(const dec_t *d) {
    return (long long)(d->weight - (int32_t)d->ndigits + 1) * DEC_LIMB_DIGITS;
}

/* Trailing zero digits of the last limb (canonical values only). */
static int32_t dec_last_limb_trailing_zeros(uint16_t v) {
    if (v % 10 != 0) return 0;
    if (v % 100 != 0) return 1;
    if (v % 1000 != 0) return 2;
    return 3;
}

/* Mathematically canonical digit count: strip the last limb's trailing
   zero digits (including its fraction-padding zeros) from the span. */
static long long dec_coeff_digits(const dec_t *d) {
    long long span =
        (long long)(d->ndigits - 1) * DEC_LIMB_DIGITS +
        dec_limb_digits(d->digits[0]);
    return span - dec_last_limb_trailing_zeros(d->digits[d->ndigits - 1]);
}

/* Digit at decimal exponent e of the coefficient (0 = units digit). */
static int dec_coeff_digit(const dec_t *d, long long e) {
    int32_t q = (int32_t)(e / DEC_LIMB_DIGITS) -
                (e % DEC_LIMB_DIGITS < 0 ? 1 : 0);   /* floor division */
    int32_t idx = 3 + 4 * q - (int32_t)e;
    uint16_t v = dec_limb_at_place(d, q);
    return (v / DEC_POW10[3 - idx]) % 10;
}

size_t dec_payload_len(const dec_t *d) {
    long long nd = d->ndigits == 0 ? 0 : dec_coeff_digits(d);
    return DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
}

dec_status dec_pack(const dec_t *d, uint8_t *out, size_t cap,
                    size_t *out_len) {
    if (!d || !out || !out_len) return DEC_ERR_INVALID;
    long long nd = d->ndigits == 0 ? 0 : dec_coeff_digits(d);
    size_t need = DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2);
    if (cap < need) return DEC_ERR_TRUNCATED;
    memset(out, 0, need);
    out[0] = DEC_PAYLOAD_VERSION;
    out[1] = (uint8_t)d->sign;
    /* [2..3] reserved stay 0 */
    uint32_t be32 = (uint32_t)nd;
    out[4] = (uint8_t)(be32 >> 24);
    out[5] = (uint8_t)(be32 >> 16);
    out[6] = (uint8_t)(be32 >> 8);
    out[7] = (uint8_t)be32;
    int64_t m = d->ndigits == 0 ? 0
                                : dec_coeff_exp_raw(d) +
                                  dec_last_limb_trailing_zeros(
                                      d->digits[d->ndigits - 1]);
    uint32_t mbe = (uint32_t)m;
    out[8] = (uint8_t)(mbe >> 24);
    out[9] = (uint8_t)(mbe >> 16);
    out[10] = (uint8_t)(mbe >> 8);
    out[11] = (uint8_t)mbe;
    uint32_t dsbe = (uint32_t)d->dscale;
    out[12] = (uint8_t)(dsbe >> 24);
    out[13] = (uint8_t)(dsbe >> 16);
    out[14] = (uint8_t)(dsbe >> 8);
    out[15] = (uint8_t)dsbe;
    for (long long k = 0; k < nd; k++) {
        int digit = dec_coeff_digit(d, m + (nd - 1 - k));   /* most significant first */
        uint8_t nib = (uint8_t)digit;
        if (k % 2 == 0) out[DEC_PAYLOAD_HEADER + k / 2] |= (uint8_t)(nib << 4);
        else out[DEC_PAYLOAD_HEADER + k / 2] |= nib;
    }
    *out_len = need;
    return DEC_OK;
}

dec_status dec_unpack(const uint8_t *buf, size_t len, dec_t *out) {
    if (!out) return DEC_ERR_INVALID;
    dec_free(out);   /* reused outputs release storage; failures leave zero */
    if (!buf) return DEC_ERR_INVALID;
    if (len < DEC_PAYLOAD_HEADER) return DEC_ERR_PAYLOAD;
    if (buf[0] != DEC_PAYLOAD_VERSION) return DEC_ERR_PAYLOAD;
    dec_sign sign = (dec_sign)buf[1];
    if (sign > DEC_SIGN_NEG) return DEC_ERR_PAYLOAD;
    if (buf[2] != 0 || buf[3] != 0) return DEC_ERR_PAYLOAD;
    long long nd = (long long)((uint32_t)buf[4] << 24 |
                               (uint32_t)buf[5] << 16 |
                               (uint32_t)buf[6] << 8 |
                               (uint32_t)buf[7]);
    long long m = (long long)(int32_t)((uint32_t)buf[8] << 24 |
                                       (uint32_t)buf[9] << 16 |
                                       (uint32_t)buf[10] << 8 |
                                       (uint32_t)buf[11]);
    long long dscale = (long long)((uint32_t)buf[12] << 24 |
                                   (uint32_t)buf[13] << 16 |
                                   (uint32_t)buf[14] << 8 |
                                   (uint32_t)buf[15]);
    if (nd > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    if ((sign == DEC_SIGN_ZERO) != (nd == 0)) return DEC_ERR_PAYLOAD;
    if (len != DEC_PAYLOAD_HEADER + (size_t)((nd + 1) / 2))
        return DEC_ERR_PAYLOAD;
    if (dscale < 0 || dscale > DEC_MAX_DSCALE) return DEC_ERR_PAYLOAD;
    if (nd == 0) {
        /* canonical zero carries no exponent */
        if (m != 0) return DEC_ERR_PAYLOAD;
        out->dscale = (int32_t)dscale;
        return DEC_OK;
    }
    /* Integer digits are the digit count shifted by the exponent; the
       bound holds for every exponent sign (m + nd <= 0 is a value
       below 1 and always fits). */
    if (m + nd > DEC_MAX_INT_DIGITS) return DEC_ERR_RANGE;
    if (m < 0 && -m > DEC_MAX_FRAC_DIGITS) return DEC_ERR_RANGE;
    if (dscale < (m < 0 ? -m : 0)) return DEC_ERR_PAYLOAD;

    if (nd < 1 || nd > DEC_MAX_COEFF_DIGITS) return DEC_ERR_RANGE;
    size_t digit_bytes = (size_t)nd;
    char *dg = malloc(digit_bytes);
    if (!dg) return DEC_ERR_NOMEM;
    for (long long k = 0; k < nd; k++) {
        uint8_t byte = buf[DEC_PAYLOAD_HEADER + k / 2];
        int nib = (k % 2 == 0) ? (byte >> 4) : (byte & 0x0F);
        if (nib > 9) { free(dg); return DEC_ERR_PAYLOAD; }
        dg[k] = (char)('0' + nib);
    }
    if (dg[0] == '0' || dg[nd - 1] == '0') {
        free(dg);
        return DEC_ERR_PAYLOAD;   /* digit string must be canonical */
    }
    if (nd % 2 == 1 &&
        (buf[DEC_PAYLOAD_HEADER + nd / 2] & 0x0F) != 0) {
        free(dg);
        return DEC_ERR_PAYLOAD;   /* unused low nibble must be zero pad */
    }
    dec_status st = dec_from_digits(out, sign, dg, (size_t)nd, m,
                                    (int32_t)dscale);
    free(dg);
    return st;
}
