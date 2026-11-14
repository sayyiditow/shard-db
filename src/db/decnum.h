/* src/db/decnum.h
 *
 * Exact decimal core: sign + base-10000 coefficient + weight, with a
 * separate display scale. No binary floating point anywhere; every
 * operation is checked (allocation, digit limits, declared precision)
 * and reports failures through dec_status. Self-contained by design —
 * this module includes only libc headers so storage, query and
 * aggregate code can consume it without pulling in unrelated internals.
 *
 * Representation: value = ±digits × 10000^weight, digits[0] the most
 * significant limb. The mathematical value is canonical (no leading or
 * trailing zero limbs, one canonical zero); equal values therefore
 * carry identical sign/weight/limbs, which is what dec_cmp and
 * dec_hash read. dscale ("display scale") is metadata: the number of
 * fractional digits to render, preserved from the input so 1.2300 and
 * 1.23 are the same value with different display scales.
 */
#ifndef DECNUM_H
#define DECNUM_H

#include <stdint.h>
#include <stddef.h>

#define DEC_LIMB_DIGITS     4            /* decimal digits per limb */
#define DEC_LIMB_BASE       10000
#define DEC_MAX_INT_DIGITS  131072       /* bare numeric integer-digit limit */
#define DEC_MAX_FRAC_DIGITS 16383        /* bare numeric fractional-digit limit */
#define DEC_MAX_COEFF_DIGITS (DEC_MAX_INT_DIGITS + DEC_MAX_FRAC_DIGITS)
#define DEC_MAX_LIMBS       ((DEC_MAX_COEFF_DIGITS + DEC_LIMB_DIGITS - 1) / DEC_LIMB_DIGITS)
/* Transient ceiling for aligned intermediates (carry limb, rounding
   carry). A contract-valid value never exceeds DEC_MAX_LIMBS; the two
   extra limbs only exist between assembly and normalization. */
#define DEC_MAX_LIMBS_ALLOC (DEC_MAX_LIMBS + 2)
#define DEC_MAX_DSCALE      16383
#define DEC_MAX_PRECISION   1000
#define DEC_MAX_SCALE       1000
#define DEC_MIN_SCALE       (-1000)
/* PostgreSQL 18 calculation constants (numeric.h): the average/division
   result-scale clamp is distinct from the stored 16383-digit limit. */
#define DEC_MIN_SIG_DIGITS  16
#define DEC_AVG_MAX_RSCALE  1000

#define DEC_PAYLOAD_VERSION 1
#define DEC_PAYLOAD_HEADER  16
#define DEC_PAYLOAD_MAX     (DEC_PAYLOAD_HEADER + (DEC_MAX_COEFF_DIGITS + 1) / 2)
#define DEC_TEXT_MAX        (2 + DEC_MAX_INT_DIGITS + 1 + DEC_MAX_FRAC_DIGITS + 1)

typedef enum {
    DEC_SIGN_ZERO = 0,
    DEC_SIGN_POS  = 1,
    DEC_SIGN_NEG  = 2
} dec_sign;

typedef enum {
    DEC_OK = 0,
    DEC_ERR_SYNTAX,     /* malformed decimal text (incl. embedded NUL) */
    DEC_ERR_EXPONENT,   /* exponent text outside the supported domain */
    DEC_ERR_RANGE,      /* value outside the finite bare-numeric digit limits */
    DEC_ERR_PRECISION,  /* constrained assignment overflows precision after rounding */
    DEC_ERR_DIV_ZERO,   /* average over zero rows */
    DEC_ERR_NOMEM,      /* allocation failure */
    DEC_ERR_PAYLOAD,    /* packed payload structurally invalid */
    DEC_ERR_INVALID,    /* NULL argument, aliasing violation, P/S out of domain */
    DEC_ERR_TRUNCATED,  /* text/payload output buffer too small */
    DEC_ERR_INTERNAL    /* invariant violation; never returned for valid input */
} dec_status;

typedef struct {
    int32_t  weight;
    int32_t  dscale;
    dec_sign sign;
    uint16_t ndigits;
    uint16_t cap;
    uint16_t *digits;
} dec_t;

/* Lifecycle. dec_init leaves a canonical zero with no buffer.
   dec_free accepts an initialized or already-freed value.
   Output contract for every function below that writes a dec_t: `out`
   must be a valid initialized (or previously freed) value and must not
   alias any input — alias rejection leaves every input untouched; on
   every failure return `out` is left as the canonical zero, on success
   it holds the result. This contract covers failure paths inside
   helpers too (a failed limb allocation inside parse/unpack/arithmetic
   resets the output). The low-level primitives make their own narrower
   promises: dec_reserve never releases storage and leaves the value
   unchanged on failure. */
void dec_init(dec_t *d);
void dec_free(dec_t *d);
/* Grow the limb buffer. DEC_ERR_NOMEM on allocation failure,
   DEC_ERR_RANGE past DEC_MAX_LIMBS_ALLOC. On failure the value is
   unchanged; storage is never released here. */
dec_status dec_reserve(dec_t *d, uint16_t limbs);
/* Canonicalize in place: strip leading/trailing zero limbs; an empty
   limb list becomes the canonical zero. dscale is kept untouched. */
void dec_normalize(dec_t *d);

/* Parse the full finite decimal grammar (output contract above):
     [+-]? ( digits+ ( "." digits* )? | "." digits+ ) ( [eE] [+-]? digits+ )?
   One outer ASCII space/tab/CR/LF run is stripped (literal parity with
   the scalar conversion boundary). The whole input must be consumed;
   an embedded NUL is a syntax error. The display scale is the
   fractional digit count after the exponent is applied, never
   negative (1.0e2 renders 100); trailing zeros stay display-only
   (1.2300 keeps display scale 4). Scanning is a single linear pass
   with no exponent-proportional allocation: an exponent run with more
   than 7 significant digits (leading zeros ignored) fails with
   DEC_ERR_EXPONENT. `out` may be a reused dec_t; any prior buffer is
   released. */
dec_status dec_parse(const char *val, size_t vlen, dec_t *out,
                     char *err, size_t errsz);

/* Exact text at display scale: integer digits, then exactly dscale
   fractional digits. dec_text_len is the exact byte count including
   the NUL; a short buffer fails with DEC_ERR_TRUNCATED. */
size_t dec_text_len(const dec_t *d);
dec_status dec_to_text(const dec_t *d, char *buf, size_t bufsz,
                       size_t *out_len);

/* Full mathematical order; display scale is ignored (1.0 == 1.00). */
int dec_cmp(const dec_t *a, const dec_t *b);

/* FNV-1a 64 over the canonical mathematical form only (sign, weight,
   limbs) — equal values hash equally regardless of display scale. */
uint64_t dec_hash(const dec_t *d);

/* Digits left of the decimal point: 0 for zero, else >= 1 (values
   below 1 count the leading "0"). */
int32_t dec_int_digits(const dec_t *d);

/* out = a + b / a - b, exact. Result display scale is the larger
   operand's. DEC_ERR_RANGE when the result leaves the bare digit
   limits; DEC_ERR_NOMEM on allocation failure. out must differ from a
   and b. */
dec_status dec_add(const dec_t *a, const dec_t *b, dec_t *out);
dec_status dec_sub(const dec_t *a, const dec_t *b, dec_t *out);

/* Round the magnitude to `scale` fractional digits, ties away from
   zero (PostgreSQL 18 round_var semantics, including carry into a new
   leading limb). scale < 0 rounds left of the point. |scale| must be
   <= DEC_MAX_DSCALE — DEC_ERR_RANGE otherwise, since the packed
   display-scale field cannot represent a wider scale — and
   DEC_ERR_RANGE when the carry produces more than DEC_MAX_INT_DIGITS
   integer digits (a maximum-sized value rounded up can cross the bare
   limit). Sets dscale to scale (callers clamp negative display scale
   where the contract requires it — see dec_apply_precision; the P/S
   domain validation keeps every constrained assignment inside these
   bounds, and a range failure there is reported as DEC_ERR_PRECISION).
   The result is canonical: rounding to zero yields the canonical zero,
   never a signed or weighted zero, and no trailing zero limbs survive.
   In-place: on any failure (DEC_ERR_NOMEM or DEC_ERR_RANGE) `d` is
   left as the canonical zero. */
dec_status dec_round_scale(dec_t *d, int32_t scale);

/* Constrained assignment (PostgreSQL 18 apply_typmod): round to
   `scale` (ties away), clamp display scale to >= 0, then enforce
   precision: the rounded value's digit count left of the point must be
   <= precision - scale. DEC_ERR_PRECISION names overflow after
   rounding. P must be 1..1000 and S -1000..1000 (DEC_ERR_INVALID
   otherwise). out must differ from in. */
dec_status dec_apply_precision(const dec_t *in, int32_t precision,
                               int32_t scale, dec_t *out,
                               char *err, size_t errsz);

/* sum / count under the frozen PostgreSQL 18 average policy:
   select_div_scale picks the result scale from normalized base-10000
   weights and leading groups (16 significant digits, both operands'
   display scales as floors, final clamp 0..1000 — distinct from the
   stored 16383-digit fractional limit); the quotient is computed to
   that scale with one guard limb and rounded half-away-from-zero on
   the magnitude. A zero sum yields zero at the selected scale.
   count == 0 is DEC_ERR_DIV_ZERO. out must differ from sum. */
dec_status dec_div_avg(const dec_t *sum, uint64_t count, dec_t *out);

/* Packed payload (frozen Phase 0 layout), DEC_PAYLOAD_HEADER bytes of
   header plus ceil(significant digits / 2) packed digit bytes:
     [0]  uint8  version (DEC_PAYLOAD_VERSION)
     [1]  uint8  sign (dec_sign)
     [2]  uint16 BE reserved = 0
     [4]  uint32 BE significant digit count (0 for zero, else 1..147455)
     [8]  int32  BE coefficient exponent m (value = ±digits × 10^m)
     [12] uint32 BE display scale
     [16] ceil(digits/2) bytes, two decimal digits per byte, high
          nibble first; an odd count pads the final low nibble with 0
   Decode validates every field: version, sign/reserved/consistency,
   nibble values, pad nibble, exact length, digit-string canonicity
   (no leading/trailing zero digit), the bare numeric digit limits and
   a display scale that can render the value losslessly. */
size_t dec_payload_len(const dec_t *d);
dec_status dec_pack(const dec_t *d, uint8_t *out, size_t cap,
                    size_t *out_len);
dec_status dec_unpack(const uint8_t *buf, size_t len, dec_t *out);

#endif /* DECNUM_H */
