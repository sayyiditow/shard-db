#include "decimal_order.h"
#include <errno.h>
#include <limits.h>
static int invalid(void) {
  errno = EILSEQ;
  return -1;
}
int decimal_order_into(const dec_t *d, uint8_t *out, size_t cap, size_t *len) {
  if (!len) {
    errno = EINVAL;
    return -1;
  }
  *len = 0;
  if (!d || d->dscale < 0 || d->dscale > DEC_MAX_DSCALE)
    return invalid();
  if (d->ndigits > d->cap || (d->ndigits && !d->digits))
    return invalid();
  if (!d->ndigits) {
    if (d->sign != DEC_SIGN_ZERO || d->weight)
      return invalid();
    if (out && cap < 1) {
      errno = ENOBUFS;
      return -1;
    }
    if (out)
      out[0] = 1;
    *len = 1;
    return 0;
  }
  if ((d->sign != DEC_SIGN_POS && d->sign != DEC_SIGN_NEG) || !d->digits[0] ||
      !d->digits[d->ndigits - 1])
    return invalid();
  for (size_t i = 0; i < d->ndigits; i++)
    if (d->digits[i] >= 10000)
      return invalid();
  /* Validate the same finite domain accepted by dec_unpack, including
     retained display scale. Arithmetic uses 64 bits before multiplying. */
  uint16_t first = d->digits[0], last = d->digits[d->ndigits - 1];
  int fd = first >= 1000 ? 4 : first >= 100 ? 3 : first >= 10 ? 2 : 1, tz = 0;
  while (last % 10 == 0) {
    tz++;
    last /= 10;
  }
  int64_t id = (int64_t)d->weight * 4 + fd;
  int64_t exponent = ((int64_t)d->weight - d->ndigits + 1) * 4 + tz;
  if (id > DEC_MAX_INT_DIGITS || exponent < -DEC_MAX_FRAC_DIGITS ||
      (exponent < 0 && d->dscale < -exponent))
    return invalid();
  size_t need = 7 + 2 * (size_t)d->ndigits;
  if (out && cap < need) {
    errno = ENOBUFS;
    return -1;
  }
  if (out) {
    out[0] = d->sign == DEC_SIGN_NEG ? 0 : 2;
    uint32_t w = (uint32_t)d->weight ^ UINT32_C(0x80000000);
    out[1] = (uint8_t)(w >> 24);
    out[2] = (uint8_t)(w >> 16);
    out[3] = (uint8_t)(w >> 8);
    out[4] = (uint8_t)w;
    for (size_t i = 0; i < d->ndigits; i++) {
      uint16_t limb = (uint16_t)(d->digits[i] + 1);
      out[5 + 2 * i] = (uint8_t)(limb >> 8);
      out[6 + 2 * i] = (uint8_t)limb;
    }
    out[need - 2] = 0;
    out[need - 1] = 0;
    if (d->sign == DEC_SIGN_NEG)
      for (size_t i = 1; i < need; i++)
        out[i] ^= 255;
  }
  *len = need;
  return 0;
}

int32_t decimal_min_dscale(const dec_t *d) {
  if (!d || !d->ndigits || !d->digits)
    return 0;
  uint16_t last = d->digits[d->ndigits - 1];
  int tz = 0;
  while (last && last % 10 == 0) {
    tz++;
    last /= 10;
  }
  int64_t exponent = ((int64_t)d->weight - d->ndigits + 1) * 4 + tz;
  return exponent < 0 ? (int32_t)-exponent : 0;
}

int decimal_order_decode(const uint8_t *in, size_t n, dec_t *out) {
  if (!out) {
    errno = EINVAL;
    return -1;
  }
  dec_free(out);
  if (!in || n == 0)
    return invalid();
  if (n == 1) {
    if (in[0] != 1)
      return invalid();
    return 0;
  }
  if (n < 7 || ((n - 7) & 1))
    return invalid();
  size_t nd = (n - 7) / 2;
  if (nd == 0 || nd > DEC_MAX_LIMBS || (in[0] != 0 && in[0] != 2))
    return invalid();
  int neg = in[0] == 0;
  uint8_t x = neg ? 255 : 0; /* undo the negative-key byte inversion */
  uint32_t w = ((uint32_t)(in[1] ^ x) << 24) | ((uint32_t)(in[2] ^ x) << 16) |
               ((uint32_t)(in[3] ^ x) << 8) | (uint32_t)(in[4] ^ x);
  int32_t weight = (int32_t)(w ^ UINT32_C(0x80000000));
  if ((uint8_t)(in[n - 2] ^ x) != 0 || (uint8_t)(in[n - 1] ^ x) != 0)
    return invalid();
  if (dec_reserve(out, (uint16_t)nd) != DEC_OK) {
    errno = ENOMEM;
    return -1;
  }
  for (size_t i = 0; i < nd; i++) {
    uint16_t raw = (uint16_t)(((uint16_t)(in[5 + 2 * i] ^ x) << 8) |
                              (uint16_t)(in[6 + 2 * i] ^ x));
    if (raw < 1 || raw > 10000) {
      dec_free(out);
      return invalid();
    }
    out->digits[i] = (uint16_t)(raw - 1);
  }
  if (!out->digits[0] || !out->digits[nd - 1]) {
    dec_free(out);
    return invalid();
  }
  out->ndigits = (uint16_t)nd;
  out->weight = weight;
  out->sign = neg ? DEC_SIGN_NEG : DEC_SIGN_POS;
  /* same finite domain as the encoder / dec_unpack */
  uint16_t first = out->digits[0];
  int fd = first >= 1000 ? 4 : first >= 100 ? 3 : first >= 10 ? 2 : 1;
  int32_t md = decimal_min_dscale(out);
  if ((int64_t)weight * 4 + fd > DEC_MAX_INT_DIGITS ||
      md > DEC_MAX_FRAC_DIGITS) {
    dec_free(out);
    return invalid();
  }
  out->dscale = md;
  return 0;
}
