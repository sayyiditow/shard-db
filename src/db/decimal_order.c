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
