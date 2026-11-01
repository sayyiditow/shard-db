#ifndef SHARD_DECIMAL_ORDER_H
#define SHARD_DECIMAL_ORDER_H
#include "decnum.h"
/* Untiered canonical value key; status 0/-1, length reset on failure.
   NULL output is a validating measurement request. */
int decimal_order_into(const dec_t *, uint8_t *, size_t, size_t *);
/* Inverse of decimal_order_into for one untiered payload (tier byte already
   removed). `out` must be an initialized/freed dec_t; on success it holds the
   exact value with the MINIMAL lossless display scale, on failure it is the
   canonical zero and errno is EILSEQ (malformed/non-canonical payload,
   outside the finite digit limits) or ENOMEM. Zero decodes from the single
   byte 01. Never trusts the length: every limb, the weight, the terminator
   and the digit-limit domain are validated. */
int decimal_order_decode(const uint8_t *in, size_t n, dec_t *out);
/* Smallest display scale that renders d losslessly (0 for zero and for
   integers); independent of d->dscale. */
int32_t decimal_min_dscale(const dec_t *d);
#endif
