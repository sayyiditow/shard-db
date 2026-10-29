#ifndef SHARD_DECIMAL_ORDER_H
#define SHARD_DECIMAL_ORDER_H
#include "decnum.h"
/* Untiered canonical value key; status 0/-1, length reset on failure.
   NULL output is a validating measurement request. */
int decimal_order_into(const dec_t *, uint8_t *, size_t, size_t *);
#endif
