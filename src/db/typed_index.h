#ifndef SHARD_TYPED_INDEX_H
#define SHARD_TYPED_INDEX_H
#include "decimal_order.h"
#include "typed_row.h"
/* Field and composite keys are tiered; text and payload keys are untiered.
   key/payload functions return 1 present, 0 absent, -1 error. All output
   lengths/pointers reset on absent/error. into(out=NULL) measures exactly.
   No helper retains a borrowed row span. */
int typed_index_fixed_width(const TypedSchema *, size_t, size_t *);
int typed_index_field_key_into(const TypedSchema *, size_t, const uint8_t *,
                               size_t, uint8_t, uint8_t *, size_t, size_t *);
int typed_index_field_key_alloc(const TypedSchema *, size_t, const uint8_t *,
                                size_t, uint8_t, size_t, uint8_t **, size_t *);
int typed_index_text_key_into(const TypedSchema *, size_t, const char *, size_t,
                              uint8_t *, size_t, size_t *);
int typed_index_record_key_into(const TypedSchema *, const uint8_t *, size_t,
                                uint8_t, const char *, uint8_t *, size_t,
                                size_t *);
int typed_index_record_payload_into(const TypedSchema *, const uint8_t *,
                                    size_t, uint8_t, const char *, uint8_t *,
                                    size_t, size_t *);
/* payload=0: NEW ordered admission, actual key <=544. payload=1: raw
   bitmap/trigram, no Btree cap. budget limits actual allocated bytes. */
int typed_index_record_alloc(const TypedSchema *, const uint8_t *, size_t,
                             uint8_t, const char *, int, size_t, uint8_t **,
                             size_t *);
int typed_index_validate_new(const TypedSchema *, const uint8_t *, size_t,
                             uint8_t, const char[][256], const enum IndexType *,
                             int);
int typed_index_old_record_key_into(const TypedSchema *, const uint8_t *,
                                    size_t, uint8_t, const char *, uint8_t *,
                                    size_t, size_t *);
int typed_index_old_record_key(const TypedSchema *, const uint8_t *, size_t,
                               uint8_t, const char *, uint8_t **, size_t *);
int typed_index_text_component_key_into(const TypedSchema *, size_t,
                                        const char *, size_t, int, uint8_t *,
                                        size_t, size_t *);
#endif
