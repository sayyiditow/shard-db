#ifndef TYPED_BULK_INSERT_H
#define TYPED_BULK_INSERT_H
#include "typed_row.h"
/* Internal, synchronous command adapter. Never binds or publishes schemas.
 * borrows schema/input/optional database through return. Production passes NULL
 * for borrowed; tests pass a private physical object with the same geometry.
 * format=0 JSON dict/array, format=1 positional delimited input.
 * Packed auto-key and secondary indexes fail closed until later integrations.
 */
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists);
#endif
