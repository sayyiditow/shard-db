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
/* Partial updates require OLD; omitted fields stay unchanged. Positional
 * empty/missing cells are unsupplied, JSON empty/null remain explicit values.
 * Unsupported packed if/criteria/index/auto-key semantics fail closed.
 * source() borrows inline input or owns file bytes through synchronous return. */
int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter);
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file);
int typed_bulk_insert_source(const char *root,const char *object,const Schema *sc,
    const TypedSchema *ts,const char *input,int format,char delimiter,int if_not_exists);
#endif
