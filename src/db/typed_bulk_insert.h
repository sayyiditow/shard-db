#ifndef TYPED_BULK_INSERT_H
#define TYPED_BULK_INSERT_H
#include "typed_row.h"
#include <stdatomic.h>
/* Compile-time availability for internal bridge-specific regression fixtures. */
#define TYPED_BULK_INDEX_BRIDGE_API 1
typedef struct {size_t limit;_Atomic size_t used;} TypedBulkMemory;
/* Internal, synchronous command adapter. Never binds or publishes schemas.
 * borrows schema/input/optional database through return. Production passes NULL
 * for borrowed; tests pass a private physical object with the same geometry.
 * format=0 JSON dict/array, format=1 positional delimited input.
 * Indexed mutations share the legacy window batching and durability hooks.
 */
int typed_bulk_insert_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter,int if_not_exists);
/* Partial updates require OLD; omitted fields stay unchanged. Positional
 * empty/missing cells are unsupplied, JSON empty/null remain explicit values.
 * Unsupported packed if/criteria semantics fail closed.
 * source() borrows inline input or owns file bytes through synchronous return. */
int typed_bulk_update_command(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *data,size_t len,int format,char delimiter);
int typed_bulk_update_source(const char *root,const char *object,
    const Schema *sc,const TypedSchema *ts,SlotcaskDb *borrowed,
    const char *input,int is_file);
int typed_bulk_insert_source(const char *root,const char *object,const Schema *sc,
    const TypedSchema *ts,const char *input,int format,char delimiter,int if_not_exists);
size_t typed_bulk_index_bridge_bytes(size_t count);
void *typed_bulk_index_bridge_create(const char*,const char*,const Schema*,
    const TypedSchema*,const char fields[][256],const enum IndexType*,int,
    SlotcaskBulkShardInput*,void *memory);
void typed_bulk_index_bridge_free(void*);
int typed_bulk_index_memory_take(void*,size_t);
void typed_bulk_index_memory_release(void*,size_t);
#endif
