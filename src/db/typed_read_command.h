#ifndef SHARD_TYPED_READ_COMMAND_H
#define SHARD_TYPED_READ_COMMAND_H
#include "typed_row.h"
/* Caller holds the object read lock, pins matching metadata/registry and has
 * no writer reservation. Owned response chunks are unpublished until success. */
typedef enum { TYPED_READ_FETCH,TYPED_READ_GET_MULTI,TYPED_READ_KEYS,
               TYPED_READ_EXISTS_MULTI,TYPED_READ_NOT_EXISTS } TypedReadKind;
typedef struct {
    TypedReadKind kind;
    const char *format,*fields,*cursor,*keys;
    int offset,limit,want_total;
    char delimiter;
    size_t budget;
    uint32_t timeout_ms;
} TypedReadOptions;
int typed_read_command(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
                        const TypedReadOptions *options,FILE *out);
int typed_read_single(SlotcaskDb *db,const FieldSchema *fs,const void *key,
                       size_t keylen,const char *fields,size_t budget,FILE *out);
int typed_read_route(const char *root,const char *object,const TypedReadOptions *options);
int typed_read_single_route(const char *root,const char *object,const void *key,
    size_t len,const char *fields);
int typed_read_exists(const char *root,const char *object,const void *key,size_t len);
#endif
