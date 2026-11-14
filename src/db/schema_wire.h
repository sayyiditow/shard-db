#ifndef SHARD_SCHEMA_WIRE_H
#define SHARD_SCHEMA_WIRE_H
#include "types.h"
/* Caller pins the object schema under its object lock. The checked original
 * declaration is the export identity, not physical descriptor width. */
int schema_wire_quote(FILE *out,const char *text,size_t len);
char *typed_schema_fields_json(const char *root,const char *object,
    const TypedSchema *ts,size_t budget);
int typed_legacy_decl_size(char *clean_type);
int typed_field_specs_parse(const char *json,char *specs[MAX_FIELDS],size_t *count);
int typed_field_lines_parse(const char *json,char (**lines)[MAX_FIELD_DEF],int *nlines);
char *typed_schema_indexes_json(const char *root,const char *object,size_t budget);
int typed_default_validate(const TypedField *field);
#endif
