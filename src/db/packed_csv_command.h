#ifndef PACKED_CSV_COMMAND_H
#define PACKED_CSV_COMMAND_H
#include "typed_row.h"
typedef struct {
    const char *fields;
    char delimiter;
    size_t buffer_budget; /* owned response lines + NULs + list nodes */
    uint32_t timeout_ms;
} PackedCsvOptions;
/* Private packed-only command backends. Caller holds shared object lock and
   keeps the matching registry instance, schema, options and inputs alive.
   No nested write reservation. These functions reserve every writer gate
   during collection, release before FILE I/O, then emit a complete prepared
   response. Construction/read failures emit one JSON error before any CSV.
   I/O errors return -1 without appending a replacement error to a CSV prefix.
   Budgets exclude borrowed input, cache maps and temporary renderer/decoder
   workspaces; this is not a total peak-memory cap. No public activation. */
int packed_csv_fetch(SlotcaskDb *db,const FieldSchema *fs,int offset,int limit,
    const char *cursor,int want_total,const PackedCsvOptions *options,FILE *out);
int packed_csv_get_multi(SlotcaskDb *db,const FieldSchema *fs,const Schema *schema,
    const char *keys_json,const PackedCsvOptions *options,FILE *out);
#endif
