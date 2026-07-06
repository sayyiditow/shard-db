/* src/db/type_desc.h — per-field-type descriptor table.
 *
 * Single source of truth for type facts that used to be duplicated across
 * parallel switches. Columns exist only when at least one call site consumes
 * them — add a column in the change that adds its consumer, never
 * speculatively (see docs/plans/2026-07-05-06-typedescriptor-table.md,
 * Definition of done). Adding a new FieldType without a table row fails the
 * build via the _Static_assert in type_desc.c. */
#ifndef TYPE_DESC_H
#define TYPE_DESC_H
#include "types.h"

typedef struct TypeDescriptor {
    const char *name;          /* "varchar", "int", ... (errors/describe-object) */
    int   int_width;           /* >0 => integer-class width for the agg int-hash
                                  fast path; else 0. Membership mirrors the old
                                  typed_field_int_width switch exactly:
                                  FT_TIMESTAMP is 8 bytes on-disk but EXCLUDED
                                  (kept at 0) — including it would silently
                                  change agg fast-path membership. */
} TypeDescriptor;

const TypeDescriptor *type_desc(enum FieldType t);  /* NULL if unknown */
#endif
