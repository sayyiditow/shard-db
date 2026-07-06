/* src/db/type_desc.c */
#include "type_desc.h"

/* Designated initializers: each row is pinned to its enum value, so table
   order can never drift from the enum. A new FieldType without a row here
   fails the _Static_assert below (FT_COUNT grows, array length doesn't). */
static const TypeDescriptor g_type_desc[FT_COUNT] = {
    [FT_NONE]       = { "none",       0 },  /* row unused: type_desc(FT_NONE) returns NULL */
    [FT_VARCHAR]    = { "varchar",    0 },
    [FT_LONG]       = { "long",       8 },
    [FT_INT]        = { "int",        4 },
    [FT_SHORT]      = { "short",      2 },
    [FT_DOUBLE]     = { "double",     0 },
    [FT_FLOAT]      = { "float",      0 },
    [FT_BOOL]       = { "bool",       0 },
    [FT_BYTE]       = { "byte",       1 },
    [FT_NUMERIC]    = { "numeric",    8 },
    [FT_DATE]       = { "date",       4 },
    [FT_DATETIME]   = { "datetime",   0 },
    [FT_DATETIMEMS] = { "datetimems", 0 },
    [FT_TIME]       = { "time",       0 },
    [FT_TIMESTAMP]  = { "timestamp",  0 },  /* excluded from int fast path — see type_desc.h */
    [FT_UUID]       = { "uuid",       0 },
    [FT_ENUM]       = { "enum",       0 },
    [FT_IPV4]       = { "ipv4",       0 },
    [FT_IPV6]       = { "ipv6",       0 },
};

_Static_assert(sizeof(g_type_desc) / sizeof(g_type_desc[0]) == FT_COUNT,
               "g_type_desc must have one row per FieldType");

const TypeDescriptor *type_desc(enum FieldType t) {
    if (t <= FT_NONE || t >= FT_COUNT) return NULL;
    return &g_type_desc[t];
}
