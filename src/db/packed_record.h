/* Inactive packed-record codec. No JSON, schema I/O, global state or activation. */
#ifndef PACKED_RECORD_H
#define PACKED_RECORD_H
#include <stddef.h>
#include <stdint.h>
#include "decnum.h"
#define PKR_MAX_FIELDS 256
#define PKR_VARCHAR_MAX UINT32_C(4194304)
#define PKR_DESCRIPTOR_BYTES 8
#define PKR_FLAGS 0x03

typedef enum { PKR_FIXED = 1, PKR_VARCHAR = 2, PKR_NUMERIC = 3 } PkrKind;
typedef struct {
    PkrKind kind;
    uint32_t width, payload_max, offset;
    int32_t precision, scale;
    uint8_t bare, currency;
} PkrField;
typedef struct {
    PkrField fields[PKR_MAX_FIELDS];
    uint32_t nfields, prefix, directory, maximum;
} PkrSchema;
typedef enum { PKR_INVALID = 0, PKR_NULL = 1, PKR_PRESENT = 2 } PkrState;
typedef struct { const uint8_t *data; size_t len; uint8_t is_null; } PkrCell;
typedef struct {
    const PkrSchema *schema;
    const uint8_t *value;
    size_t len;
} PkrView;
typedef struct { const uint8_t *data; size_t len; PkrState state; } PkrSpan;

/* All outputs must not overlap inputs or one another. Fields and schemas own no allocations.
 * Constructors clear their output on failure. Type parser accepts TYPE ONLY:
 * varchar[:N], numeric[:P[,S]], currency. Other types use fixed(width),
 * where width is the already-validated existing binary width (1..16).
 * P/S, bare/bounded mode and currency identity stay explicit metadata. */
dec_status pkr_field_parse(const char *type, PkrField *out);
dec_status pkr_field_fixed(uint32_t width, PkrField *out);
dec_status pkr_schema_build(const PkrField *fields, size_t n, PkrSchema *out);
/* Prepare exact numeric payloads. Never aliases dec_apply_precision inputs.
 * Empty text is a syntax error. On failure *used=0 and buffer is unchanged. */
dec_status pkr_numeric_encode(const PkrField *field, const char *text, size_t n,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
/* Positional cells are in declaration order, not JSON order. Fixed cells
 * contain already-checked binary bytes; variable cells contain raw varchar
 * bytes or exact numeric payloads. NULL requires data=NULL,len=0. Removed
 * fields retain a slot; later adapters supply NULL when recomposing them.
 * Schema/value and view buffers remain immutable while borrowed spans live.
 * measure allocates no record-sized memory. Numeric validation uses decnum.
 * compose validates and measures before writes; cap is ACTUAL capacity.
 * Failure leaves *used=0 and output bytes untouched. Output must not overlap
 * any cell payload; overlap is detected and rejected. */
dec_status pkr_measure(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       size_t *used);
dec_status pkr_compose(const PkrSchema *schema, const PkrCell *cells, size_t n,
                       uint8_t *out, size_t cap, size_t *used);
/* Strict packed-generation view: flags must equal PKR_FLAGS, bitmap width
 * must match schema, offsets are contiguous/in order, no trailing payload,
 * NULL entries are zero, and all numeric payloads satisfy their declaration.
 * A view is cleared on failure; spans are borrowed, including non-NULL data
 * for present-empty varchar. pkr_span requires an unchanged validated view. */
dec_status pkr_view(const PkrSchema *schema, const uint8_t *value, size_t n,
                    uint8_t flags, PkrView *out);
dec_status pkr_span(const PkrView *view, size_t index, PkrSpan *out);
/* Maximum slot bound and actual-row segment gate. Both include existing
 * 24-byte header, actual key bytes, align-8 and floor 32. No cap is changed.
 * Requests/queries remain separately bounded by their existing callers. */
dec_status pkr_slot_bound(const PkrSchema *schema, size_t max_key, size_t *out);
dec_status pkr_record_fits(const PkrSchema *schema, size_t actual, size_t key,
                           size_t segment_max, size_t *record_bytes);
#endif
