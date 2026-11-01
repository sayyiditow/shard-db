#ifndef TYPED_ROW_H
#define TYPED_ROW_H
#include "types.h"
#include "slotcask.h"

/* Internal prepared schema/record adapters; never publish an object layout.
 * No loader/create-object calls the binder in this child. Bound packed schemas
 * may only reach these adapters/packed_record APIs until consumers are converted;
 * typed_decode/typed_decode_stream support whole packed records; legacy
 * field-string and raw-offset APIs still cannot interpret packed fields.
 * Schemas, enum strings and OLD bytes remain immutable while views/prepared
 * cells borrow them. Initialize prepared objects before use; free is idempotent.
 * Outputs and inputs must not alias. Owned output arguments must be valid;
 * callers release prior owned outputs before reuse. A successful *_alloc or
 * field/decode result belongs to the caller and is released with free(). Valid
 * owned output arguments are reset to NULL/0 on failure; field NULL is indicated
 * by a NULL return with errno=0. Other failures set errno.
 * set() is failure-atomic (cell, owner, seen, byte count). defaults()/stamp()
 * are incremental: discard/free the prepared object on failure; generators are
 * never rolled back. zero()/old() preserve prior state on validation failure,
 * then replace it; later errors leave an empty free-safe object. Allocation,
 * measurement and composition never rerun defaults/sequence/random generation.
 * Actual lengths do not prove row/key fit. The write integration must call
 * pkr_record_fits with the actual key and unchanged segment cap before mutation.
 * Legacy spans keep bytes actually present; read() substitutes whole-field zero
 * for incomplete legacy fields, copy_legacy() carries partial bytes then pads.
 * The packed renderer's budget counts JSON output bytes including the NUL;
 * decimal validation/render workspaces remain independently digit-bounded.
 * JSON collection validates the complete document (including ignored fields),
 * accepts at most 64 nested containers, decodes names and byte strings, rejects
 * object/array values for known scalar fields, validates each duplicate, and
 * retains its last valid occurrence. Unknown fields are ignored after validation.
 */
typedef struct {
    const TypedSchema *schema;
    const uint8_t *value, *base, *bitmap;
    size_t len, region;
    int bitmap_bytes;
    uint8_t hdr2;
    PkrView packed;
} TypedRowView;
typedef struct {
    const uint8_t *data;
    size_t len;
    int is_null, complete;
} TypedRowSpan;
typedef struct {
    const char *data;
    size_t len;
    uint8_t supplied, is_null, quoted;
} TypedInput;
typedef struct {
    const TypedSchema *schema;
    PkrCell cells[MAX_FIELDS];
    uint8_t *owned[MAX_FIELDS];
    uint8_t seen[MAX_FIELDS];
    size_t bytes;
} TypedPrepared;

/* The schema's fields/names/defaults are already parsed. Only TYPE tokens
   are accepted. Success binds an in-memory codec; failure preserves schema.
   Legacy cache loaders do not call this function in Phase 3B. */
int typed_schema_bind_packed(TypedSchema *ts, const char *const *types,
                              size_t count, char *err, size_t errsz);
uint8_t typed_schema_hdr2(const TypedSchema *ts);
int typed_row_open(const TypedSchema *ts, const uint8_t *value, size_t len,
                    uint8_t hdr2, TypedRowView *out);
int typed_row_span(const TypedRowView *view, size_t index, TypedRowSpan *out);
/* Explicit legacy materialization contracts: read requires a complete field;
   carry copies available bytes into an exact-width zeroed destination. */
int typed_span_read(const TypedSchema *ts, size_t index,
                      const TypedRowSpan *span, const uint8_t **out);
int typed_span_copy_legacy(const TypedSchema *ts, size_t index,
                             const TypedRowSpan *span, uint8_t *out, size_t cap);

void typed_prepared_init(TypedPrepared *p);
void typed_prepared_free(TypedPrepared *p);
int typed_prepared_zero(const TypedSchema *ts, TypedPrepared *p,
                         char *err, size_t errsz);
int typed_prepared_old(const TypedSchema *ts, const SlotcaskOldRecord *old,
                        TypedPrepared *p, char *err, size_t errsz);
int typed_prepared_set(TypedPrepared *p, size_t index, const TypedInput *input,
                        char *err, size_t errsz);
int typed_prepared_defaults(TypedPrepared *p, const char *root,
                             const char *object, char *err, size_t errsz);
int typed_prepared_stamp(TypedPrepared *p, enum DefaultKind kind,
                          char *err, size_t errsz);
int typed_prepared_compose(const TypedPrepared *p, uint8_t *out, size_t cap,
                            size_t *used, char *err, size_t errsz);
int typed_prepared_alloc(const TypedPrepared *p, uint8_t **out, size_t *len,
                          char *err, size_t errsz);
int typed_encode_alloc(const TypedSchema *ts, const char *json, int defaults,
                         const char *root, const char *object,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
/* Internal single-write collectors; output is NULL/0 on failure, owned
   malloc storage on success. No schema activation. */
/* Validates one complete JSON value, including unknown nested fields. */
int typed_json_validate(const char *json);
/* Positional full replacement: no JSON defaults; preserve/stamp AUTO_CREATE
   under the same locked OLD contract as typed_insert_alloc. inputs has nfields
   entries. Unprovided cells are present zero/empty (removed cells stay NULL). */
int typed_insert_inputs_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const TypedInput *inputs,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_insert_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,
    const char *root,const char *object,uint8_t **out,size_t *len,
    char *err,size_t cap);
/* Single-update computed operations read immutable locked OLD. Bulk retains
 * its literal-only contract through typed_update_json_alloc. */
int typed_update_computed_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_update_json_alloc(const TypedSchema *ts,const SlotcaskOldRecord *old,
    const char *json,uint8_t **out,size_t *len,char *err,size_t cap);
int typed_encode_packed_into(const TypedSchema *ts, const char *json, int defaults,
                              const char *root, const char *object,
                              uint8_t *out, size_t cap, size_t *used,
                              char *err, size_t errsz);
int typed_update_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                         const TypedInput *inputs, int stamp,
                         uint8_t **out, size_t *len, char *err, size_t errsz);
int typed_transfer_ac_alloc(const TypedSchema *ts, const SlotcaskOldRecord *old,
                             const uint8_t *value, size_t vlen,
                             uint8_t **out, size_t *len,
                             char *err, size_t errsz);
char *typed_packed_decode(const TypedSchema *ts, const uint8_t *value, size_t len,
                           uint8_t hdr2, size_t budget);
char *typed_packed_field_text(const TypedSchema *ts, const uint8_t *value,
                               size_t len, uint8_t hdr2, size_t index,
                               size_t *text_len);
/* Complete JSON cells for projection/rows, preserving their bool-as-string
   policy. NULL/removed produces owned "null"; any error is NULL with errno.
   Budget includes NUL. Names are not parsed by the indexed cell API.
   Projection CSV is bounded by MAX_LINE-1/MAX_FIELDS, skips empty tokens,
   preserves order/duplicates and whitespace, maps unknown/removed to null.
   Packed composites quote the exact concatenated display bytes once. */
char *typed_packed_projected_cell(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, size_t index, size_t budget);
char *typed_packed_project_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *fields_csv, size_t budget);
/* Named packed display text accepts '+' components, skips unknown/NULL parts
   and returns exact bytes plus NUL. text_len excludes NUL; it resets on error.
   NULL/removed/unknown and an empty composite yield NULL with errno=0.
   Scalar empty varchar yields an owned empty string. Budget includes NUL.
   Names/components preserve whitespace; MAX_LINE-1/MAX_FIELDS are enforced.
   The JSON counterpart keeps scalar display policy and quotes a composite
   once, escaping its entire byte span. Errors are NULL with nonzero errno. */
char *typed_packed_named_text(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *name, size_t budget, size_t *text_len);
char *typed_packed_named_json(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const char *name, size_t budget);

/* Checked CSV output, private until command/error integration is complete.
   Byte layer: backslash doubles; C0 controls and DEL use \\u00xx.
   Other bytes (including UTF-8) pass through. Parse CSV before byte unescaping.
   No raw NUL/CR/LF enters a cell.
   Delimiter is printable ASCII except quote/backslash, or tab. Cell output
   has no newline; header/row include their final LF. Budget includes NUL;
   text_len excludes NUL and resets on error. All returned buffers are owned.
   NULL data is valid only at length zero. Missing/NULL fields are empty cells.
   Row key is caller-formatted display bytes, not a binary storage key.
   fields NULL/empty selects all nonremoved fields; otherwise comma parsing
   preserves order, duplicates and whitespace, skips empty tokens and supports
   '+' named composites, bounded by MAX_LINE-1/MAX_FIELDS. */
char *typed_csv_cell_text(const void *data, size_t len, char delimiter,
    size_t budget, size_t *text_len);
char *typed_packed_csv_header(const TypedSchema *ts, const char *fields,
    char delimiter, size_t budget, size_t *text_len);
char *typed_packed_csv_row(const TypedSchema *ts, const uint8_t *value,
    size_t len, uint8_t hdr2, const void *wire_key, size_t key_len,
    const char *fields, char delimiter, size_t budget, size_t *text_len);

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
/* Remaining heap budget shared by renderer output + transient decimal/join
 * workspace. Zero is failure with errno; no allocation or output is performed. */
size_t typed_packed_render_allowance(const TypedSchema *ts,const uint8_t *value,
    size_t len,uint8_t flags,const char *fields,size_t available);
/* Complete rows-format array with explicit key bytes and one validated view.
 * Use render_allowance first for a command-wide output/workspace budget. */
char *typed_packed_row_json(const TypedSchema *ts,const uint8_t *value,size_t len,
    uint8_t flags,const void *key,size_t key_len,const char *fields,size_t budget);
#endif
