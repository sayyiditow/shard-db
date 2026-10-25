#ifndef TYPED_ROW_H
#define TYPED_ROW_H
#include "types.h"
#include "slotcask.h"

/* Internal prepared schema/record adapters; never publish an object layout.
 * No loader/create-object calls the binder in this child. Bound packed schemas
 * may only reach these adapters/packed_record APIs until consumers are converted;
 * old typed_* and raw-offset APIs cannot interpret packed fields.
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

/* Existing scalar semantics, shared internally by the adapter. */
int encode_json_field_checked(const TypedField *f, const char *text, size_t len,
                                int quoted, uint8_t *out, char *err, size_t errsz);
int sentinel_span_parses(enum FieldType type, const char *value, size_t len);
const char *generate_default(const TypedField *f, char *buf, size_t cap,
                              const char *root, const char *object);
int decode_field_to_buf(const TypedField *f, const uint8_t *data,
                         char *buf, int cap, int render_zeros);
#endif
