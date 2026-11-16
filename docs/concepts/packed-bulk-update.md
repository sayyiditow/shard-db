# Packed keyed bulk updates (inactive schema format)

The internal `typed_bulk_update_command` composes partial replacements for
explicitly bound packed schemas through the same synchronous deferred bulk
coordinator as insertion. `typed_bulk_update_source` owns bounded file bytes or
borrows inline JSON through return. Public loaders/creates still do not bind
packed schemas. This document records private integration, not activation.

JSON dictionaries and arrays patch only supplied known fields. Omitted values
and omitted AUTO_CREATE fields carry from locked OLD; JSON empty varchar and
explicit NULL remain real values. Current scalar-update policy still permits
an explicitly supplied AUTO_CREATE patch; this slice does not silently make
that field immutable. AUTO_UPDATE is stamped after the patch, overriding client
input. Defaults, sequences and insert-time generation are not rerun.

Delimited updates differ from insertion: every blank or absent cell is
unsupplied, including numeric/varchar cells, so OLD remains unchanged. There
is no positional spelling for explicit NULL or empty varchar; use JSON for
those. Active nonremoved fields consume columns in schema order; surplus
columns are ignored. No-separator lines are skipped, including EOF/LF/CRLF;
a separator followed by an empty cell is a real no-field patch and still
stamps AUTO_UPDATE on an existing target. Input is length-bounded and may carry
embedded NUL in varchar content; this adds no CSV quoting/escape grammar.

Valid key/object row shapes count toward `matched`; malformed/missing/overlong
key or missing/non-object value rows count `skipped` without being matched.
Missing stored targets count `skipped` and never insert. Construction failures
also count skipped but produce an optional per-key `errors` array and a nonzero
command return, distinguishing them from condition misses. Existing successful
responses keep `{matched,updated,skipped}`. Encoding/NOT NULL/size/generation/OOM
failure is row-atomic; valid peers may commit. This is not a batch transaction.
A hard shard failure does not erase successful later-window outcomes: update
counts use terminal per-row statuses. Any hard coordinator failure also emits
a top-level partial-outcome warning and returns nonzero, even when keyed row
errors are already present. Pending durability overrides all counts with an
explicit do-not-retry error.

All update forms screen storage-key duplicates before any writes, including
escaped dictionary aliases and repeated delimited keys. JSON keys are decoded
before screening, even when the referenced row has an invalid patch or key
length. Repeated partial patches reject the entire request: composing each
against the same OLD could silently discard a previous patch. Insert duplicate
policy is unchanged.

Packed array `if` conditions reject the entire request before writes, including
an explicitly present empty or null condition. This is a temporary fail-closed
boundary, not implementation of legacy per-record CAS. Packed criteria updates,
including criteria dry runs, reject before any criteria compilation/scan.
Packed auto-key and secondary-index objects also reject before construction.
Existing public legacy CAS, criteria, index and dispatch policies stay active
and unchanged; their complete packed support remains later query/write work.

The adapter owns each NEW allocation until the complete synchronous request
returns, with one input per touched shard and exactly one coordinator call.
`require_existing=1` filters missing keys before the constructor;
`value_rewrites_payload=1` prevents pre-staging placeholder bytes. The callback
validates OLD generation, uses the actual-size row helpers, and checks actual
key/value bytes against slot/schema/unchanged segment limits. It never supplies
caller-OLD overrides, changes index/marker/recovery hooks or uses the single
owning callback. Recovery continues to use durable segment evidence.

Row error JSON is emitted from length-delimited keys without allocating an
escape buffer after the write. All row/input/key/NEW allocations are released
on success, hard failure or pending durability; pending results remain explicit
errors and must not be retried blindly. Raw input and schema stay immutable
and alive through every joined pipeline. File short reads fail before dispatch.

Allocation follows actual source/payload sizes; no schema-maximum arena or slab
is introduced. A maximum 256-varchar schema remains usable for small rows.
Bulk command-wide expansion budgeting remains future activation work; this
slice does not claim to impose QUERY_BUFFER_MB on previously uncapped bulk paths.

Three legacy update request blocks now guard their input-copy loops after a
failed calloc, instead of dereferencing NULL before the existing error branch.
The legacy delimiter callback also checks its heap cell copy before memcpy.
These narrow crash fixes retain existing legacy skip/response policy; broader
legacy hard-error propagation and parser allocation auditing remain Phase 4
obligations. Packed constructors already carry per-row hard-error state.

Private physical tests cover exact decimals, NULL/empty/omission, independent
shards, file input, missing targets, forbidden conditions, duplicate request
rejection, OLD/count/segment preservation, 4 MiB growth/shrinkage, a >1 GiB
schema's small actual row, reopen and concurrent disjoint patches. Public packed
route acceptance, criteria/CAS, indexes, bulk-delete, remaining readers and
migration are not discharged by those fixtures. No metadata activation occurs.
