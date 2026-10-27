# Inactive typed-row adapter

The internal `typed_row` module connects an explicitly bound in-memory schema
with the packed-record codec. It preserves exact decimal text and arbitrary
varchar bytes, including embedded NUL, through length-aware APIs. Existing
loaders, objects, commands and SDK entry points still use the legacy format.
This preparation does not activate packed storage or migrate any object.

`typed_schema_bind_packed` accepts already parsed names/defaults and type-only
tokens. It binds a by-value `PkrSchema` after validating the entire declaration;
failure preserves the schema. An all-zero packed member means legacy. Bound
packed schemas may reach the new adapters, codec and the integrated
[whole-record JSON readers](whole-record-readers.md) and checked
[JSON projection cells/projected GET](packed-json-projection.md). Field-string,
Legacy composite/CSV, query-envelope and raw-offset consumers still require
conversion. [Packed named/composite JSON](packed-named-projection.md) has its
own checked length-aware output path. Passing a bound schema to a legacy
raw-offset API is unsafe. The ordinary loader
does not call this binder.

`typed_row_open` rejects schema/record generation mismatches before returning
borrowed field spans. Packed varchar spans contain content, not a uint16 prefix;
packed numeric spans contain decnum payloads, not scaled int64. Legacy spans
retain actual stored length. Legacy reading substitutes whole-field zero for
incomplete fields; legacy update carry copies available bytes then zero pads.
These two historical behaviors remain separate operations.

A `TypedPrepared` owns changed field bytes and borrows unchanged OLD bytes.
Schema, enum strings and borrowed records must remain immutable and alive until
composition completes. Initialize before use; free is idempotent. A failed
field assignment preserves its previous cell, owner, seen marker and byte count.
Defaults and timestamp passes are incremental; discard the prepared value on
failure. Sequence/random defaults are generated once and retained. Measuring,
allocating or composing a prepared row never regenerates them.

The allocating APIs return actual-length malloc/free-compatible buffers. The
nonallocating composer receives actual destination capacity and leaves its bytes
untouched on failure. Neither uses a schema-maximum row allocation for packed
schemas. Legacy adapters retain the existing fixed-row allocation. Writers must
still check `pkr_record_fits` with the actual key before mutation; value preparation
alone cannot know the final physical fit.

Packed JSON collection validates the complete document, including ignored values, with
a 64-container nesting bound. Field names are JSON-decoded before matching.
Unknown fields are ignored after validation. Every supplied occurrence of a
known field must validate; the last valid duplicate wins. Known field values
must be scalar. Quoted varchar is decoded as a byte span; numeric and fixed
string scalars retain their existing checked conversion policies. This is not a
calendar, boolean, UUID or enum semantics overhaul.

`typed_packed_field_text` returns a length alongside owned bytes; strlen cannot
represent a binary varchar. NULL/removed fields return NULL with errno zero.
`typed_packed_decode` validates one record view, measures exact escaped JSON,
checks its output-byte budget (including NUL), then allocates one output buffer.
A tiny budget does not cause a large varchar copy or escape allocation. Decimal
validation/render workspaces are separately bounded by the decimal limits.

The schema extension costs 7184 bytes per TypedSchema on the verified x86_64
build (about 1.75 MiB across 256 cache slots). Prepared objects cost 8464 stack
bytes plus actual changed-field allocations. These are measured layout costs,
not throughput claims. No benchmark has been run.

This child does not convert production write/read consumers. Subsequent Phase
3B children must integrate actual-size callback ownership, single/bulk writes,
projections, scans, indexes, joins, maintenance and rebuilds, with explicit
errors for private unsupported numeric semantics until Phases 5/6. Public
activation, authoritative metadata and durable publication remain later work.

New adapter boundaries reject NULL JSON before legacy delegation and establish
failure errno even when existing scalar helpers return status/text only. Invalid
scalar/span inputs report EINVAL, malformed OLD reports EILSEQ, and allocation
failures report ENOMEM. Enum type binding rejects trailing declaration text and
preserves schema bytes and ENOMEM on temporary enum-parser allocation failure.

The [owning write hook](owning-write-hooks.md) supplies the actual-size NEW
buffer lifetime used by single-command integration. Bulk insertion uses its
separate caller-owned deferred-request contract; it does not pass bulk buffers
to the single-record owning hook. Neither path activates packed metadata.

[Single-command integration](single-write-integration.md) describes the first
production consumers of these helpers, locked insertion/default handling and
actual-size owning updates. [Bulk insert integration](packed-bulk-insert.md)
adds JSON and positional insert/upsert preparation under explicit schemas.
[Keyed bulk updates](packed-bulk-update.md) use the existing actual-size partial
constructors and preserve omitted OLD fields. Criteria/CAS updates, bulk-delete
and remaining read/index consumers stay future work; packed schemas remain
inactive in public loaders and creates.
