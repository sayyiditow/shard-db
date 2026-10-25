# Inactive packed-record codec

The internal packed_record module prepares future exact numeric/varchar records.
It has no production typed-schema adapter or public activation path. Existing
objects, declarations and commands keep their current behavior. Do not manually
put packed values into a public object.

The codec takes immutable schema-order cells. Fixed binary values stay in the
directory; variable fields use an eight-byte big-endian offset/length descriptor
and contiguous actual-size payloads. A null bitmap distinguishes NULL, empty
varchar and decimal zero. Numeric bytes use decnum's exact packed format.

pkr_measure computes actual value bytes; pkr_compose validates before writing
and receives actual destination capacity. No schema-maximum record buffer is
needed. Constructors/views clear outputs on failure; composition and numeric
encoding leave destination bytes untouched and report used=0 on failure.
Inputs, schemas, views and outputs follow the nonoverlap/borrowed-lifetime
contract in src/db/packed_record.h.

Raw transport accepts generation bytes0,1,3. Generation3 means PACKED+NULLMAP.
Unknown flags and PACKED alone are rejected before incoming upserts stage any
rows. This transport support does not activate packed schemas.

Bare varchar has a codec limit of4MiB. Bare numeric retains decnum's finite
PostgreSQL-sized limits. Actual rows remain subject to the existing segment
limit; a large schema maximum is not a promise that all maximum fields fit
together in one segment.

Consumer integration, authoritative layout metadata, durable publication,
downgrade protection and public activation require later reviewed plans.
No migration or application-level numeric capability is delivered by this
internal preparatory module alone.
