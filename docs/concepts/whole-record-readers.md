# Whole-record typed JSON readers (packed format inactive)

`typed_decode` dispatches by immutable schema binding. An explicitly bound
packed schema uses `typed_packed_decode`; legacy schemas keep existing checked
fixed-field rendering. The packed view validates exact generation, directory,
NULL entries and decimal payloads before rendering. Packed schema/legacy bytes
and legacy schema/packed bytes both reject. Transport flag recognition alone
never chooses a layout. Public loaders and creates still do not bind packed.

The shared full-record APIs now preserve exact numeric text, raw varchar bytes
through JSON escaping, explicit NULL, empty/zero values and removed fields.
Whole-field zero substitution for incomplete legacy fields is unchanged; this
is distinct from a writer's partial-prefix carry/pad policy. Bare varchar and
numeric limits remain 4 MiB and 131072 integer/16383 fractional digits.

`typed_decode_stream` is a checked FILE adapter over the allocating decoder.
It fully validates/renders before writing, so input, conversion and allocation
failures leave the output untouched. Returns 0 only if fwrite accepts every
byte and no FILE error is present. The caller owns flushing and closing. It
does not promise transactional I/O: a failing FILE can accept a prefix first.
GET therefore does not append a replacement JSON error when ferror is set,
and it checks its final newline. Pre-output ENOMEM becomes an explicit error.

This intentionally replaces the old direct streaming loop, whose failed large
field allocation fell back to 512 bytes and whose enum/render failures were
silently omitted. It adds an owned response buffer to GET, including legacy GET. Packed
rendering allocates exact escaped JSON bytes plus NUL, with separately bounded
decimal workspaces. Legacy rendering retains its conservative estimate: base4
plus, per nonremoved field, name bytes + need +4; need is5 for NULL, 6*n+3 for
varchar, 6*n+4 for enum, and512 otherwise. Here n is actual/clamped stored varchar
length or selected enum text length. During rendering a heap field workspace
(when need >512) can coexist with that response buffer; all such workspaces
are freed before output. Thus legacy peak includes raw record + estimated
response capacity + largest simultaneous heap field workspace, not merely
final JSON length plus NUL. An ordinary string may reserve about six times its
text length; scalar fields reserve512 bytes each. There is no declared-schema
ceiling allocation. One 4 MiB packed all-control varchar needs about24 MiB JSON;
other fields add their output. This deliberately does not change the legacy
allocator just to make its memory estimate exact.
No new response cap or QUERY_BUFFER_MB enforcement is claimed. The configured
query buffer cap and limits of other read routes remain later activation work.
No benchmark or throughput claim accompanies this correctness tradeoff.

This is a rendering bridge, not query integration. `typed_get_field_str`,
projections, composite/CSV/rows/join extraction, matching, sorting, indexes and
aggregates still have legacy assumptions and separate error/length contracts.
A binary varchar cannot be safely passed to their strlen-based paths. A decoded
full row does not prove the query's planner/matcher/index/emit-failure path is
packed-ready. Existing callers that substitute null on decode failure remain
explicit follow-up work; no public packed activation is authorized here.

Private C tests cover exact JSON and stream equality, NULL/empty/binary data,
all removed fields, both layout mismatches, invalid flags/descriptors/truncation,
legacy short tails/long enums, actual disk bytes and reopen, full decimal limits,
4 MiB values and a tiny row under a >1 GiB schema. Scratch-only probes inject
allocation and output failures in actual decoder code and the source-extracted
GET body. They mock schema/registry/read dependencies locally, never install a
production global switch or bind a public object. Public packed GET acceptance,
metadata reload and end-to-end network failure handling remain later gates.
