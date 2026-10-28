# Packed CSV fetch and multi-get commands (public format inactive)

`packed_csv_fetch` and `packed_csv_get_multi` are concrete, packed-only read
backends. Existing fetch and multi-key GET select them only for CSV and an
explicitly bound packed schema. Loaders/create-object still never bind packed;
R1 routes ordinary legacy/non-CSV reads through the shared checked collector.
No new query mode, format flag or startup migration is introduced.

The caller holds the shared object lock and pins the matching registry instance
through the call. Tests may own a stable directly opened physical fixture.
Schema/options/input storage stays immutable for the duration. The backend
uses the existing per-shard KF/segment read handles while collecting values.
It adds no writer gate, reservation, lock or lock-order change. Read handles are
released before FILE output. Callbacks do no index work, network I/O or database
reentry. Different rows can reflect different committed times; no object-wide
snapshot or cross-request cursor snapshot is promised.

Offsets skip live entries directly in the keyfile without accessing their
segments. Returned rows still require checked backing-reference reads; failures
are reported instead of silently dropping rows. Limits stop further scanning
once enough rows are collected. No global integrity proof or throughput
improvement is claimed.

Each line is rendered completely with the checked byte-aware CSV helpers. The
backend owns a linked list of exact header/row buffers and publishes nothing
until collection succeeds. Request/conversion/allocation/record/reference errors
free all prepared lines and emit one JSON error. A late bad requested key or row
cannot turn earlier prepared rows into a successful partial response.

After collection the backend writes the prepared lines and checks fwrite,
ferror and final fflush. If a FILE accepts a prefix and then fails, the backend
returns -1 and does not append replacement JSON to that prefix. This is not
transactional transport; the caller owns framing/connection handling. Command
status reaches the immediate fetch/get-multi caller; existing outer dispatch
conventions remain unchanged.

`PackedCsvOptions.buffer_budget` covers all retained response line allocations,
their NULs and list nodes, selector copies, decoded-key allocation capacity,
and a conservative rendering allowance for decimal/composite workspaces.
Production uses QUERY_BUFFER_MB. Failure before first output is explicit;
there is no truncated fallback or unbounded response streaming. Borrowed request
input, storage cache mappings and fixed automatic frames are excluded. The
allowance is a conservative admission bound, not a schema-ceiling allocation.

Fetch accepts NULL/empty cursor as zero, otherwise only nonnegative decimal
digits; leading zeros are allowed. Cursor+positive offset must fit int64.
Negative offset remains zero; the effective limit must be positive after the
command applies its existing global default. Fields/composites/delimiter follow
the checked CSV helpers, without the legacy selector truncation. total+CSV is
rejected. Empty objects produce a header-only response.

Multi-get requires a valid JSON array of strings. It decodes each string once,
uses explicit key lengths, preserves request order and duplicates, and emits a
row with empty value cells for each missing key. An empty array produces only
the header. Keys are not reparsed from response JSON. Plain binary keys preserve
NUL; UUID/sequence wire strings use the existing strict storage-key parsers and
reject embedded NUL or malformed forms. Raw escaped text bounds account for
the difference between an auto-key's display length and binary storage width.
The response echoes each decoded requested key; fetch formats stored keys in
the existing canonical wire form. Multi-get field projection is not added.

The command samples its deadline during collection and after traversal. The
existing cache read acquisition and stdio write are blocking operations;
no cancellation guarantee while waiting for those or client I/O is claimed.
Error paths release owned lines, cache handles and the caller's scoped registry
reference. Construction errors are returned as -1 with errno and a JSON error;
I/O failure also returns -1, with EIO.

Visible NUL encoding remains `\u0000` with doubled literal backslashes. Ordinary
CSV parsing precedes byte unescaping. Public import/SDK decoding is still an
activation gate, not delivered by these output backends. Find/joins, aggregate
CSV and other predicate/delete/index/maintenance consumers remain in
the Phase 3B inventory; exact numeric criteria/indexes and aggregate operations
retain their later roadmap owners. This child does not close Phase 3B.

Current ordinary read/write and declaration integration contracts are in
[ordinary type integration](ordinary-type-integration.md).
