# Packed bulk insert adapter (inactive schema format)

`typed_bulk_insert_command` is an internal synchronous adapter used by both
JSON bulk-insert runners (dict/array, inline/file) and positional delimited
runners (inline/file) when an explicitly bound packed schema is present.
Public loaders and object creation still produce legacy schemas. This document
records private integration, not public availability or migration completion.

Rows carry source spans and actual composed bytes rather than `total_size`
arenas. Keys/JSON values are copied with checked allocations; raw delimited
spans borrow the input through the complete deferred request. A schema with
256 bare varchar fields can have a theoretical slot bound above 1 GiB, while
small rows still consume roughly two KiB. Bare varchar remains 4 MiB; decimal
limits and assignment rules come from the shared decimal/packed core.

The adapter validates complete JSON before any write. It handles JSON dict and
array forms, decoded keys, escaped/embedded-NUL varchar values, NULL, duplicates
inside field objects via the shared row collector, and ignored fields only
after syntax validation. Keys must be nonempty and fit max_key. Non-object or
missing record values are per-row errors. Duplicate record keys keep input
order within each shard and retain the existing coordinator's outcomes rather
than adding last-wins semantics: two fresh duplicate keys in the measured
single window insert the first and report the later row as an error. A unified
record-duplicate policy across windows remains a Phase 4 activation contract.

Delimited input uses bounded spans and active schema field order, treats CR/LF
as line boundaries, pads missing columns with empty spans, ignores surplus
columns, and does not add CSV quoting/escaping. Lines with no separator are
skipped; an explicit empty column remains a real value. Empty varchar is present empty;
empty numeric is invalid; empty fixed scalars retain present zero. Removed
fields consume no input column. Unlike JSON, positional import does not run
JSON defaults or AUTO_UPDATE stamping. AUTO_CREATE is preserved from locked
OLD or stamped for a fresh row, through the same finalizer as JSON insertion.

Records are grouped by kf shard with one input per shard and one synchronous
`slotcask_bulk_request_execute` per command. `value_compute` runs through the
existing M-phase locked-OLD path with `value_rewrites_payload=1`; NEW never
gets staged from placeholder/source bytes. The callback validates OLD's full
generation, constructs an owning actual-size row, and checks the real key,
record length, schema slot bound and unchanged segment ceiling before returning
it. The caller retains each allocated NEW through the complete request and
frees it once after all pipelines return. Existing deferred durability,
markers/replay and per-shard gates remain authoritative; no new commit protocol
or metadata publication is introduced.

The bulk engine represents a nonzero value callback result as a condition skip.
This adapter keeps a row-local construction-error side channel, so syntax,
range, generation, allocation and size failures count as `errors`, while an
existing key under `if_not_exists` counts as `skipped` and does not execute the
constructor/defaults. `inserted` counts only fresh rows. Mixed valid/invalid
rows retain best-effort semantics; the batch is not a transaction. Coordinator
failures cannot become success, and pending durability is an explicit error
that must not be retried blindly. Counts remain in keyfile headers.

The shared legacy collector now reports its requested input count even if its
allocation fails. Both existing callers can therefore mark prepared workers
failed instead of folding an unexecuted batch as successful.

Allocation scales with actual source and composed values. This child does not
introduce a new command-wide memory-budget contract: bulk-insert is not one of
the existing QUERY_BUFFER_MB collections. Ordinary TCP ingress keeps its
existing request limit; embedded commands and file imports retain their
existing entry-point policies. Defaults and input/output expansion, command-wide
memory budgeting, auto-generated keys and packed indexed writes remain Phase 4/5
activation work. The 8 MiB allocation ceiling in the scratch probe is an
instrumented assertion for small maximum-schema rows, not a new public limit.

Packed auto-key objects and every packed secondary-index declaration currently
fail closed before defaults or mutation. Private adapter tests exercise actual
storage and request execution. Public schema activation, public end-to-end
packed commands, criteria/CAS updates, bulk-delete, readers, schema rewrites
and migration are not discharged by these fixtures. Existing public legacy
tests remain mandatory compatibility coverage.

[Keyed partial bulk updates](packed-bulk-update.md) reuse this collection and
deferred-request lifetime while requiring OLD. Their blank positional cells
preserve OLD rather than writing insertion's empty/zero values. Packed
conditions/criteria remain guarded until query integration is ready.
