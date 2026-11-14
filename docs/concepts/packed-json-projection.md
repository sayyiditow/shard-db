# Checked JSON projections (packed format inactive)

The private packed JSON APIs produce exact selected values, rather than decoding
an entire row and parsing it again. `typed_packed_projected_cell` accepts a field
index and returns one owned JSON cell. `typed_packed_project_json` accepts a CSV
selector and returns one complete owned JSON object. Both use explicit record
lengths and reject inconsistent fixed carrier/type and enum metadata before
reading scalar spans. They validate the whole packed view, including unselected descriptors and
decimals, before rendering. Schema/generation disagreement rejects. Budgets
include NUL; errors return NULL with errno, never a successful partial string.
NULL or removed fields render as bare null. Unknown selected names render null
only after record validation. Numeric values are exact bare JSON numbers;
projection/rows booleans retain the existing quoted display strings "true" and
"false". Full-record JSON keeps its separate native boolean policy. Varchar
uses its full byte span, so embedded NUL is escaped as \u0000 and later bytes
survive. Names and string values are escaped with the same checked helpers.

The CSV selector is at most MAX_LINE-1 bytes and at most MAX_FIELDS nonempty
names. Empty comma tokens collapse, whitespace is significant, order and
duplicates are preserved. Empty selectors produce {} at this direct API;
the existing public dispatcher sends an empty fields argument to full GET.
Unknown/removed names map to null. Oversized selectors reject with E2BIG rather
than being silently truncated in this private helper. Packed '+' projections now
use [length-aware named display text](packed-named-projection.md): skip absent
parts, concatenate exact bytes in order, then quote/escape once. This replaces
the former private ENOTSUP restriction. Legacy raw-string composites/parsing
remain unchanged; upstream JSON-to-CSV parsing retains its own limits/unescaping
behavior. This helper does not repair ingress or activate packed queries.

Packed-only branches in json_projected_field and json_typed_row_cell call the
new checked cell API. Their legacy branches are unchanged. Packed cmd_get_fields
builds the complete selected object before writing: conversion, corruption or
allocation failure emits one error response without a success prefix. It checks
fwrite, FILE errors and its newline, and never appends a replacement error after
an I/O prefix. FILE ownership, flushing and closing stay with the caller; this
is accepted-byte checking, not transactional socket delivery. ENOMEM becomes
"out of memory", ENOTSUP explains pending packed composites, E2BIG reports the
projection limit and malformed records become "corrupt record".

Object output allocation is exact escaped selected JSON plus NUL, with an
actual-length CSV copy and separately bounded decimal workspaces. An unselected
large varchar is validated as a span but never copied into a whole JSON row.
View validation can allocate decimal validation workspace even for unselected
decimals. cmd_get_fields and the shared cell adapters pass SIZE_MAX, preserving
the existing uncapped output policy. QUERY_BUFFER_MB integration is future
activation work. No schema-max allocation or speed improvement is claimed.

This adds a rendering primitive, not packed query/rows/join activation. Those
callers still have independent matching, sorting, raw-offset, envelope and error
policies, including NULL/error substitution. Their use of a correct cell helper
does not establish end-to-end packed correctness. typed_get_field_str,
decode_field, CSV, composites, legacy allocation/error handling, and legacy
projected GET streaming remain unconverted. No public object binds packed.
Metadata, migration and the separate certification rig remain later work.

Private C tests exercise exact decimal cells and digit limits, binary strings,
NULL/removed/unknown values, duplicate selectors, quoted names, limits, invalid
flags and unselected corruption, and ordinary legacy scalar/composite controls.
Scratch probes instrument actual row/query modules and the source-extracted
cmd_get_fields body with local schema/registry/read mocks, exercising every
selected allocation, output failure, maximum-schema bounds and unselected
4 MiB content. They do not prove public network/metadata acceptance.
