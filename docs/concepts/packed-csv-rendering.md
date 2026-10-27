# Checked packed CSV rendering (public format inactive)

`typed_csv_cell_text` converts an explicit byte span into one owned CSV cell.
`typed_packed_csv_header` and `typed_packed_csv_row` return complete owned lines,
including LF. Every API returns an authoritative length excluding its final NUL;
the supplied budget includes that NUL. Failure returns NULL, nonzero errno and
length zero. NULL data/key with positive length is invalid; zero-length spans
are valid. These helpers do not write to FILE or activate a public route.

CSV has no built-in JSON unescaping. This private packed contract therefore
defines a byte escape layer inside ordinary CSV quoting:

- NUL becomes the six visible bytes `\u0000`.
- Other C0 controls and DEL become lowercase `\u00xx`, including CR/LF/tab.
- A literal backslash becomes `\\`, so literal `\u0000` differs from NUL.
- Other bytes pass through unchanged, preserving readable UTF-8. Arbitrary
  non-UTF-8 bytes are still bytes; this layer makes no charset validation claim.
- After byte escaping, a cell containing the delimiter or quote is surrounded
  by quotes, and each internal quote doubles. Decode ordinary CSV first, then
  byte-unescape `\\` and `\u00xx`, then interpret text in its chosen charset.
  This is byte escaping, not a general JSON/Unicode-codepoint decoder.

The escape layer is reversible for every byte value. It needs neither Base64
nor a magic prefix. Empty string and missing/NULL field both remain empty CSV
cells. The literal text `null` remains text. Packed rows preserve embedded CR/LF
through escapes; legacy CSV still normalizes CR/LF to spaces. Existing clients
see visible escape text unless they opt into the documented unescape step.
Public activation must document this format difference and SDK behavior.

Delimiters are printable ASCII except quote/backslash, plus tab. Invalid
delimiters reject EINVAL rather than producing ambiguous output. This does not
change the existing public `parse_csv_delim` or legacy CSV grammar.

Header and row selection agree: NULL/empty selector means all nonremoved fields;
nonempty comma lists preserve order, duplicates and whitespace and skip empty
tokens. Unknown, removed and NULL selections produce empty cells. '+' joins raw
display bytes before escaping; numeric components retain every decimal digit.
Selector limits are MAX_LINE-1 bytes, MAX_FIELDS columns and MAX_FIELDS nonempty
components per column. Oversized input rejects E2BIG. Row keys must already be
formatted display bytes (UUID/sequence formatting stays with the caller).

Rows validate the complete schema/packed view before projection, including
unselected payloads. Generation mismatch/corruption rejects; header validation
checks schema metadata but has no record to validate. No descriptor is treated
as a legacy scalar. Helpers build a complete line before returning it; this is
not an atomicity guarantee for an enclosing response or FILE write.

Memory is actual encoded output plus one selected raw composite/decimal
workspace at a time and bounded validation workspaces. Sizing and writing each
construct and release that workspace. The output budget does not bound total
peak memory; no QUERY_BUFFER_MB integration or performance improvement is
claimed. A 4 MiB all-NUL varchar expands to 24 MiB; a one-field row with key `k`
needs 25,165,828 bytes including separators, LF and NUL. The >1 GiB maximum
schema does not imply a >1 GiB rendering allocation for small selected values.

No production caller uses these new APIs yet. Legacy cell/header/row writers,
CSV find/fetch/get-multi/joins/aggregates/keys, query error propagation and
matching/sort/group/index consumers remain unchanged. A following consumer
integration plan must use owned complete output, carry allocation/conversion/
I/O failure to the command result, avoid treating errors as empty cells, define
errors after any previously accepted CSV prefix, and enforce request memory
limits before public packed activation. Loaders/create-object remain inactive.

Default all-fields output retains physical field indices rather than parsing
stored names as expressions. Explicit selectors still interpret '+'. Header
metadata validation uses the codec's pure pkr_schema_validate entry point;
unknown kinds, bad directory/prefix/offsets, declaration limits and payload
bounds reject even if the malformed field is not selected.
