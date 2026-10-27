# Packed named display text and composite JSON (public format inactive)

`typed_packed_named_text` returns owned display bytes and an authoritative length
excluding its terminating NUL. It accepts one name or '+' concatenation, validates
one complete packed view first, and never routes through legacy fixed offsets.
The caller must consume text_len, not strlen: varchar and enum display text can
contain bytes needing JSON escaping, and varchar can contain embedded NUL.
Errors return NULL with nonzero errno and reset text_len to zero. Legitimate
NULL/removed/unknown scalar and a composite with zero combined display bytes
return NULL with errno zero. Scalar empty varchar is an owned empty string.
Budgets include NUL; zero budget rejects even absence. Legacy raw APIs remain
unchanged and do not gain packed dispatch.

`typed_packed_named_json` uses the same explicit view, rendering policy and
checked ownership. Scalars preserve existing projection policy: exact numeric
text is bare, boolean is quoted, strings are escaped, NULL is literal null.
A composite is always one quoted display string, irrespective of numeric parts;
its entire concatenation is escaped once. Components are joined in order with
no separator; unknown/removed/NULL parts are skipped, zero/false are real text.
An empty concatenation produces literal null, including all-empty varchar parts,
matching the established legacy concatenation convention. Embedded NUL and its
suffix survive as escaped JSON bytes. Full-record boolean behavior is separate.

Both selectors are NUL-terminated and bounded by MAX_LINE-1 bytes. '+' is the
composite separator; empty '+' tokens collapse, whitespace is significant,
and at most MAX_FIELDS nonempty components are allowed, including unknown names.
No 255-byte selector truncation is inherited from legacy decode_field. Limits
reject E2BIG; malformed views reject EILSEQ; invalid API/schema arguments reject
EINVAL; allocation/budget failure rejects ENOMEM; arithmetic overflow rejects
EOVERFLOW. Legitimate missing output must be distinguished using errno/text_len.
The APIs do not promise errno zero for non-NULL successful output.

Packed quoting feeds the int-returning escape backend pieces of at most4096
input bytes, each emitting at most24576 bytes, while totals remain size_t.
Repeated components may therefore escape beyond INT_MAX without narrowing the
complete count or imposing an undocumented response ceiling.

The shared packed JSON projection helper and complete projected GET object now
use named rendering. Their former packed-composite ENOTSUP gate is deliberately
removed. Object selector comma parsing still has its existing MAX_LINE/MAX_FIELDS
limits and preserves order/duplicates; every selection remains unpublished until
the whole object is built. A component allocation failure aborts the whole cell/
object, never skips a failed component or emits a successful shorter value.
The unchanged projected GET command checks writes and its newline; a FILE may
accept a prefix before failing, so no transactional I/O guarantee is claimed.

Memory is actual selected output plus NUL and, for a composite, one exact raw
concatenation workspace plus bounded scalar/decimal workspaces. During JSON
rendering that raw concatenation coexists with cell/object output. Object sizing
and writing each create and release that temporary independently. Duplicate
components can make selected output larger than the stored record; the bound
is the caller's explicit output budget, not original vlen. No schema-maximum
allocation, whole unselected-varchar JSON copy, speedup or QUERY_BUFFER_MB
integration is claimed. Production JSON adapters retain SIZE_MAX while packed
binding remains inactive; activation still requires an enclosing memory policy.

This is output-only integration. typed_get_field_str, decode_field, CSV, matching,
sorting, cursor capture, aggregate grouping, join building and surrounding query
error handling retain their own contracts. Teaching their strlen callers to
accept binary data would truncate it; no such change occurs here. The new raw
named API has no production consumer. Loaders/create-object still never bind
packed schemas; no metadata/version/migration/default-format change is made.
Private tests and actual-source mocked command probes prove this boundary, not
public packed network/query acceptance or concurrency safety of activation.
