# Ordinary typed writes, reads and schema transfer

Packed storage remains inactive in public loaders/create-object. These internal
adapters prepare the ordinary routes for activation after criteria/indexes,
aggregates, other type grammars and durable migration are complete.

Ordinary readers retain the existing per-shard KF/segment read-handle lifetime.
Multi-key reads keep shard buckets, one I/O-pool job per touched shard and the
existing batched lookup/resolve-fetch pattern. They add no writer gate, writer
reservation, lock, lock-order change or object
snapshot guarantee. A fetch or multi-get can observe different committed times
across rows, exactly as ordinary readers already can. Responses are collected
before client I/O; cache handles are released before publication. An invalid live
reference fails the command instead of being silently omitted or called absent.
The private packed CSV route no longer reserves every shard's writer gate.

GET, fetch (default/rows/dict/CSV), projected values, multi-get, keys and existence
routes preserve explicit key lengths, JSON escaping and decimal number text.
Fetch keeps its existing envelope and numeric skip cursor. Offsets skip live
entries in the keyfile before segment access, preserving the earlier pagination
optimization; only returned records require backing-reference checks. Multi-get keeps
request order and duplicate members; missing records render null. Bounded owned
response chunks share the request budget with selector copies and temporary
rendering allocations. Packed rendering reserves conservative decimal/join
workspace before validation; its length admission scan does not replace record
validation. Legacy decoder allocations are charged by capacity, not strlen.
Borrowed input/cache mappings and fixed automatic frames are not response heap.
Failed output streams return I/O failure without attempting another JSON envelope
following bytes already accepted by the stream.

Single computed updates evaluate every operand against immutable OLD, then
compose complete NEW. Numeric addition uses the decimal core and applies the
column's rounding once on assignment; `$copy` retains type/declaration compatibility.
Legacy scaled-int64 numeric uses exact addition and its real width/scale limit;
its historically informational P is not retroactively enforced. Every duplicate
computed occurrence is validated, including one later overwritten by a literal.
Memory admission covers retained payload owners, OLD numeric validation,
generated text plus its copied payload, and aligned decimal/result workspace
before allocation. Allocation failures preserve their resource-error identity.  Migration audits
that metadata separately. Bulk updates retain their existing literal-only contract;
criteria/per-row guards remain owned by R2. Packed bulk input rejects repeated
normalized storage keys before request submission, preserving its approved private
policy; the older public legacy bulk duplicate policy is unchanged. Auto-key bulk
inserts return generated keys in input order. Generator/sequence gaps after a
failed command are allowed; consumed sequence numbers are never rolled back.
Bulk accounting uses conservative request admission and thread-safe byte counters
inside existing worker callbacks; it changes no worker scheduling or lock protocol.

New create/add/edit declarations consume their complete JSON strings and type
parameters. Defaults have one modifier; literal defaults retain the 255-byte limit.
Malformed generators, overlength declarations, duplicate fields and currently
invalid scalar literal defaults fail before schema publication. Existing legacy
metadata retains its historical read interpretation. Malformed legacy default
syntax is marked unsafe for generation rather than making old stored records
unreadable. Calendar/bool/UUID/IP/string grammar expansion remains R4; these checks
reuse current checked encoders and do not claim those later contracts complete.

Describe includes the exact original `spec`, streams, auto-key identity and
`declaration_layout`. Logical varchar capacity is independent of descriptor width.
Argv/TUI schema export copies the exact spec; it never guesses numeric P=18.
Exports stage their complete manifest and preserve an existing destination on
failure. A packed manifest cannot be imported as legacy: create-object refuses
its layout marker until authoritative packed publication/migration arrives in R5.
Tagged schema manifests with a differing imported stream count are refused
explicitly until R5 can honor geometry; ordinary untagged create requests keep
their historical handling. No migration, schema binding or activation is
introduced here.

Ordinary CSV uses the approved reversible byte-escape layer after CSV decoding:
backslash doubles; control bytes use `\u00xx`. JSON remains the typed interchange
format because CSV still conflates NULL and an empty cell. Existing positional
bulk input is not an inverse CSV export parser. Npm exposes raw response text;
finance applications must preserve number tokens rather than passing arbitrary
precision decimals through JavaScript Number/JSON.parse. The optional pure-JS
cell decoder accepts bytes from a byte-preserving transport. The native query
string route remains UTF-8 text; it is not a new binary transport API.
