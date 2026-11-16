# Single-command typed write integration

Single insert/upsert and partial update construct final NEW through the owning
Slotcask callback. No registry/loader/create-object path binds packed layouts;
only explicit immutable test schemas exercise the packed branch at this stage.
This is preparation, not public numeric/varchar activation or migration.

The callback runs under the kf shard write lock after built-in conditions. An
explicit new_from_old_alloc_on_insert option permits a fresh key with NULL OLD;
its zero default preserves the previous OLD-required owning-hook behavior.

Legacy single writes keep existing encoding, computed operations, CAS and index
semantics. Their owning producers allocate total_size, trim their own final
legacy bytes, and never allocate slot_size scratch. Insert auto_create transfer
now uses the locked OLD rather than a separate pre-lock snapshot.

Packed insert collection uses complete scalar JSON validation and positional
fields. Every known duplicate is validated, the last valid value wins, and
unknown values must still be valid JSON. Ordinary omitted defaults run once;
auto_create is copied from OLD with nullness, or stamped once for a fresh key,
before final NOT NULL validation. Insert auto_update retains the adapter's
existing omitted-default policy; Phase 4 owns any broader policy change.
Packed partial update borrows OLD, overwrites supplied scalar fields, preserves
omissions, applies no insertion defaults, and stamps auto_update after the patch.
JSON escapes are decoded once, including embedded NUL in varchar byte spans.

Packed indexed/conditional writes fail before defaults or writes, and known
computed object/array patches fail before composition. These are private guard
rails until Phases 4–5 integrate them; no public legacy operation is disabled.
The callback validates OLD generation in both schema mismatch directions.
Dry-run validates generation and performs no mutation.

NEW generation comes from the immutable schema. Actual physical size includes
24 header bytes, actual key bytes and 8-byte alignment, and must fit both the
object slot bound and the current segment ceiling. The theoretical schema
maximum may exceed the segment ceiling: small actual records still work.
Single packed insert/update allocates no schema-maximum scratch buffer.

When return_record is requested, its JSON is prepared from final actual NEW
before segment reservation. Allocation/render failure rejects the write;
a successful response cannot silently omit the requested record. The shared
buffered legacy decoder now checks all allocations and rendering results and
sizes varchar/enum buffers from actual values. This also repairs its existing
buffered readers; stream-reader integration remains later work. Slotcask owns
NEW after callback return; command context owns only response JSON and any
unconsumed legacy insertion buffer. Rejections and durability-pending outcomes
free those allocations and use existing transaction/recovery ordering.

Future required integrations remain bulk formats, read/query/index/maintenance
consumers, exact computed operations, public metadata/publication and activation.
A physical private-schema fixture is not a public daemon/SDK acceptance test.
