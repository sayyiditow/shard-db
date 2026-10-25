# Owning NEW-from-OLD write hook

SlotcaskUpsertOpts.new_from_old_alloc is an optional internal alternative to
the existing buffer-based new_from_old. The constructor returns actual-length
malloc/free-compatible bytes instead of the adapter allocating a theoretical
slot maximum. Both options together fail with EINVAL before either callback;
old callbacks and ordinary callers retain existing behavior.

The callback receives current OLD under the kf-shard write lock, after condition
gates, once before reservation/pre-commit. Missing OLD and rejected conditions
do not invoke it. It is not a fresh-insert default generator. It must not
re-enter the database or perform network I/O. Nonzero return is a hard error:
set errno for a cause; otherwise Slotcask supplies EINVAL.

Slotcask initializes output pointer/length to NULL/0 and takes ownership of
every non-NULL pointer returned, including a partial result on failure.
Success requires an allocated buffer even for zero bytes. Output must not
alias OLD or caller/schema storage. Accepted output survives the synchronous
transaction hooks; rejected and completed buffers are freed exactly once.
Durable marker recovery uses segment bytes, not freed callback storage.

The output contains final bytes. Supplied value/vlen are ignored, and neither
input nor output is passed to the registry's legacy trim callback in this mode.
A future legacy owning producer must trim its own final bytes first. Packed
producers return actual composed bytes. Generation comes from record_hdr2.

Before reservation, Slotcask checks uint32 value length and 8-byte-aligned
physical bytes including actual key and the 24-byte header against both slot
bound and current segment maximum. Overflow is EFBIG; invalid success output
is EINVAL. Constructor errno survives cleanup. Existing condition and
pre-commit failure policies remain unchanged.

There is no production constructor caller yet. This hook does not activate
packed schemas, change persisted formats, migrate objects, or complete single
and bulk command conversion. Explicit-schema tests exercise real physical
updates; public legacy tests remain the compatibility gate. Later write/read,
index, metadata and activation plans retain their own obligations.
