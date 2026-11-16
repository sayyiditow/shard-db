# Owning NEW-from-OLD write hook

SlotcaskUpsertOpts.new_from_old_alloc is an optional internal alternative to
the existing buffer-based new_from_old. The constructor returns actual-length
malloc/free-compatible bytes instead of the adapter allocating a theoretical
slot maximum. Both options together fail with EINVAL before either callback;
old callbacks and ordinary callers retain existing behavior.

The callback receives current OLD under the kf-shard write lock, after condition
gates, once before reservation/pre-commit. Rejected conditions never invoke
it. Missing OLD invokes it only with new_from_old_alloc_on_insert=1; otherwise
the previous OLD-required behavior remains. The flag without an owning callback
is EINVAL. A fresh-capable callback must handle NULL OLD explicitly. It must not
re-enter Slotcask or perform network I/O. Nonzero return is a hard error:
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

Single-command insert/upsert/update now uses the constructor under the
[write integration](single-write-integration.md). [Packed bulk insertion](packed-bulk-insert.md)
uses a separate caller-owned value callback: its buffers survive the entire
deferred bulk request and are freed by that adapter, not by this owning hook.
Both integrations remain private while packed schemas are inactive. Public
legacy tests remain the compatibility gate; later read/index, metadata,
migration and activation plans retain their obligations.
