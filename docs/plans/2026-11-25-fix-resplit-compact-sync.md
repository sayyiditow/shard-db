# Fix plan 6 — keyfile sync ordering in resplit and segment compaction (AUD-KF-RESPLIT-SYNC, AUD-COMPACT-ORDER) and the minors batch

## Brief
**Problem and root cause.**
1. `kfcache_resplit_locked` ignored the Phase C `msync` of the rebuilt keyfile and renamed it over the live keyfile
   anyway; a failed post-rename directory fsync was only used to protect bitmaps. A sync failure could publish an
   unsynced keyfile (records present in segments, missing from the keyfile after a crash).
2. Segment compaction repointed keyfile entries in the mapping (page cache only) and unlinked the donor segment
   file immediately; a power loss before keyfile writeback left on-disk entries naming a deleted file.

**Logical flow.**
1. Resplit: Phase C checks `msync`; on failure the resplit is abandoned (old keyfile kept, retry delayed like a
   failed bitmap prepare). After the rename, a failed directory fsync sets `dir_unsynced` on the cache entry; every
   keyfile sync retries that directory fsync first and fails while it keeps failing, so no write is acknowledged
   on a rename that may not survive power loss.
2. Compaction: before a donor is dropped, `compact_sync_all_kf` msyncs every keyfile shard; a failure stops the
   compaction with the donor still in place. `compact_drop_seg_file` reports a failed directory fsync.
3. Minors: delete K re-entry is idempotent (an already-tombstoned entry with the same hash and key counts as done);
   `bulk-insert`/`bulk-insert-delimited` check every arena allocation and refuse the whole batch on out-of-memory
   (including the silent `{"inserted":0}` on a record-array realloc failure); NQL validates `dir`; `list-tokens`
   fingerprints only tokens longer than 16 characters.

**Decisions.** All DEFAULT (smallest change that makes the sync order correct); no config or protocol change except
the clearer errors above. Blast radius: `slotcask.c` (resplit, sync, compaction), `query_bulk.c` (two parsers),
`server.c` (NQL, list-tokens), `shard_test_ctl.h` (three TEST_BUILD seams). Not breaking.

**Tests.** `test-resplit-msync-failure`, `test-resplit-dirsync-gate`, `test-compact-donor-kf-sync` (red-proven by
disabling the sync), `test-kf-window-delete-replay` (red-proven), `test-kf-window-duplicate-key-split`,
`test-bulk-insert-arena-oom` (JSON and delimited, first and growth allocation), IP-set tombstone wire case in
`test-token-store-chain`.

**Accepted, not changed:** the 60 `cert-err34-c` rows (`atoi`/`strtol` without error checks) are operator-controlled
`db.env`/engine-written config values (threat model A5); a garbage value yields 0 and the range checks reject it.
AUD-PLN-2/3 (planner quality, correct results) and AUD-REDUCE-1 (refactor) stay in the index for later.

**Review log.** none yet (blind review of the branch diff is still owed).
