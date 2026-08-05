# Retire `rebuild-kf`; refuse a full rebuild with unreadable live kf entries

Status: **revised after the durable-write work merged; ready for execution approval.**

## Decision and compatibility boundary

Remove `rebuild-kf` in its entirety. It reconstructs authoritative kf state
by guessing from physical segment history: if two live physical records share
a hash, its file-id ordering can repoint the kf to the wrong value. That is
not a safe repair mechanism.

PR #273 (`fix/durability-write-ordering`) has closed the crash ordering that
created the defect this command was introduced to repair. Segment bytes are
durable before publication, and indexed writes use crash-recoverable kf
markers. The command therefore must not remain as a normal or automatic
recovery path.

This does **not** repair objects already damaged by a pre-fix release.
Removing the command is an intentional breaking maintenance-API change:
operators that upgraded from an affected build without ever running the
2026.07.1 repair must run the last release containing `rebuild-kf` against a
backup before upgrading to this release. The new binary detects invalid live
kf references before a full rebuild and leaves the original object intact;
it never attempts to infer a replacement pointer.

Keep a narrow full-rebuild preflight. It is not part of the durability
protocol and does not validate ordinary reads or light vacuum. It prevents a
different class of data loss: `rebuild_object_v2` currently uses
`slotcask_walk_live`, whose best-effort scan silently skips a live kf entry
when its segment cannot be opened or does not match. A full rebuild could
then commit a smaller object. Under `objlock_wrlock` there is no legitimate
concurrent-repoint explanation, so this path must abort the transaction.

## Invariants

- Kf remains the only logical authority. No code derives a new live kf
  pointer from segment-file order.
- New durable writes cannot create a kf entry whose published segment record
  is missing, uncommitted, or hash-mismatched.
- A full rebuild (`vacuum` with `compact`, `splits`, or streams correction;
  add/edit/remove-field rebuilds) validates every live kf reference before
  it copies anything. A bad reference produces an error and
  `rebuild_txn_abort`; the pre-rebuild data and schema remain live.
- Normal reads and the light in-place vacuum retain their existing
  best-effort/stale-pointer behaviour. The new strict check is called only
  while the caller holds the object write lock.
- Do not expose hashes, offsets, or paths in the wire response. They are
  internal storage details and the old plan's unbounded “complete list” was
  both an OOM risk and an unnecessary protocol expansion. The response
  reports the invalid-reference count; the operator restores from backup or
  uses the prior release's repair tool.
- Historical release notes continue to say what old releases shipped. Only
  remove links that would otherwise point to a deleted current-protocol
  section.

## Consumers and call sites checked before changing the public surface

`rg -n -i 'rebuild[-_]kf|\.kf_rebuild_done' -g '!docs/plans/**' .` currently
finds all live consumers:

| Consumer | Required change |
| --- | --- |
| `src/db/slotcask.c`, `src/db/slotcask.h` | Delete the unsafe scanner and declaration. |
| `src/db/main.c` | Delete the offline CLI command. |
| `src/db/query_maint.c`, `src/db/types.h` | Delete the maintenance handler and declaration. |
| `src/db/server.c` | Remove JSON mode, NQL command, and object-admin allow-list entry. |
| `src/db/embedded.c`, `src/migrate/main.c` | Remove automatic repair and `.kf_rebuild_done`. Keep varlen migration and compact. |
| `npm/index.js`, `npm/index.d.ts` | Remove `rebuildKf()` and the query-union member. |
| `build.sh`, `src/test/cases/test_rebuild_kf.c` | Remove the old repair test registration and source. |
| User docs and `AGENTS.md` | Remove the command from current interfaces and upgrade instructions. |

Historical mentions in `CHANGELOG.md`, `docs/release-notes/`, and
`docs/reference/changelog.md` describe released versions. Keep their claims;
remove only current-doc links to `#rebuild-kf` so the rendered history has no
dead link.

## Task 1 — prove the strict rebuild failure, then add it

### Test first

Replace the success-after-data-loss expectation in
`src/test/cases/test_rebuild_recovery.c`. Locate this exact comment:

```c
/* Trigger rebuild_object_v2 via splits change 8 → 16.
   Bug 2 fix: corrupt record is skipped, walk completes — no error. */
```

The test must instead assert all of the following after corrupting the first
live segment record's `vlen` and issuing `vacuum` with `"splits":16`:

```c
ASSERT_CONTAINS(resp, "\"error\":\"Rebuild aborted: 1 invalid live kf reference; original data restored\"",
                "rebuild rejects invalid backing record");
```

Then reconnect and assert `count == 100`, `item0000` remains readable, and
`schema.conf` still says eight splits. The three
`.rebuild_txn.{active,done,preparing}` paths must all be absent after the
response. This fails on the current tree because the rebuild succeeds and
commits 99 records.

Create `src/test/cases/test_rebuild_validation.c` from the setup/teardown
shape in the old `test_rebuild_kf.c`, but do **not** copy its repair
assertions. Its sole case must:

1. create an eight-split object with one record;
2. stop the daemon;
3. mutate the selected live kf entry's `file_id` to `0xffff`, `msync` and
   unmap it;
4. restart, request `{"mode":"vacuum",...,"splits":16}`; and
5. assert the same one-invalid-reference error, an unchanged count and
   unchanged eight-split schema.

This gives separate coverage for an unreadable segment header and an absent
segment file. It must not mutate process-wide environment state or use a
fixed port; use `TestEnv` exactly as the existing rebuild tests do.

Add the new test immediately after the existing
`src/test/cases/test_rebuild_recovery.c` entry in the `TEST_FILES` list
anchored by:

```make
src/test/cases/test_rebuild_recovery.c \
src/test/cases/test_rebuild_txn_recovery.c \
```

Run both tests and paste their failing output before implementing this task.

### Implementation

Add this public declaration immediately before the existing
`slotcask_walk_live` declaration in `src/db/slotcask.h`:

```c
/* Strictly validate every flag=1 kf entry against its backing segment.
   Caller must hold objlock_wrlock, so a failed check cannot be a concurrent
   repoint race. Returns 0 when all entries are valid, 1 when one or more
   entries are invalid, and -1 when the kf itself cannot be read. */
int slotcask_validate_live_refs(SlotcaskDb *db, uint64_t *out_invalid);
```

In `src/db/slotcask.c`, insert the complete helper immediately before the
existing `/* ============================================================ Query
primitives` comment. It intentionally has no callback and no error list:
the rebuild needs a count, not a second general-purpose scanning API.

```c
int slotcask_validate_live_refs(SlotcaskDb *db, uint64_t *out_invalid) {
    if (!db || !out_invalid) return -1;
    *out_invalid = 0;

    for (int shard = 0; shard < db->num_shards; shard++) {
        char kf_path[PATH_MAX];
        kf_path_for(kf_path, db->data_dir, shard);
        SlotcaskKfHandle kh;
        if (kfcache_acquire(&kh, kf_path, db->slots_per_shard, 0) != 0)
            return -1;

        for (size_t slot = 0; slot < kh.capacity; slot++) {
            SlotcaskKfEntry *entry = &kh.map[slot];
            if (__atomic_load_n(&entry->flag, __ATOMIC_ACQUIRE) != 1)
                continue;

            int invalid = entry->stream_id >= db->num_streams;
            SlotcaskSegHandle sh = { .slot = -1, .fd = -1 };
            if (!invalid) {
                char seg_path[PATH_MAX];
                seg_path_for(seg_path, db->data_dir, entry->stream_id,
                             entry->file_id);
                if (segcache_acquire(&sh, seg_path, 0, 0, 0) != 0) {
                    invalid = 1;
                } else if (entry->offset > sh.map_size ||
                           sh.map_size - entry->offset < 24) {
                    invalid = 1;
                } else {
                    const uint8_t *record = sh.map + entry->offset;
                    uint16_t klen = seg_rec_klen(record);
                    uint32_t vlen = seg_rec_vlen(record);
                    size_t record_size = db->format == SLOTCASK_FORMAT_VARIABLE
                        ? slotcask_record_size_varlen((size_t)klen, (size_t)vlen)
                        : (size_t)db->slot_size;
                    size_t encoded_size = 24u + (size_t)klen + (size_t)vlen;

                    if (record_size > (size_t)db->slot_size ||
                        encoded_size > (size_t)db->slot_size ||
                        record_size > sh.map_size - entry->offset ||
                        !seg_rec_live_with_hash(record, entry->hash))
                        invalid = 1;
                }
            }
            if (sh.slot >= 0 || sh.fd >= 0) segcache_release(&sh);
            if (invalid) (*out_invalid)++;
        }
        kfcache_release(&kh);
    }
    return *out_invalid == 0 ? 0 : 1;
}
```

This code deliberately checks bounds before dereferencing the record, unlike
the best-effort walk. `uint16_t` plus `uint32_t` cannot overflow `size_t` on
the supported 64-bit platforms. `record_size <= slot_size` and
`encoded_size <= slot_size` cover both fixed and variable formats without
requiring a plaintext key that a kf entry does not store.

In `src/db/query_find.c`, immediately after the existing successful-open
guard:

```c
if (!legacy_open || !new_open) {
    failure = "Failed to open slotcask handles for rebuild";
    goto txn_fail;
}
```

insert the following complete block and add `char failure_buf[128];` beside
the existing `const char *failure` local:

```c
uint64_t invalid_refs = 0;
int validate_rc = slotcask_validate_live_refs(&legacy_db, &invalid_refs);
if (validate_rc != 0) {
    if (validate_rc < 0) {
        failure = "Failed to validate live kf references";
    } else {
        snprintf(failure_buf, sizeof(failure_buf),
                 "Rebuild aborted: %llu invalid live kf reference%s",
                 (unsigned long long)invalid_refs,
                 invalid_refs == 1 ? "" : "s");
        failure = failure_buf;
    }
    goto txn_fail;
}
```

Change the final error emission at the `txn_fail:` label from the existing
two-argument conditional expression to this complete block, so a successful
rollback has the stable regression-test response:

```c
if (abort_rc == 0)
    OUT("{\"error\":\"%s; original data restored\"}\n", failure);
else
    OUT("{\"error\":\"%s; rollback incomplete, restart required\"}\n", failure);
return 1;
```

Do not change `slotcask_walk_live`, `v2_rebuild_walk_cb`, light vacuum, or
the record-copy algorithm. The strict preflight runs before the callback can
insert into `new_db`; `rebuild_txn_abort` restores the staged original data.

After implementation, run the two new/changed cases. Prove the regression
by temporarily reverting only this validation call and error block, run the
two cases to show the expected failure, then re-apply the block and paste the
passing output.

## Task 2 — remove the unsafe recovery interface

### Test first

Add the following assertion at the end of
`test_rebuild_validation_run`, after its rollback assertions:

```c
tc_request(tc,
    "{\"mode\":\"rebuild-kf\",\"dir\":\"default\",\"object\":\"rebuildvalidation\"}",
    &resp);
ASSERT_CONTAINS(resp, "\"error\":\"Unknown mode: rebuild-kf\"",
                "removed rebuild-kf mode is rejected");
free(resp); resp = NULL;
```

It fails while the legacy dispatcher remains. The test does not attempt to
run the removed process-level CLI command: that binary starts by trying to
connect to a daemon, while the JSON rejection is the stable public protocol
contract and covers the npm query path as well.

### Implementation

1. In `src/db/slotcask.c`, delete exactly the contiguous region beginning:

   ```c
   /* File-id comparator for qsort (ascending). */
   ```

   and ending immediately before:

   ```c
   /* Public entry point. Caller must hold objlock_wrlock for the object. */
   int slotcask_compact_segs(SlotcaskDb *db, int *out_dropped) {
   ```

   This removes `cmp_fid_asc`, `RebuildKfCandidate`,
   `rebuild_kf_apply_candidates`, and `slotcask_rebuild_kf`. Confirm with
   `rg -n 'cmp_fid_asc|RebuildKfCandidate|rebuild_kf_apply_candidates|slotcask_rebuild_kf' src/db`
   that no live reference remains.

2. In that same file, replace this obsolete compact comment:

   ```c
   donor must be preserved so rebuild-kf can recover it.
   ```

   with:

   ```c
   donor must be preserved rather than deleting a segment the live kf
   entry still references.
   ```

   Keep the `kf_failed == 0` guard unchanged. It remains the correct
   protection against deleting a donor when a repoint cannot be verified.

3. Delete the `slotcask_rebuild_kf` declaration from `src/db/slotcask.h`,
   `cmd_rebuild_kf` from `src/db/query_maint.c`, and its declaration from
   `src/db/types.h`. For the latter two, delete the exact function/declaration
   beginning `int cmd_rebuild_kf(const char *db_root, const char *object)`;
   do not disturb the adjacent `cmd_recount` documentation.

4. In `src/db/main.c`, delete the entire block from:

   ```c
   /* rebuild-kf — offline repair of corrupted kf entries by rescanning
   ```

   through its closing brace immediately before:

   ```c
   /* All other commands — route through server via TCP */
   ```

5. In `src/db/server.c`, remove `"rebuild-kf"` from the object-admin mode
   list and delete both `else if` branches whose conditions are respectively
   `strcmp(mode, "rebuild-kf") == 0` and
   `strcasecmp(cmd, "rebuild-kf") == 0`. Preserve the surrounding chains.

6. In `src/db/embedded.c`, make startup migration varlen-only: delete the
   `.kf_rebuild_done` setup/write, the step-2 block, and the second bullet of
   the `run_startup_migration` doc comment. It must still open and close each
   object and fail startup only when varlen migration fails.

7. In `src/migrate/main.c`, delete the phase-2 repair loop and sentinel
   write. Rename the surviving labels/output from `Phase 1/3` and `Phase 3/3`
   to `Phase 1/2` and `Phase 2/2`. `migrate-varlen` remains fatal; offline
   compact remains best-effort exactly as before.

8. Delete `ShardDb.prototype.rebuildKf` from `npm/index.js`, its method
   declaration and `QueryBody` union member from `npm/index.d.ts`, delete
   `src/test/cases/test_rebuild_kf.c`, and delete its now-dangling
   `build.sh` `TEST_FILES` line. Do not touch the
   `src/test/cases/test_rebuild_validation.c` line — Task 1 already added
   it in its own spot in the list.

## Task 3 — documentation and historical links

Update current documentation only:

- `AGENTS.md`: remove the command from source layout, CLI maintenance list,
  and schema-mutation reference table; add one sentence to the maintenance
  description that corrupted live kf references make a full rebuild abort
  and require restore/previous-release recovery.
- `docs/cli/index.md`: remove the maintenance table row.
- `docs/query-protocol/schema-mutations.md`: remove the complete
  `## rebuild-kf` section and its lock-model row; add the full-rebuild abort
  behaviour to the existing `vacuum` section.
- `docs/getting-started/install.md` and `docs/operations/deployment.md`:
  replace claims that current `./migrate` runs `rebuild-kf` with the two-phase
  varlen/compact description. State the compatibility boundary from this
  plan's first section, including that the old 2026.07.1 binary is required
  to attempt repair of legacy affected data.

In historical material, preserve feature history but remove dead links only:
remove the `schema-mutations.md#rebuild-kf` link target from the 2026.07.1
release note and `docs/reference/changelog.md`; do not rewrite their claims,
phase descriptions, or `CHANGELOG.md` entry.

Add a new bullet under the existing `## Unreleased` heading in
`docs/reference/changelog.md` (do not create a new heading — one already
exists there): state that `rebuild-kf` is removed, that a full rebuild
(`vacuum` with `compact`/`splits`/streams correction, or a field
add/edit/remove rebuild) now aborts with an invalid-live-kf-reference error
and restores the pre-rebuild object instead of attempting a repair, and
the pre-upgrade compatibility note from this plan's first section
(operators who upgraded from an affected build without running the
2026.07.1 repair must run the last release containing `rebuild-kf` against
a backup before upgrading past this release).

Finish with:

```bash
rg -n -i 'rebuild[-_]kf|\.kf_rebuild_done' \
  -g '!docs/plans/**' -g '!docs/release-notes/**' \
  -g '!docs/reference/changelog.md' -g '!CHANGELOG.md' .
```

It must produce no output. The remaining historical hits must be reviewed
manually and be factual descriptions of old releases only.

## Verification and handoff

1. Re-run the targeted tests from Tasks 1 and 2, then a fresh normal build
   and full suite:

   ```bash
   SKIP_TESTS=1 ./build.sh
   ./build/bin/shard-db-test run-all
   ```

2. This touches kfcache/segcache access and rebuild transactions, so run the
   required local dynamic-safety gates:

   ```bash
   BUILD_MODE=asan SKIP_TESTS=1 ./build.sh
   ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" \
     ./build/bin/shard-db-test run-all --jobs 2

   BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh
   TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" \
     ./build/bin/shard-db-test run-all --jobs 1
   ```

3. Paste all command summaries, compiler warnings, and the base-fail /
   reverted-fail / reapplied-pass evidence into the execution handoff. Leave
   the work uncommitted for raw-diff review.

## Execution rules

- Branch from the current default branch as `fix/retire-rebuild-kf`.
- Execute Tasks 1 → 2 → 3 in order. If any quoted anchor is absent, write
  `PLAN_NOTES.md` with the mismatch and halt the entire execution run; do not
  reinterpret the plan or continue to another task.
- If a design decision not covered here is required, stop and ask. In
  particular, do not reintroduce segment-history reconstruction under a new
  name and do not broaden the strict validator to ordinary query paths.
- Leave all work uncommitted pending the required independent raw-diff review.
