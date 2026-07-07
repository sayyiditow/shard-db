# Coverity: misc hardening (silent accept-loop errors, corrupt-schema OOB read, unchecked ftruncate ×2, oversized memcpy)

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-misc-hardening main`.
- Do the five tasks below **in order**. They touch four different files and are independent of each other.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all five tasks are done: `./build/bin/shard-db-test run-all`.
- Every insertion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

This plan covers the five remaining true-positive Coverity findings not already covered by the other six themed plans (stack-overflow-fixes, disk-corruption-hardening, null-deref-registry, resource-leaks, data-races, dead-code-cleanup). Full triage context: `docs/coverity-triage-2026-07.md`. These five don't share one root cause — they're grouped here because each is a small, standalone hardening fix:

- **CID 1696474** (`cmd_server`, `src/db/server.c`): the accept-loop's error branch retries forever on *any* errno, expected or not, without ever logging the unexpected case — an operational-visibility gap, not a crash.
- **CID 1696469** (`decode_field_to_buf`, `src/db/config.c`): a corrupt on-disk schema (`fields.conf` reporting `f->size < 2` for a `varchar` field) can drive a negative `int` into a `(size_t)` cast, producing an astronomically large length passed to `json_escape_into` — a genuine out-of-bounds read triggered by disk corruption, not attacker input over the wire (schema files aren't part of the query protocol).
- **CID 1696470** (`compact_stream_worker`, `src/db/slotcask.c`) and **CID 1696449** (`slotcask_migrate_to_varlen`, `src/db/slotcask.c`): both functions rotate to a new destination segment file by `munmap`-ing the old one, then shrinking it to its actual used size with `ftruncate`, then `close`-ing it — but neither checks whether that shrink-`ftruncate` succeeded. A small number of lines later, each function *does* check the return value of the analogous grow-`ftruncate` on the *next* segment file — so the missing check is an inconsistency within the same function, not a missing pattern.
- **CID 1696418** (`cmd_add_indexes`, `src/db/index.c`): copies a fixed `MAX_FIELDS`-sized stack array in full via `memcpy`, when only the first `btree_count` rows were ever written — reads uninitialized stack memory into `fields` (harmless in practice since `nfields` is set to `btree_count` right after, so the garbage rows are never iterated, but still a defect Coverity correctly flags and worth fixing for its own sake).

None of these five need new dedicated tests — see each task's own rationale.

---

## Task 1 — `cmd_server` accept-loop swallows unexpected errno silently (CID 1696474)

### The bug

`src/db/server.c`'s `cmd_server` accept loop retries on both the expected transient errnos (`EINTR`/`EAGAIN`/`EWOULDBLOCK`) and any other, completely unexpected errno (e.g. `EMFILE`, `ENFILE`, `ECONNABORTED`) — identically, with no logging in the unexpected case:

```c
        struct sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int cfd = accept(sfd, (struct sockaddr *)&caddr, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            continue;
        }
```

If the process runs out of file descriptors (`EMFILE`/`ENFILE`) the server silently spins retrying accept() forever with zero operator-visible signal — nothing in the logs indicates why new connections have stopped being accepted.

### The fix

In `src/db/server.c`, find this exact block:

```c
        struct sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int cfd = accept(sfd, (struct sockaddr *)&caddr, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            continue;
        }
```

Replace it with:

```c
        struct sockaddr_in caddr;
        socklen_t clen = sizeof(caddr);
        int cfd = accept(sfd, (struct sockaddr *)&caddr, &clen);
        if (cfd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
                continue;
            LOG_ERROR(LOG_SUB_SERVER, "accept() failed: errno=%d (%s)",
                      errno, strerror(errno));
            continue;
        }
```

### Regression test

None. This only adds a log line on an error path (unexpected accept() failure) that the test suite has no way to deterministically trigger (it would require exhausting file descriptors or another kernel-level accept() failure). Covered by the full test suite continuing to pass unchanged — every existing server-connection test exercises the success path (`cfd >= 0`) and the expected-errno retry path, neither of which this edit touches.

---

## Task 2 — `decode_field_to_buf` OOB read from corrupt schema (CID 1696469)

### The bug

`src/db/config.c`'s `decode_field_to_buf`, in its `FT_VARCHAR` case, computes `content_max = f->size - 2` without checking it can't go negative:

```c
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (len > content_max) len = content_max;  /* defensive */
        if (len == 0) return 0;
        if (buflen < 4) return -1;  /* "" + NUL */
        buf[0] = '"';
        int esc = json_escape_into(buf + 1, (size_t)buflen - 3,
                                   (const char *)(data + 2), (size_t)len);
```

`f->size` is the on-disk varchar field's declared byte width, loaded from `fields.conf`. On a corrupt or hand-edited schema file reporting `f->size < 2` for a varchar field, `content_max` becomes negative (e.g. `-2` when `f->size == 0`). `len` (decoded from the record's own 2-byte length prefix, range 0–65535) is always greater than a negative `content_max`, so the clamp `if (len > content_max) len = content_max;` sets `len` to that negative value. `len` is then passed as `(size_t)len` to `json_escape_into` — the cast turns e.g. `-2` into `SIZE_MAX - 1` (~1.8×10^19 on 64-bit), causing `json_escape_into` to read far past the end of `data`.

### The fix

In `src/db/config.c`, find this exact block:

```c
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (len > content_max) len = content_max;  /* defensive */
```

Replace it with:

```c
        int len = ((int)data[0] << 8) | (int)data[1];
        int content_max = f->size - 2;
        if (content_max < 0) content_max = 0;
        if (len > content_max) len = content_max;  /* defensive */
```

### Regression test

None. Triggering this requires a corrupt `fields.conf` (`size < 2` on a varchar field) — not reachable via the query protocol, only via direct on-disk tampering or a hand-edited config, which the existing test suite has no fixture for and which is out of scope to add here (this is a hardening fix, not a new feature needing coverage). Covered by the full test suite continuing to pass unchanged, since `content_max` is unaffected by this clamp for every valid schema (`f->size >= 2` always holds for any varchar field created through `create-object`/`add-field`, since the on-disk width is `content_size + 2` and content_size can't be negative).

---

## Task 3 — `compact_stream_worker` ignores `ftruncate` return on segment rotation (CID 1696470)

### The bug

`src/db/slotcask.c`'s `compact_stream_worker`, when rotating away from a full destination segment, calls `ftruncate` to shrink the segment down to its actual used size but never checks the return value:

```c
            if (!dest_ptr || dest_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest_ptr) {
                    munmap(dest_ptr, dest_alloc);
                    ftruncate(dest_fd, (off_t)dest_off);
                    close(dest_fd);
                    dest_ptr = NULL; dest_fd = -1;
                    dest_fid++;
                    dest_off = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); a->rc = -1; goto worker_fail; }
                if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0) {
                    close(fd); kfcache_release(&kh); a->rc = -1; goto worker_fail;
                }
```

Note that the *next* `ftruncate` call three lines below (growing the newly-opened segment to `SLOTCASK_SEG_MAX_BYTES`) is correctly checked — only the shrink-truncate on the segment being closed is unchecked. If that shrink fails (e.g. `ENOSPC`, `EIO`), the file is silently left at its old, oversized length instead of the correct used size, and compaction proceeds as if nothing went wrong.

### The fix

In `src/db/slotcask.c`, find this exact block:

```c
            if (!dest_ptr || dest_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest_ptr) {
                    munmap(dest_ptr, dest_alloc);
                    ftruncate(dest_fd, (off_t)dest_off);
                    close(dest_fd);
                    dest_ptr = NULL; dest_fd = -1;
                    dest_fid++;
                    dest_off = 0;
                }
```

Replace it with:

```c
            if (!dest_ptr || dest_off + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest_ptr) {
                    munmap(dest_ptr, dest_alloc);
                    dest_ptr = NULL;
                    if (ftruncate(dest_fd, (off_t)dest_off) < 0) {
                        close(dest_fd);
                        dest_fd = -1;
                        kfcache_release(&kh);
                        a->rc = -1;
                        goto worker_fail;
                    }
                    close(dest_fd);
                    dest_fd = -1;
                    dest_fid++;
                    dest_off = 0;
                }
```

**Note on anchor uniqueness**: `ftruncate(dest_fd, (off_t)dest_off);` occurs exactly once in this file (verified via `grep -c`) — `compact_stream_worker` is the only function using scalar `dest_fd`/`dest_ptr` variables (the sibling `slotcask_migrate_to_varlen`, fixed in Task 4, uses array-indexed `dest[sid].fd`/`dest[sid].base` instead, a textually distinct pattern).

`worker_fail:` (the existing label this function already uses for every other error path — see the `fd < 0` and grow-`ftruncate` checks immediately below in the same block) unconditionally does `if (dest_ptr) { munmap(dest_ptr, dest_alloc); close(dest_fd); }` before returning. Setting `dest_ptr = NULL` before the new check (mirroring this function's own existing style) prevents that generic cleanup from double-`munmap`ing the same region; explicitly closing and resetting `dest_fd` in the new error branch (mirroring the `fd < 0`/grow-failure branches right below, which also `close()` explicitly before `goto worker_fail`) prevents a leaked file descriptor. `kh` (the kfcache handle) is already in scope at this point — the new error branch must call `kfcache_release(&kh)` before `goto worker_fail`, matching the `fd < 0` and grow-`ftruncate` branches right below, which both release it; `worker_fail:` itself does not release `kh`, so omitting this call here would leak the kfcache handle.

### Regression test

None. Triggering an `ftruncate` failure requires an I/O error (`ENOSPC`/`EIO`/`EROFS` on the destination filesystem), which the test suite has no deterministic way to inject. Covered by the full test suite continuing to pass unchanged — the success path (`ftruncate` returning `0`) is exercised by every existing compact/reindex test and is untouched by this fix (the only new behavior is on the error path, which was previously silently ignored).

---

## Task 4 — `slotcask_migrate_to_varlen` ignores `ftruncate` return on segment rotation (CID 1696449)

### The bug

`src/db/slotcask.c`'s `slotcask_migrate_to_varlen` has the identical pattern to Task 3, in its own (non-worker, array-indexed) destination-rotation logic:

```c
            /* Open dest if needed or rotate when full. */
            if (!dest[sid].base ||
                dest_off[sid] + rec_size > SLOTCASK_SEG_MAX_BYTES) {
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    ftruncate(dest[sid].fd, (off_t)used);
                    close(dest[sid].fd);
                    dest[sid].base = NULL;
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
                char np[PATH_MAX];
                seg_path_for(np, db->data_dir, sid, dest_fid[sid]);
                { char d2[PATH_MAX]; snprintf(d2, sizeof(d2), "%s", np);
                  char *sl = strrchr(d2, '/'); if (sl) { *sl = '\0'; mkdirp_local(d2); } }
                int fd = open(np, O_RDWR | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) { kfcache_release(&kh); goto fail; }
                if (ftruncate(fd, (off_t)SLOTCASK_SEG_MAX_BYTES) < 0)
                    { close(fd); kfcache_release(&kh); goto fail; }
```

Same defect: the shrink-`ftruncate` on the segment being closed (`ftruncate(dest[sid].fd, (off_t)used);`) is unchecked, while the grow-`ftruncate` on the newly-opened segment three lines below is checked.

### The fix

In `src/db/slotcask.c`, find this exact block:

```c
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    ftruncate(dest[sid].fd, (off_t)used);
                    close(dest[sid].fd);
                    dest[sid].base = NULL;
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
```

Replace it with:

```c
                if (dest[sid].base) {
                    size_t used = dest_off[sid];
                    munmap(dest[sid].base, dest[sid].alloc);
                    dest[sid].base = NULL;
                    if (ftruncate(dest[sid].fd, (off_t)used) < 0) {
                        close(dest[sid].fd);
                        dest[sid].fd = -1;
                        kfcache_release(&kh);
                        goto fail;
                    }
                    close(dest[sid].fd);
                    dest[sid].fd   = -1;
                    dest_fid[sid]++;
                    dest_off[sid] = 0;
                }
```

**Note on anchor uniqueness**: `ftruncate(dest[sid].fd, (off_t)used);` occurs exactly once in this file (verified via `grep -c`).

`fail:` (this function's own existing label, used by the `fd < 0`/grow-`ftruncate` checks immediately below in the same block) iterates every stream's `dest[s]`/`src[s]` and does `if (dest[s].base) { munmap(dest[s].base, dest[s].alloc); close(dest[s].fd); }` before returning `-1`. Setting `dest[sid].base = NULL` before the new check prevents that generic cleanup from double-`munmap`ing; explicitly closing and resetting `dest[sid].fd` in the new error branch (mirroring the `fd < 0`/grow-failure branches right below, which also `close()` explicitly before `goto fail`) prevents a leaked file descriptor. `kh` (the kfcache handle) is already in scope at this point in the loop, same as the two adjacent existing error branches.

### Regression test

None — same rationale as Task 3: an `ftruncate` I/O failure can't be deterministically injected by the test suite, and the success path is unchanged and already covered by every existing migrate-to-varlen test.

---

## Task 5 — `cmd_add_indexes` copies uninitialized stack rows via oversized `memcpy` (CID 1696418)

### The bug

`src/db/index.c`'s `cmd_add_indexes` accumulates btree-typed field names into `btree_fields[MAX_FIELDS][256]`, but only the first `btree_count` rows are ever written (bitmap/trigram-typed fields are `continue`d past without writing a row) — yet the final copy into `fields` uses `sizeof(btree_fields)`, the full `MAX_FIELDS`-row size, regardless of how many rows were actually populated:

```c
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, sizeof(btree_fields));
    nfields = btree_count;
```

`btree_count <= nfields <= MAX_FIELDS`, and is frequently `< MAX_FIELDS` for any schema with even one bitmap or trigram field. The `memcpy` above reads `MAX_FIELDS * 256` bytes from `btree_fields` regardless, including the trailing rows that were never written by the preceding loop — i.e. it copies uninitialized stack memory into `fields`. In practice this is masked from ever being read back out, since `nfields = btree_count;` (the line right after) means only the first `btree_count` rows of `fields` are ever iterated downstream — but the read of uninitialized memory during the `memcpy` itself is exactly what Coverity flags, and it's also strictly wasted work (`MAX_FIELDS * 256` bytes copied instead of `btree_count * 256`).

### The fix

In `src/db/index.c`, find this exact block:

```c
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, sizeof(btree_fields));
    nfields = btree_count;
```

Replace it with:

```c
        memcpy(btree_fields[btree_count], names[i], 256);
        btree_count++;
    }
    memcpy(fields, btree_fields, (size_t)btree_count * sizeof(btree_fields[0]));
    nfields = btree_count;
```

### Regression test

None. The set of rows actually read back out of `fields` (indices `0` .. `nfields - 1`, where `nfields` is set to `btree_count` on the very next line) is unchanged by this fix — only the number of bytes physically copied by the `memcpy` changes, from `MAX_FIELDS * 256` down to `btree_count * 256`. Covered by the full test suite continuing to pass unchanged; every existing add-index test (btree-only, mixed btree+bitmap, mixed btree+trigram, force-rebuild) already exercises this function's downstream behavior via `fields`/`nfields`, which is identical before and after this fix.

---

## Final verification

After all five tasks are complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** showing `# total: N passed, 0 failed`, with N unchanged from the pre-fix baseline (no new test cases were added in this plan). Do not consider this plan done, and do not report success, without pasting this real output.
