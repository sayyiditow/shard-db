# Coverity: outstanding round 2 (post-fix rescan, 54 CIDs)

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-round2 main`.
- Do the eight tasks below **in order**. They touch eight independent files/functions — no ordering dependency between them, but do them in order anyway for a clean, reviewable diff.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all tasks are done: `./build/bin/shard-db-test run-all`.
- Every insertion/edit below is anchored on **quoted exact text** from the current source (verified against `main` at commit `f4333e0` / after PR #228). If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all` and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done.

## Background

This is round 2 of Coverity triage: a fresh scan taken after the round-1 fixes (PRs #220–#227: disk-corruption, null-deref-registry, build-warnings, resource-leaks, data-races, dead-code-cleanup, perf/btcache-miss-path, misc-hardening) still reports 54 outstanding CIDs. Of those 54:

- **8 CIDs are already fixed in source** (verified via explicit `CID <n>` citation comments already present at the fix site) but Coverity's dashboard hasn't cleared them yet — no action needed here, they should drop out on the next scan after this branch merges. List: 1696471, 1696413, 1696466, 1696465, 1696451, 1696448, 1696430, 1696403.
- **2 CIDs were already fixed by very recent commits** on `main` (`bbdbd75`, `40b1517`) that this scan predates or that Coverity hasn't re-indexed yet: 1696472 (`bulk_ins_run` wire_keys leak) and 1696469 (`decode_field_to_buf` OOB read). No action needed.
- **2 CIDs (1696409 `cmd_exists_multi`, 1696460 `cmd_not_exists`) are an intentionally accepted risk**, not a fix target: the code at both sites already carries an explicit comment (`Coverity CID 1693843` / `CID 1693844` — old CID numbers from an earlier scan of the same code) documenting that the OOM-leak-on-realloc-failure is acceptable. This round's scan re-flagged the same lines under new CID numbers. Recommendation: mark these two as non-issues in the dashboard (see the companion false-positive list) rather than touch code that's already a deliberate, documented trade-off.
- **1 CID (1696482, `run_startup_migration` TOCTOU)** is accepted risk at Low severity — a stat-then-open pattern in single-threaded startup migration code. Not touched by this plan; mark as acknowledged/won't-fix in the dashboard, not as a false positive (it's a real theoretical TOCTOU, just not worth the complexity of an atomic open-and-check rewrite for a one-time startup path).
- **~32 CIDs are false positives** — data races that are actually write-once-before-threads-start config reads, double-checked-locking idioms already proven safe elsewhere in this codebase, and null-check/bounds patterns Coverity's flow analysis loses track of. These are listed separately for direct marking in the Coverity dashboard (not part of this code-fix plan).
- **8 CIDs below are true positives** requiring an actual code fix — that's what this plan covers.

---

## Task 1 — `selective_reindex_dirty` stack buffer overflow (CID 1696444)

### The bug

`src/db/query_schema.c`'s `selective_reindex_dirty` collects index.conf lines that reference a dirty field into a fixed-size stack array `affected_specs[MAX_FIELDS][256]`, but never checks `n_aff` against `MAX_FIELDS` (256) before writing `affected_specs[n_aff]`. Since `index.conf` can accumulate more than 256 lines over an object's lifetime (repeated `add-index` calls), a sufficiently indexed object triggers a stack buffer overflow here.

Current code:

```c
    char affected_specs[MAX_FIELDS][256];
    int n_aff = 0;
    char line[512];
    while (fgets(line, sizeof(line), ic)) {
        line[strcspn(line, "\n")] = '\0';
        if (!line[0]) continue;
        /* Index spec form: <fname>[+<fname>...][':' index-type-tail].
           Match any '+'-separated token against dirty_names[]. */
        int hit = 0;
        const char *p = line;
        while (*p && !hit) {
            const char *next = p;
            while (*next && *next != '+' && *next != ':') next++;
            size_t toklen = (size_t)(next - p);
            for (int d = 0; d < n_dirty; d++) {
                size_t dn = strlen(dirty_names[d]);
                if (toklen == dn && memcmp(p, dirty_names[d], dn) == 0) { hit = 1; break; }
            }
            if (*next == '+') p = next + 1;
            else break;
        }
        if (hit) {
            strncpy(affected_specs[n_aff], line, sizeof(affected_specs[0]) - 1);
            affected_specs[n_aff][sizeof(affected_specs[0]) - 1] = '\0';
            n_aff++;
        } else (*out_skipped)++;
    }
    fclose(ic);
```

### The fix

Find this exact block in `src/db/query_schema.c`:

```c
        if (hit) {
            strncpy(affected_specs[n_aff], line, sizeof(affected_specs[0]) - 1);
            affected_specs[n_aff][sizeof(affected_specs[0]) - 1] = '\0';
            n_aff++;
        } else (*out_skipped)++;
```

Replace it with:

```c
        if (hit && n_aff < MAX_FIELDS) {
            strncpy(affected_specs[n_aff], line, sizeof(affected_specs[0]) - 1);
            affected_specs[n_aff][sizeof(affected_specs[0]) - 1] = '\0';
            n_aff++;
        } else (*out_skipped)++;
```

### Regression test

None practical — reproducing requires an object with >256 index.conf entries (`MAX_FIELDS` is also the ceiling on indexes per object, so this is close to the practical maximum already). Covered by the full test suite continuing to pass unchanged; the `n_aff < MAX_FIELDS` guard is a no-op on every existing test fixture (all well under 256 indexes).

---

## Task 2 — `cmd_aggregate_do` negative array index read (CID 1696426)

### The bug

`src/db/query_aggregate.c`'s `cmd_aggregate_do`, in the no-criteria/no-group-by fast path, resolves each non-count agg spec's field to a `TypedField` without checking whether `typed_field_index` returned -1:

```c
            int processed[MAX_AGG_SPECS] = {0};
            int n_idx = index_splits_for(sch.splits);
            for (int i = 0; i < nspecs && has_noncount; i++) {
                if (processed[i] || specs[i].fn == AGG_COUNT) continue;
                const char *fld = specs[i].field;
                int fi = typed_field_index(fs.ts, fld);
                const TypedField *tf = &fs.ts->fields[fi];
```

If `fi` is -1 (field not found — e.g. a race with a concurrent `remove-field`, or a spec that slipped past the earlier `validate_field` call under different criteria), `fs.ts->fields[-1]` is a negative-index read, and every subsequent use of `tf` is undefined behavior.

### The fix

Find this exact block in `src/db/query_aggregate.c`:

```c
                const char *fld = specs[i].field;
                int fi = typed_field_index(fs.ts, fld);
                const TypedField *tf = &fs.ts->fields[fi];
```

Replace it with:

```c
                const char *fld = specs[i].field;
                int fi = typed_field_index(fs.ts, fld);
                if (fi < 0) { processed[i] = 1; continue; }
                const TypedField *tf = &fs.ts->fields[fi];
```

### Regression test

None practical — this fast path is only reachable when `validate_field` already passed for every spec earlier in the same function call, so triggering `fi < 0` here requires a schema mutation racing between the two checks. Covered by the full suite continuing to pass unchanged (no behavior change when `fi >= 0`, which is every existing test case).

---

## Task 3 — `keyset_emit_find` NULL join-buffer dereference (CID 1696442)

### The bug

`src/db/query.c`'s `keyset_emit_find`, in the per-record join-resolution block, allocates two arrays sized by `njoins` and uses them immediately without checking either `calloc` for failure:

```c
                RecordRef *jrr = NULL;
                const uint8_t **jraws = NULL;
                int dropped = 0;
                if (njoins > 0) {
                    jrr = calloc(njoins, sizeof(RecordRef));
                    jraws = calloc(njoins, sizeof(const uint8_t *));
                    for (int i = 0; i < njoins; i++) {
                        char lk[1024];
                        int llen = extract_local_key(&joins[i], raw,
                                                     fs ? fs->ts : NULL, lk, sizeof(lk));
                        int jfound = 0;
                        if (llen > 0) {
                            jfound = lookup_remote(&joins[i], db_root, lk, (size_t)llen,
                                                   &jrr[i]);
                            if (jfound) jraws[i] = jrr[i].val;
                        }
                        if (!jfound && joins[i].type == JOIN_INNER) { dropped = 1; break; }
                    }
                }
```

On OOM, `jrr[i]` / `jraws[i]` writes inside the loop dereference a NULL pointer.

### The fix

Find this exact block in `src/db/query.c`:

```c
                RecordRef *jrr = NULL;
                const uint8_t **jraws = NULL;
                int dropped = 0;
                if (njoins > 0) {
                    jrr = calloc(njoins, sizeof(RecordRef));
                    jraws = calloc(njoins, sizeof(const uint8_t *));
                    for (int i = 0; i < njoins; i++) {
                        char lk[1024];
                        int llen = extract_local_key(&joins[i], raw,
                                                     fs ? fs->ts : NULL, lk, sizeof(lk));
                        int jfound = 0;
                        if (llen > 0) {
                            jfound = lookup_remote(&joins[i], db_root, lk, (size_t)llen,
                                                   &jrr[i]);
                            if (jfound) jraws[i] = jrr[i].val;
                        }
                        if (!jfound && joins[i].type == JOIN_INNER) { dropped = 1; break; }
                    }
                }
```

Replace it with:

```c
                RecordRef *jrr = NULL;
                const uint8_t **jraws = NULL;
                int dropped = 0;
                if (njoins > 0) {
                    jrr = calloc(njoins, sizeof(RecordRef));
                    jraws = calloc(njoins, sizeof(const uint8_t *));
                    if (!jrr || !jraws) {
                        free(jrr); free(jraws);
                        jrr = NULL; jraws = NULL;
                        dropped = 1;
                    } else {
                        for (int i = 0; i < njoins; i++) {
                            char lk[1024];
                            int llen = extract_local_key(&joins[i], raw,
                                                         fs ? fs->ts : NULL, lk, sizeof(lk));
                            int jfound = 0;
                            if (llen > 0) {
                                jfound = lookup_remote(&joins[i], db_root, lk, (size_t)llen,
                                                       &jrr[i]);
                                if (jfound) jraws[i] = jrr[i].val;
                            }
                            if (!jfound && joins[i].type == JOIN_INNER) { dropped = 1; break; }
                        }
                    }
                }
```

This is safe with the existing cleanup further down the function (`if (jrr) { for (...) release_record_ref(&jrr[i]); free(jrr); free(jraws); }`) since `jrr` is left NULL on the OOM path.

### Regression test

None practical — forcing this specific `calloc` to fail requires a fault-injection harness this codebase doesn't have. Covered by the full suite continuing to pass unchanged; every existing join test exercises the non-OOM path (`jrr`/`jraws` both non-NULL), which is untouched by this change.

---

## Task 4 — `seq_next_val_batch` signed overflow (CID 1696422)

### The bug

`src/db/config.c`'s `seq_next_val_batch` computes `val += n` without checking for signed 64-bit overflow:

```c
    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    long long start = val + 1;
    val += n;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
```

A sequence file near `LLONG_MAX` (only reachable after an astronomical number of allocations, but Coverity flags the missing guard regardless) silently wraps to a negative value on the next batch request.

### The fix

Find this exact block in `src/db/config.c`:

```c
    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    long long start = val + 1;
    val += n;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
```

Replace it with:

```c
    long long val = 0;
    FILE *f = fopen(seq_path, "r");
    if (f) { if (fscanf(f, "%lld", &val) != 1) val = 0; fclose(f); }
    if (val < 0 || (long long)n > LLONG_MAX - val - 1) {
        flock(lockfd, LOCK_UN);
        close(lockfd);
        return -1;
    }
    long long start = val + 1;
    val += n;
    f = fopen(seq_path, "w");
    if (f) { fprintf(f, "%lld\n", val); fclose(f); }

    flock(lockfd, LOCK_UN);
    close(lockfd);
    return start;
```

`LLONG_MAX` is available via `types.h`'s existing transitive `<limits.h>` include — no new `#include` needed in `config.c`.

### Regression test

None practical — requires a sequence file already at `~LLONG_MAX`. Covered by the full suite continuing to pass unchanged; every existing sequence test starts from 0 or a small value, so `val < 0 || n > LLONG_MAX - val - 1` is always false on those paths.

---

## Task 5 — `bm_mkdir_p` / `bm_open` unchecked `mkdir` (CID 1696436, CID 1696440)

### The bug

`src/db/bitmap.c`'s `bm_mkdir_p` ignores every `mkdir` return value, and its only caller (`bm_open`) discards `bm_mkdir_p`'s own return value too — so a directory-creation failure (permissions, disk full, `ENOTDIR` from a path component that's actually a file) is silently swallowed, and the subsequent `stat`/`bm_write_initial` call fails with a less diagnostic error later.

Current `bm_mkdir_p`:

```c
static int bm_mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755);
}
```

Current caller in `bm_open`:

```c
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; bm_mkdir_p(dir); }
```

### The fix

`src/db/bitmap.c` has no existing `errno` usage — add `#include <errno.h>` near the top of the file. Find this exact line:

```c
#define _GNU_SOURCE
#include "types.h"
#include "bitmap.h"
```

Replace it with:

```c
#define _GNU_SOURCE
#include "types.h"
#include "bitmap.h"
#include <errno.h>
```

Find this exact block (the `bm_mkdir_p` definition):

```c
static int bm_mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755);
}
```

Replace it with:

```c
static int bm_mkdir_p(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, 0755) != 0 && errno != EEXIST) return -1;
    return 0;
}
```

Find this exact block in `bm_open`:

```c
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; bm_mkdir_p(dir); }
```

Replace it with:

```c
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", path);
    char *slash = strrchr(dir, '/');
    if (slash) { *slash = '\0'; if (bm_mkdir_p(dir) != 0) return NULL; }
```

### Regression test

None practical — requires simulating a `mkdir` failure (permissions/disk-full), not doable deterministically in this suite. Covered by the full suite continuing to pass unchanged: every existing test creates bitmap shards under a writable tmpdir, so `mkdir` always succeeds or returns `EEXIST` (already-created parent dirs from a prior shard in the same object), both of which are treated as success by the new code, identical to today's silent-success behavior.

---

## Task 6 — `od_open` unchecked `posix_fadvise` (CID 1696454)

### The bug

`src/db/io_direct.c`'s `od_open` calls `posix_fadvise` twice in two different branches without checking (or even casting away) the return value, unlike the sibling macOS branch a few lines above which explicitly casts `fcntl`'s return to `(void)` with a "best-effort" comment:

```c
#if defined(__APPLE__)
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
    (void)fcntl(fd, F_NOCACHE, 1);   /* best-effort; ignore failures */
    return fd;

#elif defined(O_DIRECT) && O_DIRECT != 0
    fd = open(path, O_RDONLY | O_DIRECT);
    if (fd >= 0) return fd;
    /* EINVAL — filesystem doesn't support O_DIRECT (tmpfs, overlayfs …).
       Fall back to buffered + fadvise. */
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
#  ifdef POSIX_FADV_SEQUENTIAL
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#  endif
    return fd;

#else
    fd = open(path, O_RDONLY);
    if (fd < 0) return -1;
#  ifdef POSIX_FADV_SEQUENTIAL
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
#  endif
    return fd;
#endif
```

### The fix

Both occurrences of the two-line `posix_fadvise` pair are textually identical — use a `replace_all` edit. Find this exact two-line block (appears twice in the file):

```c
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
```

Replace **every occurrence** with:

```c
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL); /* best-effort; ignore failures */
    (void)posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);   /* best-effort; ignore failures */
```

### Regression test

None — purely a cosmetic cast + comment; no behavior change. Covered by the full suite continuing to pass unchanged.

---

## Task 7 — `slotcask_bulk_resolve_and_fetch` leaks `resolved` when `resolved_n == 0` (CID 1696408)

### The bug

`src/db/slotcask.c`'s `slotcask_bulk_resolve_and_fetch` frees `resolved` on the normal-completion path but not on the early return that fires when `resolved` is non-NULL yet `resolved_n` is 0 (e.g. `slotcask_bulk_resolve_hashes` allocated its output array but every hash resolved to nothing):

```c
int slotcask_bulk_resolve_and_fetch(SlotcaskDb *db,
                                     const uint8_t (*hashes)[16],
                                     size_t n,
                                     void *ctx,
                                     SlotcaskScanCb cb) {
    size_t resolved_n = 0;
    SlotcaskResolvedRec *resolved = slotcask_bulk_resolve_hashes(db, hashes, n, &resolved_n);
    if (!resolved || resolved_n == 0) return 0;
    int rc = slotcask_bulk_fetch_resolved(db, resolved, resolved_n, ctx, cb);
    free(resolved);
    return rc;
}
```

### The fix

Find this exact block in `src/db/slotcask.c`:

```c
    size_t resolved_n = 0;
    SlotcaskResolvedRec *resolved = slotcask_bulk_resolve_hashes(db, hashes, n, &resolved_n);
    if (!resolved || resolved_n == 0) return 0;
    int rc = slotcask_bulk_fetch_resolved(db, resolved, resolved_n, ctx, cb);
    free(resolved);
    return rc;
```

Replace it with:

```c
    size_t resolved_n = 0;
    SlotcaskResolvedRec *resolved = slotcask_bulk_resolve_hashes(db, hashes, n, &resolved_n);
    if (!resolved || resolved_n == 0) { free(resolved); return 0; }
    int rc = slotcask_bulk_fetch_resolved(db, resolved, resolved_n, ctx, cb);
    free(resolved);
    return rc;
```

(`free(NULL)` is a no-op, so this is safe regardless of which half of the `||` fired.)

### Regression test

None practical — requires a batch where every hash resolves to nothing but the allocator still returns a non-NULL zero-length array, an internal detail of `slotcask_bulk_resolve_hashes` not directly controllable from a test. Covered by the full suite continuing to pass unchanged.

---

## Task 8 — `index_parallel` unreachable dead code (CID 1696484)

### The bug

`src/db/index.c`'s `index_parallel`, in the composite-index token loop, has an `if (!all_present) break;` check that can never fire: every code path that sets `all_present = 0` already `break`s out of the loop immediately at the point of assignment, so control never reaches this line with `all_present == 0`.

```c
                if (!txt || txt[0] == '\0') { all_present = 0; break; }
                if (fidx >= 0) {
                    const TypedField *f = &ts->fields[fidx];
                    size_t blen = 0;
                    encode_field_for_index(f, txt, strlen(txt),
                                            (uint8_t *)result + pos, &blen);
                    if (blen == 0) { all_present = 0; break; }
                    if (pos + (int)blen < (int)sizeof(result)) { pos += (int)blen; }
                    else { all_present = 0; break; }
                } else {
                    /* No schema — pass raw bytes */
                    int len = strlen(txt);
                    if (pos + len < (int)sizeof(result)) {
                        memcpy(result + pos, txt, len);
                        pos += len;
                    }
                }
                if (!all_present) break;
                tok = strtok_r(NULL, "+", &_tok_save);
```

### The fix

Find this exact line in `src/db/index.c`:

```c
                if (!all_present) break;
                tok = strtok_r(NULL, "+", &_tok_save);
```

Replace it with:

```c
                tok = strtok_r(NULL, "+", &_tok_save);
```

### Regression test

None needed — this is a pure dead-code deletion with no behavioral effect (the line is provably unreachable). Covered by the full suite continuing to pass unchanged.

---

## Optional bonus fix (not one of the 54 CIDs) — `bulk_ins_delim_run` wire_keys leak

While auditing `bulk_ins_run` (Task target of the already-fixed CID 1696472), its sibling function `bulk_ins_delim_run` (same file, delimited-format variant, starts a few hundred lines later) has the **identical** wire_keys-leak shape at all three of its own OOM/timeout bail sites — but Coverity did not flag it (only `bulk_ins_run` appears in the 54-CID list). Since it's the same bug class and a one-line-per-site mirror of the already-applied fix, consider fixing it opportunistically in a follow-up — not included in this plan's required tasks to keep this plan's scope matched to the 54 CIDs under triage.

---

## Already fixed in source — no action, awaiting Coverity re-scan

| CID | File / function | Evidence |
|---|---|---|
| 1696471 | slotcask.c `recover_scan_tombstones_od` | citation comment at slotcask.c:2388 |
| 1696413 | query.c `find_via_composite_prefix` | citation comment at query.c:364 |
| 1696466 | io_direct.c `seg_scan_o_direct_varlen` | citation comment at io_direct.c:758 |
| 1696465 | btree.c `btree_walk_all_values` | citation comment at btree.c:2216 |
| 1696451 | index.c `reindex_seg_cb` | citation comment at index.c:2504 |
| 1696448 | btree.c `iter_init_desc_leaves` | citation comments at btree.c:2012, 2239 |
| 1696430 | bitmap.c `bm_dict_add` | citation comment at bitmap.c:564 |
| 1696403 | bitmap.c `bm_dict_used_bytes` | citation comment at bitmap.c:249 |
| 1696472 | query_bulk.c `bulk_ins_run` | fixed by commit `bbdbd75` |
| 1696469 | config.c `decode_field_to_buf` | fixed by commit `40b1517` |

## Accepted risk — mark acknowledged in dashboard, not a code-fix target

| CID | File / function | Rationale |
|---|---|---|
| 1696409 | storage.c `cmd_exists_multi` | Existing in-code comment documents this OOM-leak-on-realloc-failure as an accepted trade-off (previously reviewed under old CID 1693843). |
| 1696460 | storage.c `cmd_not_exists` | Same pattern, same existing comment (old CID 1693844). |
| 1696482 | embedded.c `run_startup_migration` | TOCTOU (stat-then-open) in single-threaded startup migration; Low severity; not worth an atomic-open rewrite for a one-time startup path. |
