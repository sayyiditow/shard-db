# Coverity: hardening against untrusted on-disk data

## Execution rules (read first)

- Branch off `main`: `git checkout -b fix/coverity-disk-corruption main`.
- Do the tasks below **in order**. Each task is self-contained; nothing later depends on earlier code, but doing them in order keeps the diff easy to review incrementally.
- Build with `SKIP_TESTS=1 ./build.sh` after each task to catch compile errors early. Run the full suite only after all tasks are done.
- Every insertion/edit below is anchored on **quoted exact text** from the current source. If a quoted anchor is not found verbatim in the file, STOP — do not guess, do not reinterpret, do not "fix it forward." Instead write `docs/plans/PLAN_NOTES.md` describing exactly what you searched for and what you found instead, and stop working on this plan.
- Leave all work **uncommitted** when done. Do not run `git add`, `git commit`, `git push`, or open a PR — that happens outside this workflow, after human review.
- After the last task, run `./build/bin/shard-db-test run-all --filter coverity` and also `./build/bin/shard-db-test run-all` (full suite), and paste the **real terminal output** (not a paraphrase) showing `# total: N passed, 0 failed` before considering this plan done. If any test fails, do not modify the test to make it pass — the test encodes the bug; a failing test after your fix means the fix is incomplete or wrong. Stop and report.

## Background

This plan covers 12 Coverity findings that share one theme: **on-disk bytes are untrusted input**. Segment-file headers, B+tree page headers, bitmap dictionary headers, and slotcask stream-migration source files are all read directly via mmap/O_DIRECT and their length/count/pointer fields are used as unchecked bounds for subsequent pointer arithmetic, `memcpy`, or loop iteration. A corrupted file (disk bitrot, a bug elsewhere, a crash mid-write, or a malicious operator with filesystem access) can turn any of these into an out-of-bounds read, a crash, or in the worst case a controlled overflow. Full triage context: `docs/coverity-triage-2026-07.md`.

Two things found during investigation are **explicitly out of scope** for this plan and are noted here for visibility rather than silently dropped:

1. `io_direct.c`'s `seg_scan_o_direct_varlen` Stage 2 block has a second, structurally similar `chunk`/`chunk + pos` offset issue near the same carry-buffer logic being fixed under CID 1696466 below. Constructing a byte-perfect test that forces both a Stage-1 header-completion carry AND a Stage-2 payload-continuation carry to land on the same chunk boundary is disproportionately complex for a non-triaged, currently-unreachable-in-content case. Not fixed here — flagging so it isn't forgotten.
2. CID 1696430 (`bm_dict_add` missing a `vlen > 0xffff` guard) is fixed here as defense-in-depth, but is **not independently reachable today**: varchar's on-disk max content (65535 bytes, from its uint16 length prefix) exactly coincides with `BM_HARD_CEILING` (65535) in `bitmap.h`, so no legitimately-encoded value can ever reach `bm_dict_add` with `vlen > 0xffff`. No dedicated test is added for it; the fix is covered by the existing full suite continuing to pass.

Test strategy across the 12 CIDs: 8 get new dedicated regression tests (grouped into 4 new test files by mechanism), 4 rely on the existing full test suite continuing to pass (rationale given per-CID below — either currently unreachable, or no safe/deterministic way to trigger the failure mode in a test).

---

## Task 1 — `slotcask_migrate_to_varlen` uninitialized `dest[]` on early `goto fail` (CID 1696419)

### The bug

`src/db/slotcask.c`, inside `slotcask_migrate_to_varlen`, declares `DestMap dest[SLOTCASK_MAX_STREAMS]` and zero-initializes it **after** the loop that can `goto fail` on allocation failure:

```c
       for (int s = 0; s < n_streams; s++) {
           uint32_t cnt = 0;
           for (;;) {
               char p[PATH_MAX];
               seg_path_for(p, db->data_dir, s, cnt);
               if (access(p, F_OK) != 0) break;
               cnt++;
           }
           src_cnt[s] = cnt;
           if (cnt == 0) continue;
           src[s] = calloc((size_t)cnt, sizeof(SrcMap));
           if (!src[s]) goto fail;
           for (uint32_t f = 0; f < cnt; f++) {
               char p[PATH_MAX];
               seg_path_for(p, db->data_dir, s, f);
               int fd = open(p, O_RDONLY);
               if (fd < 0) continue;
               struct stat st;
               if (fstat(fd, &st) == 0 && st.st_size > 0) {
                   void *m = mmap(NULL, (size_t)st.st_size,
                                  PROT_READ, MAP_SHARED, fd, 0);
                   if (m != MAP_FAILED) {
                       src[s][f].base = (uint8_t *)m;
                       src[s][f].sz   = (size_t)st.st_size;
                   }
               }
               close(fd);
           }
       }

       /* Per-stream dest segment state (one open mmap at a time per stream). */
       typedef struct { uint8_t *base; size_t alloc; int fd; } DestMap;
       DestMap dest[SLOTCASK_MAX_STREAMS];
       memset(dest, 0, sizeof(dest));
       for (int s = 0; s < n_streams; s++) dest[s].fd = -1;
       uint32_t dest_fid[SLOTCASK_MAX_STREAMS];
       size_t   dest_off[SLOTCASK_MAX_STREAMS];
       for (int s = 0; s < n_streams; s++) {
           dest_fid[s] = MIGRATE_STREAM_BASE + (uint32_t)s * 1000u;
           dest_off[s] = 0;
       }
```

If `calloc` fails for `src[s]` on some stream `s`, the code jumps to the `fail:` label. The `fail:` cleanup path (further down in the function) iterates `dest[]` to close/unmap any open destination segments — but at that point `dest[]` has never been declared or zeroed, because its declaration textually comes *after* the loop that can jump away. This is a use of uninitialized stack memory: the cleanup code reads garbage `fd`/`base` values and may `close()`/`munmap()` bogus values.

### The fix

Move the `DestMap dest[...]` declaration, its `memset`, the `fd = -1` initialization loop, and the `dest_fid`/`dest_off` declarations to **before** the per-stream source-scanning loop, so they're guaranteed initialized before any possible `goto fail`.

In `src/db/slotcask.c`, find this exact block:

```c
    memset(src, 0, sizeof(src));
    memset(src_cnt, 0, sizeof(src_cnt));

    for (int s = 0; s < n_streams; s++) {
        uint32_t cnt = 0;
        for (;;) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, cnt);
            if (access(p, F_OK) != 0) break;
            cnt++;
        }
        src_cnt[s] = cnt;
        if (cnt == 0) continue;
        src[s] = calloc((size_t)cnt, sizeof(SrcMap));
        if (!src[s]) goto fail;
        for (uint32_t f = 0; f < cnt; f++) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, f);
            int fd = open(p, O_RDONLY);
            if (fd < 0) continue;
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size > 0) {
                void *m = mmap(NULL, (size_t)st.st_size,
                               PROT_READ, MAP_SHARED, fd, 0);
                if (m != MAP_FAILED) {
                    src[s][f].base = (uint8_t *)m;
                    src[s][f].sz   = (size_t)st.st_size;
                }
            }
            close(fd);
        }
    }

    /* Per-stream dest segment state (one open mmap at a time per stream). */
    typedef struct { uint8_t *base; size_t alloc; int fd; } DestMap;
    DestMap dest[SLOTCASK_MAX_STREAMS];
    memset(dest, 0, sizeof(dest));
    for (int s = 0; s < n_streams; s++) dest[s].fd = -1;
    uint32_t dest_fid[SLOTCASK_MAX_STREAMS];
    size_t   dest_off[SLOTCASK_MAX_STREAMS];
    for (int s = 0; s < n_streams; s++) {
        dest_fid[s] = MIGRATE_STREAM_BASE + (uint32_t)s * 1000u;
        dest_off[s] = 0;
    }
```

Replace it with:

```c
    memset(src, 0, sizeof(src));
    memset(src_cnt, 0, sizeof(src_cnt));

    /* Per-stream dest segment state (one open mmap at a time per
       stream). Declared and zeroed before the source-scanning loop
       below so that if that loop `goto fail`s partway through (e.g.
       a calloc failure), the fail: cleanup path finds a fully
       zero-initialized dest[] instead of uninitialized stack memory
       (CID 1696419). */
    typedef struct { uint8_t *base; size_t alloc; int fd; } DestMap;
    DestMap dest[SLOTCASK_MAX_STREAMS];
    memset(dest, 0, sizeof(dest));
    for (int s = 0; s < n_streams; s++) dest[s].fd = -1;
    uint32_t dest_fid[SLOTCASK_MAX_STREAMS];
    size_t   dest_off[SLOTCASK_MAX_STREAMS];
    for (int s = 0; s < n_streams; s++) {
        dest_fid[s] = MIGRATE_STREAM_BASE + (uint32_t)s * 1000u;
        dest_off[s] = 0;
    }

    for (int s = 0; s < n_streams; s++) {
        uint32_t cnt = 0;
        for (;;) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, cnt);
            if (access(p, F_OK) != 0) break;
            cnt++;
        }
        src_cnt[s] = cnt;
        if (cnt == 0) continue;
        src[s] = calloc((size_t)cnt, sizeof(SrcMap));
        if (!src[s]) goto fail;
        for (uint32_t f = 0; f < cnt; f++) {
            char p[PATH_MAX];
            seg_path_for(p, db->data_dir, s, f);
            int fd = open(p, O_RDONLY);
            if (fd < 0) continue;
            struct stat st;
            if (fstat(fd, &st) == 0 && st.st_size > 0) {
                void *m = mmap(NULL, (size_t)st.st_size,
                               PROT_READ, MAP_SHARED, fd, 0);
                if (m != MAP_FAILED) {
                    src[s][f].base = (uint8_t *)m;
                    src[s][f].sz   = (size_t)st.st_size;
                }
            }
            close(fd);
        }
    }
```

Note: this moves code verbatim — no logic changes, just reordering the two blocks. `n_streams` is already assigned earlier in the function (`int n_streams = db->num_streams;`) so it's available at the new, earlier location.

### Regression test

No new dedicated test. Reaching the `goto fail` path requires injecting a `calloc` failure (OOM simulation), for which this codebase has no fault-injection mechanism, and building one solely for this one-line reordering fix would be disproportionate. The existing `test-variable-length` case (`src/test/cases/test_variable_length.c`) already exercises the non-failure path of `slotcask_migrate_to_varlen` end-to-end and continues to serve as a regression guard against breaking that path while reordering these declarations. Verify by running:

```
./build/bin/shard-db-test run test-variable-length
```

---

## Task 2 — `recover_scan_tombstones_od` carry-buffer offset/bound bug (CID 1696471)

### The bug

`src/db/slotcask.c`, inside `recover_scan_tombstones_od`'s VARLEN branch, reassembles a record header/body that straddles two O_DIRECT read chunks using a `carry` buffer. The "not enough bytes yet" branch has a coupled bug: it copies from `buf` (not `buf + pos`) and compares/sizes against the *full* chunk size `nr` instead of the *remaining unconsumed* bytes (`nr - pos`), where `pos` may already be nonzero because Stage 1 (header completion) already consumed some bytes from this same chunk:

```c
                uint16_t klen; memcpy(&klen, carry + 16, 2);
                uint32_t vlen; memcpy(&vlen, carry + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                int need2 = (int)rec_size - carry_len;
                if (need2 > 0) {
                    if ((ssize_t)need2 > nr) {
                        if ((size_t)(carry_len + nr) > carry_cap) {
                            carry_cap = (size_t)(carry_len + nr);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf, (size_t)nr);
                        carry_len += (int)nr;
                        file_off2 += nr; continue;
                    }
                    if (rec_size > carry_cap) {
                        carry_cap = rec_size;
                        uint8_t *nc = realloc(carry, carry_cap);
                        if (!nc) { free(carry); free(buf); close(fd); return -1; }
                        carry = nc;
                    }
                    memcpy(carry + carry_len, buf, (size_t)need2);
                    pos += (size_t)need2; carry_len = (int)rec_size;
                }
                if (carry[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       carry_off, (uint32_t)rec_size, db->slot_size);
                carry_len = 0;
```

Both the guard (`(ssize_t)need2 > nr`) and the two `memcpy` calls treat `nr` as if it were the number of bytes still available starting at `buf`, but `buf + pos` is the actual start of unconsumed data and `nr - pos` is what's actually left. This is a hardening fix, not a currently-exploitable one: the only fields read out of `carry` afterward are `carry[18]` (the flag byte) and `rec_size`, both of which come from the header that was already correctly assembled by Stage 1 before this block runs — so a wrong copy here can corrupt bytes that are never subsequently read in this function. Still, it's worth fixing correctly since a future caller reading more of `carry` would hit real corruption.

### The fix

In `src/db/slotcask.c`, find this exact block:

```c
                uint16_t klen; memcpy(&klen, carry + 16, 2);
                uint32_t vlen; memcpy(&vlen, carry + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                int need2 = (int)rec_size - carry_len;
                if (need2 > 0) {
                    if ((ssize_t)need2 > nr) {
                        if ((size_t)(carry_len + nr) > carry_cap) {
                            carry_cap = (size_t)(carry_len + nr);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf, (size_t)nr);
                        carry_len += (int)nr;
                        file_off2 += nr; continue;
                    }
                    if (rec_size > carry_cap) {
                        carry_cap = rec_size;
                        uint8_t *nc = realloc(carry, carry_cap);
                        if (!nc) { free(carry); free(buf); close(fd); return -1; }
                        carry = nc;
                    }
                    memcpy(carry + carry_len, buf, (size_t)need2);
                    pos += (size_t)need2; carry_len = (int)rec_size;
                }
                if (carry[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       carry_off, (uint32_t)rec_size, db->slot_size);
                carry_len = 0;
```

Replace it with:

```c
                uint16_t klen; memcpy(&klen, carry + 16, 2);
                uint32_t vlen; memcpy(&vlen, carry + 20, 4);
                size_t rec_size = slotcask_record_size_varlen((size_t)klen, (size_t)vlen);
                int need2 = (int)rec_size - carry_len;
                if (need2 > 0) {
                    /* Bytes actually remaining in this chunk starting at
                       buf + pos — NOT the full chunk size nr, since Stage 1
                       (header completion) may have already consumed pos
                       bytes from the front of this same chunk
                       (CID 1696471). */
                    size_t remain = (size_t)nr - pos;
                    if ((size_t)need2 > remain) {
                        if ((size_t)(carry_len + remain) > carry_cap) {
                            carry_cap = (size_t)(carry_len + remain);
                            uint8_t *nc = realloc(carry, carry_cap);
                            if (!nc) { free(carry); free(buf); close(fd); return -1; }
                            carry = nc;
                        }
                        memcpy(carry + carry_len, buf + pos, remain);
                        carry_len += (int)remain;
                        file_off2 += nr; continue;
                    }
                    if (rec_size > carry_cap) {
                        carry_cap = rec_size;
                        uint8_t *nc = realloc(carry, carry_cap);
                        if (!nc) { free(carry); free(buf); close(fd); return -1; }
                        carry = nc;
                    }
                    memcpy(carry + carry_len, buf + pos, (size_t)need2);
                    pos += (size_t)need2; carry_len = (int)rec_size;
                }
                if (carry[18] == 2)
                    pool_push_free_cap(&db->streams[sid], (uint16_t)file_id,
                                       carry_off, (uint32_t)rec_size, db->slot_size);
                carry_len = 0;
```

### Regression test

No new dedicated test. Engineering a byte-perfect crafted segment file that forces this exact carry path (a record header split across an O_DIRECT chunk boundary during crash-recovery tombstone scanning) while also making the corrupted-vs-correct byte range observable through `pool_push_free_cap`'s side effects is disproportionately complex relative to the fix, since (as established above) only header-derived fields are read afterward in this function. Rely on the full test suite continuing to pass — this is a pure code-correctness fix with no observable behavior change on well-formed files (where `pos == 0` at this point in the existing code paths that exercise it today, making `remain == nr` and the fix a no-op for all currently-tested inputs).

---

## Task 3 — `reindex_seg_cb` unvalidated `klen` before pointer arithmetic (CID 1696451)

### The bug

`src/db/index.c`'s `reindex_seg_cb` reads a 2-byte key length directly from an on-disk segment record and uses it, unchecked, to offset into the record for the value pointer:

```c
static int reindex_seg_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *ctx) {
    SegScanWorker *w = (SegScanWorker *)ctx;
    uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
    const uint8_t *value = rec + 24 + klen;
    if (w->padded_value && vlen < (size_t)w->ts->total_size) {
        memset(w->padded_value, 0, (size_t)w->ts->total_size);
        if (vlen > 0) memcpy(w->padded_value, value, vlen);
        value = w->padded_value;
    }
    for (int fi = 0; fi < w->n_fields; fi++) { ... }
    return 0;
}
```

This callback is invoked via `seg_scan_o_direct` for FIXED-format objects (`src/db/index.c`'s `seg_scan_worker`: `rc = seg_scan_o_direct(path, (int)w->slot_size, reindex_seg_cb, w);` when `w->format != SLOTCASK_FORMAT_VARIABLE`). `seg_scan_o_direct` does not itself validate `klen` against the fixed slot size before invoking the callback (unlike the VARLEN scan path, which already validates `klen` before calling its callback). A corrupted on-disk `klen` (e.g. from disk bitrot or a torn write) can push `value = rec + 24 + klen` arbitrarily far past the record's actual bounds, and the subsequent `memcpy(w->padded_value, value, vlen)` then reads from that out-of-bounds pointer.

### The fix

In `src/db/index.c`, find this exact block:

```c
static int reindex_seg_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *ctx) {
    SegScanWorker *w = (SegScanWorker *)ctx;
    uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
    const uint8_t *value = rec + 24 + klen;
```

Replace it with:

```c
static int reindex_seg_cb(const uint8_t *rec, size_t vlen,
                           const uint8_t hash16[16], void *ctx) {
    SegScanWorker *w = (SegScanWorker *)ctx;
    uint16_t klen = (uint16_t)rec[16] | ((uint16_t)rec[17] << 8);
    /* FIXED-format records are exactly w->slot_size bytes; a corrupted
       on-disk klen that would push the value pointer past the record's
       actual bounds must be rejected before the pointer arithmetic below
       (CID 1696451). VARLEN-format records already have klen validated by
       the caller (seg_scan_o_direct_varlen) before this callback runs. */
    if (w->format != SLOTCASK_FORMAT_VARIABLE &&
        (size_t)24 + klen > (size_t)w->slot_size) {
        w->had_error = 1;
        return 0;
    }
    const uint8_t *value = rec + 24 + klen;
```

### Regression test

New file `src/test/cases/test_coverity_disk_corruption_segments.c` (created in Task 5 below, alongside the CID 1696428 and CID 1696427 tests) covers this by corrupting a FIXED-format segment record's on-disk `klen` field to an oversized value, then running `reindex` and asserting the daemon does not crash.

---

## Task 4 — `bt_page` unbounded page access at two call sites (CID 1696448, CID 1696465)

### The bug

`src/db/btree.c`'s `bt_page` is a raw offset helper with no bounds checking:

```c
static inline uint8_t *bt_page(BtFile *bt, uint32_t page_id) {
    return bt->map + (size_t)page_id * bt_page_size;
}
```

Two call sites walk on-disk page-chain pointers (`next_leaf`, or child pointers descended from an internal page) with no check that the resulting `page_id` is within the file's actual `page_count`, and no cycle/hop-count guard — a corrupted `next_leaf` or child pointer can send either loop off into unmapped memory, or spin forever on a corrupted cycle.

`iter_init_desc_leaves`:

```c
static int iter_init_desc_leaves(BtRangeIter *it) {
    BtFileHeader *fh = (BtFileHeader *)it->bt.map;
    uint32_t page_id;

    int unbounded = (it->max_len == 4 &&
                     it->max_val[0] == (char)0xff && it->max_val[1] == (char)0xff &&
                     it->max_val[2] == (char)0xff && it->max_val[3] == (char)0xff);
    if (unbounded) {
        page_id = fh->last_leaf_page;
    } else {
        page_id = fh->root_page;
        while (1) {
            uint8_t *page = bt_page(&it->bt, page_id);
            BtPageHeader *ph = (BtPageHeader *)page;
            if (ph->page_type == 1) break;
            int pos = page_bsearch(page, it->max_val, it->max_len);
            if (pos < (int)ph->count) {
                uint8_t *e = page_entry(page, pos);
                if (val_cmp(int_entry_value(e), int_entry_vlen(e),
                            it->max_val, it->max_len) == 0) {
                    page_id = entry_child(e);
                    continue;
                }
            }
            if (pos == 0) page_id = ph->next_leaf;
            else          page_id = entry_child(page_entry(page, pos - 1));
            if (page_id == 0) break;
        }
    }

    it->desc_leaves = malloc(sizeof(uint32_t));
    if (!it->desc_leaves) return -1;
    it->desc_leaves[0] = page_id;
    it->desc_leaf_count = (page_id != 0) ? 1 : 0;
    it->desc_li = (int)it->desc_leaf_count - 1;
    it->desc_snap_i = -1;
    if (it->desc_li >= 0) iter_load_desc_snap(it);
    return 0;
}
```

`btree_walk_all_values` has the analogous unguarded leftmost-descent loop and main leaf-chain loop.

### The fix

In `src/db/btree.c`, find this exact block:

```c
        page_id = fh->root_page;
        while (1) {
            uint8_t *page = bt_page(&it->bt, page_id);
            BtPageHeader *ph = (BtPageHeader *)page;
            if (ph->page_type == 1) break;
            int pos = page_bsearch(page, it->max_val, it->max_len);
            if (pos < (int)ph->count) {
                /* If entry[pos] == max_val, descend its own child (which
                   contains keys with that separator and above-within-range). */
                uint8_t *e = page_entry(page, pos);
                if (val_cmp(int_entry_value(e), int_entry_vlen(e),
                            it->max_val, it->max_len) == 0) {
                    page_id = entry_child(e);
                    continue;
                }
            }
            /* No exact match: descend the child immediately before pos. */
            if (pos == 0) page_id = ph->next_leaf;
            else          page_id = entry_child(page_entry(page, pos - 1));
            if (page_id == 0) break;
        }
```

Replace it with:

```c
        page_id = fh->root_page;
        uint32_t desc_page_count = fh->page_count;
        uint32_t desc_hops = 0;
        while (page_id != 0) {
            /* page_id comes from an on-disk child/next_leaf pointer that
               may be corrupted; bound it against the file's actual
               page_count and cap total hops at page_count so a corrupted
               cycle can't spin forever (CID 1696448). */
            if (page_id >= desc_page_count || ++desc_hops > desc_page_count) {
                page_id = 0;
                break;
            }
            uint8_t *page = bt_page(&it->bt, page_id);
            BtPageHeader *ph = (BtPageHeader *)page;
            if (ph->page_type == 1) break;
            int pos = page_bsearch(page, it->max_val, it->max_len);
            if (pos < (int)ph->count) {
                /* If entry[pos] == max_val, descend its own child (which
                   contains keys with that separator and above-within-range). */
                uint8_t *e = page_entry(page, pos);
                if (val_cmp(int_entry_value(e), int_entry_vlen(e),
                            it->max_val, it->max_len) == 0) {
                    page_id = entry_child(e);
                    continue;
                }
            }
            /* No exact match: descend the child immediately before pos. */
            if (pos == 0) page_id = ph->next_leaf;
            else          page_id = entry_child(page_entry(page, pos - 1));
        }
```

Now open `src/db/btree.c` and locate `btree_walk_all_values` (`int btree_walk_all_values(const char *path, bt_value_only_cb cb, void *ctx)`). Find this exact block (the leftmost-descent loop, immediately after the `fh->entry_count == 0` early-return check):

```c
    uint32_t page_id = fh->root_page;
    while (1) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
        if (page_id == 0) {
            if (set_seq) madvise(bt.map, bt.map_size, MADV_RANDOM);
            bt_release(&bt); return 0;
        }
    }
```

Replace it with:

```c
    uint32_t page_id = fh->root_page;
    uint32_t walk_page_count = fh->page_count;
    uint32_t walk_hops = 0;
    while (1) {
        /* Same corrupted-pointer / cycle guard as iter_init_desc_leaves
           (CID 1696448 / CID 1696465): page_id descends via on-disk
           next_leaf pointers with no inherent bound. */
        if (page_id >= walk_page_count || ++walk_hops > walk_page_count) {
            if (set_seq) madvise(bt.map, bt.map_size, MADV_RANDOM);
            bt_release(&bt); return 0;
        }
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
        if (page_id == 0) {
            if (set_seq) madvise(bt.map, bt.map_size, MADV_RANDOM);
            bt_release(&bt); return 0;
        }
    }
```

Then, still in `btree_walk_all_values`, find this exact block (the main leaf-chain loop — anchored uniquely on `int rc = 0;`, which appears nowhere else in the file):

```c
    char key_buf[BT_MAX_VAL_LEN];
    size_t key_len = 0;
    int rc = 0;

    while (page_id != 0) {
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        int cnt = ph->count;
        for (int slot = 0; slot < cnt; slot++) {
            uint8_t *e = page_entry(page, slot);
            uint8_t plen = leaf_entry_prefix_len(e);
            size_t slen = leaf_entry_suffix_len(e);
            if ((slot & (BT_LEAF_RESTART_K - 1)) == 0) {
                /* Anchor — full key in suffix bytes. */
                key_len = slen;
                if (key_len > BT_MAX_VAL_LEN) key_len = BT_MAX_VAL_LEN;
                memcpy(key_buf, leaf_entry_suffix(e), key_len);
            } else {
                /* Prefix-compressed: keep first plen bytes of previous
                   key, append this slot's suffix. */
                /* plen ≤ 255 (uint8_t) and BT_MAX_VAL_LEN is 512, so
                   key_buf + plen is always in-range; only clamp the
                   total length. */
                size_t klen = (size_t)plen + slen;
                if (klen > BT_MAX_VAL_LEN) klen = BT_MAX_VAL_LEN;
                size_t take = (klen > (size_t)plen) ? (klen - (size_t)plen) : 0;
                memcpy(key_buf + plen, leaf_entry_suffix(e), take);
                key_len = klen;
            }
            if (leaf_entry_is_tomb(e)) continue;
            rc = cb(key_buf, key_len, ctx);
            if (rc != 0) goto done;
        }
        page_id = ph->next_leaf;
    }
done:
```

Replace it with:

```c
    char key_buf[BT_MAX_VAL_LEN];
    size_t key_len = 0;
    int rc = 0;

    uint32_t chain_hops = 0;
    while (page_id != 0) {
        /* Same corrupted-pointer / cycle guard as above: page_id advances
           via ph->next_leaf with no inherent bound (CID 1696448). */
        if (page_id >= walk_page_count || ++chain_hops > walk_page_count) {
            break;
        }
        uint8_t *page = bt_page(&bt, page_id);
        BtPageHeader *ph = (BtPageHeader *)page;
        int cnt = ph->count;
        for (int slot = 0; slot < cnt; slot++) {
            uint8_t *e = page_entry(page, slot);
            uint8_t plen = leaf_entry_prefix_len(e);
            size_t slen = leaf_entry_suffix_len(e);
            if ((slot & (BT_LEAF_RESTART_K - 1)) == 0) {
                /* Anchor — full key in suffix bytes. */
                key_len = slen;
                if (key_len > BT_MAX_VAL_LEN) key_len = BT_MAX_VAL_LEN;
                memcpy(key_buf, leaf_entry_suffix(e), key_len);
            } else {
                /* Prefix-compressed: keep first plen bytes of previous
                   key, append this slot's suffix. */
                /* plen ≤ 255 (uint8_t) and BT_MAX_VAL_LEN is 512, so
                   key_buf + plen is always in-range; only clamp the
                   total length. */
                size_t klen = (size_t)plen + slen;
                if (klen > BT_MAX_VAL_LEN) klen = BT_MAX_VAL_LEN;
                size_t take = (klen > (size_t)plen) ? (klen - (size_t)plen) : 0;
                memcpy(key_buf + plen, leaf_entry_suffix(e), take);
                key_len = klen;
            }
            if (leaf_entry_is_tomb(e)) continue;
            rc = cb(key_buf, key_len, ctx);
            if (rc != 0) goto done;
        }
        page_id = ph->next_leaf;
    }
done:
```

**Anchor-uniqueness note for the executing model**: both blocks above live inside `btree_walk_all_values` (`int btree_walk_all_values(const char *path, bt_value_only_cb cb, void *ctx)`), which uses a locally-declared `BtFile bt;` and calls `bt_page(&bt, page_id)` — distinct from `iter_init_desc_leaves`'s `it->bt` / `bt_page(&it->bt, page_id)`, already handled above. The second block is anchored on `int rc = 0;`, which is unique in the file (verify with `grep -n "int rc = 0;" src/db/btree.c` if in doubt).

### Regression test

New file `src/test/cases/test_coverity_disk_corruption_btree.c` (created in Task 5 below) covers CID 1696448 by corrupting an on-disk `.idx` file's leaf `next_leaf` field to point at itself (a self-cycle) and confirming a descending-order find/cursor query does not hang or crash.

---

## Task 5 — `btree_leaf_scan_o_direct` unvalidated slot count (CID 1696431) and new disk-corruption test files

### The bug

`src/db/io_direct.c`'s `btree_decode_leaves_in_range` reads a leaf page's `count` field directly from on-disk bytes and uses it, unchecked, as the loop bound for iterating slot offsets:

```c
static int btree_decode_leaves_in_range(const uint8_t *range, size_t range_len,
                                        off_t start_off, int page_sz,
                                        od_leaf_cb cb, void *ctx, int *stop_out)
{
    size_t off = 0;
    if (start_off == 0 && range_len >= sizeof(BtFileHeader)) {
        const BtFileHeader *fh = (const BtFileHeader *)range;
        uint32_t magic; memcpy(&magic, &fh->magic, 4);
        if (magic != BT_MAGIC) return -EINVAL;
        uint64_t ec; memcpy(&ec, &fh->entry_count, 8);
        if (ec == 0) { *stop_out = 1; return 0; }
        off = (size_t)page_sz;
    }

    char   key_buf[BT_MAX_VAL_LEN];
    size_t key_len = 0;

    while (off + (size_t)page_sz <= range_len) {
        const uint8_t *pg = range + off;
        const BtPageHeader *ph = (const BtPageHeader *)pg;
        uint32_t ptype; memcpy(&ptype, &ph->page_type, 4);

        if (ptype == 1) {
            uint32_t cnt; memcpy(&cnt, &ph->count, 4);

            for (uint32_t s = 0; s < cnt; s++) {
                uint16_t eoff = bts_slot_off(pg, s);
                if ((size_t)eoff + 3 > (size_t)page_sz) break;  /* corrupt */
                ...
```

`cnt` comes straight from the on-disk page header with no upper bound. `bts_slot_off(pg, s)` computes a slot-table offset as a function of `s`; a corrupted `cnt` far larger than the page can actually hold sends `s` iterating past the slot table into whatever else follows in the mapped/buffered range before the per-iteration `eoff` check has a chance to catch it (the check happens **after** computing `bts_slot_off`, so the read that produces `eoff` itself is already out of the intended slot-table region for large enough `s`).

### The fix

In `src/db/io_direct.c`, find this exact block:

```c
        if (ptype == 1) {
            uint32_t cnt; memcpy(&cnt, &ph->count, 4);

            for (uint32_t s = 0; s < cnt; s++) {
                uint16_t eoff = bts_slot_off(pg, s);
                if ((size_t)eoff + 3 > (size_t)page_sz) break;  /* corrupt */
```

Replace it with:

```c
        if (ptype == 1) {
            uint32_t cnt; memcpy(&cnt, &ph->count, 4);
            /* cnt is read straight from an on-disk page header; clamp it
               to the maximum number of 2-byte slot-table entries that can
               possibly fit after the page header before iterating, so a
               corrupted cnt can't walk bts_slot_off() past the page
               (CID 1696431). */
            size_t max_slots = (page_sz > BT_PAGE_DATA_START)
                                    ? ((size_t)page_sz - BT_PAGE_DATA_START) / 2
                                    : 0;
            if (cnt > max_slots) cnt = (uint32_t)max_slots;

            for (uint32_t s = 0; s < cnt; s++) {
                uint16_t eoff = bts_slot_off(pg, s);
                if ((size_t)eoff + 3 > (size_t)page_sz) break;  /* corrupt */
```

`BT_PAGE_DATA_START` is already defined in `btree.h`, which `io_direct.c` already includes — no new include needed. If `grep -n "BT_PAGE_DATA_START" src/db/btree.h` returns nothing, stop and write `PLAN_NOTES.md` instead of inventing a value.

### New test files (this task creates all four)

Add `src/test/cases/test_coverity_disk_corruption_segments.c`:

```c
/* src/test/cases/test_coverity_disk_corruption_segments.c
 *
 * CID 1696451: reindex_seg_cb (index.c) trusted an on-disk segment
 * record's klen field without validating it against the FIXED-format
 * slot_size before using it in pointer arithmetic. A corrupted klen could
 * push the value pointer out of bounds.
 *
 * CID 1696428 / CID 1696427: mf_append_field (index.c, streaming reindex
 * path) and tg_estimate_cb (query_maint.c, estimate-index path) both
 * trusted an on-disk varchar length prefix without clamping it to the
 * field's actual declared size before calling tg_extract_distinct.
 *
 * Pattern: spin up a real daemon, create an object + insert real records,
 * stop the daemon, directly corrupt specific on-disk bytes, restart the
 * daemon at the same db_root/port, and confirm the vulnerable operation
 * (reindex / estimate-index) completes without crashing the daemon.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

/* Find the first regular file directly under dir_path whose name ends
   with suffix. Writes the full path into out (size out_sz). Returns 0 on
   success, -1 if none found. */
static int find_first_file_with_suffix(const char *dir_path,
                                        const char *suffix,
                                        char *out, size_t out_sz) {
    DIR *d = opendir(dir_path);
    if (!d) return -1;
    struct dirent *de;
    int found = -1;
    size_t suf_len = strlen(suffix);
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < suf_len) continue;
        if (strcmp(de->d_name + nlen - suf_len, suffix) != 0) continue;
        snprintf(out, out_sz, "%s/%s", dir_path, de->d_name);
        found = 0;
        break;
    }
    closedir(d);
    return found;
}

/* --- CID 1696451: corrupt a FIXED-format segment record's on-disk klen. */
static int test_coverity_seg_klen_corruption_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-seg-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"segrec\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"v:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: segrec");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"segrec\","
        "\"key\":\"k1\",\"value\":{\"v\":\"hello\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    /* Locate the segment data file: <db_root>/d/segrec/data/streams/000/000000.dat */
    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/segrec/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    /* Segment record header: 16B hash + 2B klen + 1B flag + 1B reserved +
       4B vlen, at file offset 0 for the first record. Corrupt klen
       (offset 16, 2 bytes) to an oversized value. */
    uint16_t bad_klen = 60000;
    ssize_t wr = pwrite(fd, &bad_klen, sizeof(bad_klen), 16);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_klen), "corrupt klen write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Trigger reindex_seg_cb via the streaming reindex path. Must not
       crash the daemon regardless of what it returns. */
    tc_request(tc, "{\"mode\":\"reindex\",\"dir\":\"d\",\"object\":\"segrec\"}", &resp);
    free(resp); resp = NULL;

    /* Daemon must still be alive and responsive afterward. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"segrec\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-klen reindex");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696428: corrupt an on-disk varchar length prefix ahead of a
   streaming reindex's trigram field extraction (mf_append_field). */
static int test_coverity_reindex_trigram_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-tg1-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"bio:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: tgobj");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"key\":\"k1\",\"value\":{\"bio\":\"hello world\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    /* Corrupt the varchar length prefix inside the stored value. Record
       layout: 16B hash + 2B klen + 1B flag + 1B reserved + 4B vlen (24B
       header) + key bytes + value bytes. key = "k1" (2 bytes). The
       "bio" field's on-disk encoding starts with its own 2-byte length
       prefix. Corrupt that prefix to a value far larger than the field's
       declared size (32 bytes → 30 content bytes max). */
    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/tgobj/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    size_t value_off = 24 + 2; /* header + klen("k1") */
    uint16_t bad_len = 60000;
    ssize_t wr = pwrite(fd, &bad_len, sizeof(bad_len), (off_t)value_off);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_len), "corrupt varchar len write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    tc_request(tc,
        "{\"mode\":\"add-index\",\"dir\":\"d\",\"object\":\"tgobj\","
        "\"fields\":[\"bio:trigram\"],\"force\":true}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"tgobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-varchar-len trigram build");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696427: same class of bug via the estimate-index JSON mode
   (tg_estimate_cb in query_maint.c), which samples records directly
   rather than going through the streaming reindex path. */
static int test_coverity_estimate_index_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-tg2-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"bio:varchar:32\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: tgobj2");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"key\":\"k1\",\"value\":{\"bio\":\"hello world\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char seg_path[600];
    snprintf(seg_path, sizeof(seg_path), "%s/d/tgobj2/data/streams/000/000000.dat", db_root);
    int fd = open(seg_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open segment file for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    size_t value_off = 24 + 2;
    uint16_t bad_len = 60000;
    ssize_t wr = pwrite(fd, &bad_len, sizeof(bad_len), (off_t)value_off);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_len), "corrupt varchar len write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    tc_request(tc,
        "{\"mode\":\"estimate-index\",\"dir\":\"d\",\"object\":\"tgobj2\","
        "\"spec\":\"bio:trigram\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted-varchar-len estimate-index");
    free(resp); resp = NULL;

    /* Follow-up request confirms the daemon process itself is still alive. */
    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"tgobj2\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon still responsive after estimate-index");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-seg-klen-corruption", test_coverity_seg_klen_corruption_run);
TEST_REGISTER("test-coverity-reindex-trigram-overflow", test_coverity_reindex_trigram_overflow_run);
TEST_REGISTER("test-coverity-estimate-index-overflow", test_coverity_estimate_index_overflow_run);
```

Notes for the executing model:
- `find_first_file_with_suffix` is defined but unused by the three tests above (they use hardcoded, single-shard paths since `splits:8` with 1 record deterministically routes to shard 0 for the segment stream — the segment file used, `data/streams/000/000000.dat`, is stream 0 file 0, which always exists as the first segment regardless of hash routing, since stream_id only affects *which* segment a given key's data lands in, not whether stream 0 file 0 exists). Leave the helper in place — it is used by `test_coverity_disk_corruption_btree.c` and `test_coverity_disk_corruption_bitmap.c` below, which must each `#include` their own copy or this file must expose it. **To avoid a multiple-definition link error, mark `find_first_file_with_suffix` `static` in each of the three new test files that use it** (it already is `static` above) — each `.c` file gets its own private copy; do not share it via a header.
- If `pwrite`/`open` fail unexpectedly (e.g. path doesn't exist), the `ASSERT_TRUE`/`ASSERT_EQ_INT` calls will fail loudly with a descriptive message — do not add extra error suppression.

Add `src/test/cases/test_coverity_disk_corruption_btree.c`:

```c
/* src/test/cases/test_coverity_disk_corruption_btree.c
 *
 * CID 1696448 / CID 1696465: bt_page() call sites in iter_init_desc_leaves
 * and btree_walk_all_values followed on-disk page-chain pointers
 * (next_leaf / child pointers) with no bound against the file's actual
 * page_count and no cycle guard. A corrupted next_leaf pointing back at
 * its own page (a 1-cycle) would spin the descending iterator forever
 * pre-fix.
 *
 * CID 1696431: btree_decode_leaves_in_range (io_direct.c) trusted an
 * on-disk leaf page's `count` field as a loop bound for slot-table
 * iteration with no clamp against how many slots can actually fit in one
 * page.
 *
 * Discovers the on-disk .idx file at runtime (rather than hand-computing
 * hash routing) by reading each shard file's BtFileHeader.entry_count
 * until a non-empty shard is found.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

#define BT_PAGE_SIZE 4096

/* Read a BtFileHeader's entry_count (offset 16, 8 bytes) from an .idx
   file. Returns 0 on read failure (treated as "empty"). */
static uint64_t read_entry_count(const char *path) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return 0;
    uint64_t ec = 0;
    ssize_t n = pread(fd, &ec, sizeof(ec), 16);
    close(fd);
    if (n != (ssize_t)sizeof(ec)) return 0;
    return ec;
}

/* Scan <idx_dir> for the first NNN.idx shard whose entry_count > 0.
   Writes the full path into out. Returns 0 on success, -1 if none found. */
static int find_nonempty_idx_shard(const char *idx_dir, char *out, size_t out_sz) {
    DIR *d = opendir(idx_dir);
    if (!d) return -1;
    struct dirent *de;
    int found = -1;
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < 4) continue;
        if (strcmp(de->d_name + nlen - 4, ".idx") != 0) continue;
        char candidate[600];
        snprintf(candidate, sizeof(candidate), "%s/%s", idx_dir, de->d_name);
        if (read_entry_count(candidate) > 0) {
            snprintf(out, out_sz, "%s", candidate);
            found = 0;
            break;
        }
    }
    closedir(d);
    return found;
}

static int test_coverity_btree_nextleaf_cycle_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bt-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"btobj\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\"],"
        "\"indexes\":[\"n\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: btobj");
    free(resp); resp = NULL;

    for (int i = 0; i < 200; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"btobj\","
            "\"key\":\"k%03d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char idx_dir[600];
    snprintf(idx_dir, sizeof(idx_dir), "%s/d/btobj/indexes/n", db_root);
    char idx_path[600];
    int rc = find_nonempty_idx_shard(idx_dir, idx_path, sizeof(idx_path));
    ASSERT_EQ_INT(rc, 0, "found a non-empty btree index shard");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(idx_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open idx shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    uint32_t root_page = 0;
    pread(fd, &root_page, sizeof(root_page), 4);
    ASSERT_TRUE(root_page > 0, "root_page discovered");

    /* Walk from the root page to the first leaf page (page_type == 1) so
       we corrupt an actual leaf's next_leaf, not an internal page's. */
    uint32_t page_id = root_page;
    uint32_t page_type = 0;
    for (int hop = 0; hop < 32; hop++) {
        off_t page_off = (off_t)page_id * BT_PAGE_SIZE;
        pread(fd, &page_type, sizeof(page_type), page_off + 0);
        if (page_type == 1) break;
        /* Internal page: descend via the first entry's child pointer.
           Entry layout is not needed byte-precise here — if this object's
           200 records fit in a single leaf (root_page IS the leaf), the
           loop above already broke out with page_type==1 on the first
           iteration, which is the common case for this dataset size and
           avoids needing internal-page entry parsing at all. */
        break;
    }
    ASSERT_EQ_INT((int)page_type, 1, "located a leaf page");

    /* Corrupt next_leaf (page-relative offset 8) to point at this same
       page — a 1-cycle that would spin the pre-fix descending iterator
       forever. */
    off_t page_off = (off_t)page_id * BT_PAGE_SIZE;
    uint32_t self_cycle = page_id;
    ssize_t wr = pwrite(fd, &self_cycle, sizeof(self_cycle), page_off + 8);
    ASSERT_EQ_INT((int)wr, (int)sizeof(self_cycle), "corrupt next_leaf write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    cfg.io_timeout_ms = 10000; /* short timeout: pre-fix this would hang */
    tc_close(tc);
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Descending order_by drives iter_init_desc_leaves through the
       corrupted next_leaf pointer. Must return (not hang) and must not
       crash the daemon. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d\",\"object\":\"btobj\","
        "\"criteria\":[],\"order_by\":{\"field\":\"n\",\"dir\":\"desc\"},"
        "\"limit\":5}", &resp);
    ASSERT_NOT_NULL(resp, "descending find returns instead of hanging");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"btobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon still responsive after cycle corruption");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

/* --- CID 1696431: corrupt a leaf page's `count` field to an oversized
   value ahead of a full-index O_DIRECT leaf scan (vacuum triggers one). */
static int test_coverity_btree_leafcount_overflow_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bt2-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"btobj2\","
        "\"splits\":8,\"max_key\":16,\"fields\":[\"n:int\"],"
        "\"indexes\":[\"n\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: btobj2");
    free(resp); resp = NULL;

    for (int i = 0; i < 50; i++) {
        char req[256];
        snprintf(req, sizeof(req),
            "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"btobj2\","
            "\"key\":\"k%03d\",\"value\":{\"n\":%d}}", i, i);
        tc_request(tc, req, &resp);
        free(resp); resp = NULL;
    }
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char idx_dir[600];
    snprintf(idx_dir, sizeof(idx_dir), "%s/d/btobj2/indexes/n", db_root);
    char idx_path[600];
    int rc = find_nonempty_idx_shard(idx_dir, idx_path, sizeof(idx_path));
    ASSERT_EQ_INT(rc, 0, "found a non-empty btree index shard");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(idx_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open idx shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    uint32_t root_page = 0;
    pread(fd, &root_page, sizeof(root_page), 4);
    ASSERT_TRUE(root_page > 0, "root_page discovered");

    /* Corrupt the root/leaf page's count field (page-relative offset 4)
       to a huge value. With 50 records, splits:8, index_splits_for(8)=2,
       the leaf almost certainly fits in one page (root IS the leaf). */
    off_t page_off = (off_t)root_page * BT_PAGE_SIZE;
    uint32_t bad_count = 0xFFFFFF00u;
    ssize_t wr = pwrite(fd, &bad_count, sizeof(bad_count), page_off + 4);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_count), "corrupt leaf count write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* vacuum triggers a full-index O_DIRECT leaf scan via
       btree_decode_leaves_in_range. Must not crash the daemon. */
    tc_request(tc, "{\"mode\":\"vacuum\",\"dir\":\"d\",\"object\":\"btobj2\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"btobj2\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted leaf count during vacuum");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-btree-nextleaf-cycle", test_coverity_btree_nextleaf_cycle_run);
TEST_REGISTER("test-coverity-btree-leafcount-overflow", test_coverity_btree_leafcount_overflow_run);
```

Add `src/test/cases/test_coverity_disk_corruption_bitmap.c`:

```c
/* src/test/cases/test_coverity_disk_corruption_bitmap.c
 *
 * CID 1696403: bm_dict_used_bytes (bitmap.c) walked the on-disk
 * dictionary using n_values (an on-disk header field) as the loop bound,
 * with no check that each entry's [len][bytes] pair actually stayed
 * within the mapped region before bm_dict_add's next write.
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

static int find_first_bm_shard(const char *dir_path, char *out, size_t out_sz) {
    DIR *d = opendir(dir_path);
    if (!d) return -1;
    struct dirent *de;
    int found = -1;
    while ((de = readdir(d)) != NULL) {
        size_t nlen = strlen(de->d_name);
        if (nlen < 3) continue;
        if (strcmp(de->d_name + nlen - 3, ".bm") != 0) continue;
        snprintf(out, out_sz, "%s/%s", dir_path, de->d_name);
        found = 0;
        break;
    }
    closedir(d);
    return found;
}

static int test_coverity_bitmap_nvalues_corruption_run(void) {
    char base[256], db_root[256];
    snprintf(base,    sizeof(base),    "/tmp/shard-db-cov-bm-%d", (int)getpid());
    snprintf(db_root, sizeof(db_root), "%s/db", base);
    tu_run_cmd("rm -rf %s", base);
    mkdir(base, 0755);
    mkdir(db_root, 0755);

    int port = test_pick_port();
    if (port < 0) { ASSERT_TRUE(0, "pick port"); tu_run_cmd("rm -rf %s", base); return 1; }

    TestEnv env = {0};
    if (test_env_start_at(&env, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon spawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }

    TestClientCfg cfg = { .port = port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect");
    if (!tc) { test_env_kill(&env); tu_run_cmd("rm -rf %s", base); return 1; }

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"name\":\"d\"}", &resp);
    free(resp); resp = NULL;
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"splits\":8,\"max_key\":16,"
        "\"fields\":[\"cat:varchar:16\"],\"indexes\":[\"cat:bitmap\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create-object: bmobj");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"key\":\"k1\",\"value\":{\"cat\":\"red\"}}", &resp);
    free(resp); resp = NULL;
    tc_close(tc); tc = NULL;

    test_env_stop_keep(&env);

    char bm_dir[600];
    snprintf(bm_dir, sizeof(bm_dir), "%s/d/bmobj/indexes/cat", db_root);
    char bm_path[600];
    int rc = find_first_bm_shard(bm_dir, bm_path, sizeof(bm_path));
    ASSERT_EQ_INT(rc, 0, "found a bitmap shard file");
    if (rc != 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    int fd = open(bm_path, O_RDWR);
    ASSERT_TRUE(fd >= 0, "open bitmap shard for corruption");
    if (fd < 0) { tu_run_cmd("rm -rf %s", base); return 1; }

    /* Corrupt n_values (header offset 12, 4 bytes) to a huge value, far
       larger than the dictionary region actually holds. */
    uint32_t bad_n_values = 0x0FFFFFFFu;
    ssize_t wr = pwrite(fd, &bad_n_values, sizeof(bad_n_values), 12);
    ASSERT_EQ_INT((int)wr, (int)sizeof(bad_n_values), "corrupt n_values write");
    close(fd);

    TestEnv env2 = {0};
    if (test_env_start_at(&env2, db_root, port) != 0) {
        ASSERT_TRUE(0, "daemon respawn"); tu_run_cmd("rm -rf %s", base); return 1;
    }
    cfg.port = env2.port;
    tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "reconnect after corruption");
    if (!tc) { test_env_kill(&env2); tu_run_cmd("rm -rf %s", base); return 1; }

    /* Insert a second record with a NEW distinct value — this calls
       bm_dict_add, which calls bm_dict_used_bytes using the corrupted
       n_values as its loop bound. Must not crash the daemon. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d\",\"object\":\"bmobj\","
        "\"key\":\"k2\",\"value\":{\"cat\":\"blue\"}}", &resp);
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"count\",\"dir\":\"d\",\"object\":\"bmobj\"}", &resp);
    ASSERT_NOT_NULL(resp, "daemon survives corrupted n_values during bm_dict_add");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_kill(&env2);
    tu_run_cmd("rm -rf %s", base);
    return 0;
}

TEST_REGISTER("test-coverity-bitmap-nvalues-corruption", test_coverity_bitmap_nvalues_corruption_run);
```

Now register all three new files in `build.sh`. Find this exact line:

```
    src/test/cases/test_secure_random_keys.c \
```

Replace it with:

```
    src/test/cases/test_secure_random_keys.c \
    src/test/cases/test_coverity_disk_corruption_segments.c \
    src/test/cases/test_coverity_disk_corruption_btree.c \
    src/test/cases/test_coverity_disk_corruption_bitmap.c \
```

(The fourth new test file, `test_coverity_seg_scan_varlen_overflow.c`, is added separately in Task 6 below, appended after these three in the same list.)

---

## Task 6 — `seg_scan_o_direct_varlen` int-narrowing of `size_t rec_size` (CID 1696466)

### The bug

`src/db/io_direct.c`'s `seg_scan_o_direct_varlen` computes a record's total size from on-disk, unvalidated `klen`/`vlen` fields, then narrows it into a 32-bit `int` before using it in carry-buffer arithmetic:

```c
                size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

                int need = (int)rec_size - carry_len;
                if (need > 0) {
                    ...
                }

                if (flag == 1) {
                    if (cb(carry, (size_t)vlen, carry, ctx) != 0) {
                        ret = 1; goto done;
                    }
                }
                carry_len = 0;
```

If the on-disk `vlen` is corrupted to a very large value (e.g. `0xFFFFFFF0`), `rec_size` becomes a huge `size_t` (on the order of 4 billion). Casting that to `int` truncates it to the low 32 bits, which can produce a small or even *negative* `need`. A concrete case: with `klen=0`, `vlen=0xFFFFFFF0`, `rec_size = 4294967304`; `(int)rec_size` truncates to `8` (4294967304 mod 2^32 = 8); if `carry_len` is 24 at that point, `need = 8 - 24 = -16 <= 0`, so the "need more data" branch is skipped entirely, and the code falls through to `cb(carry, (size_t)vlen, carry, ctx)` with `vlen = 0xFFFFFFF0` against `carry`, a buffer nowhere near that size — an out-of-bounds read/callback-side overflow.

### The fix

Reject corrupted records whose claimed `rec_size` exceeds the maximum any legitimate segment record could ever be, before the narrowing cast happens. `SLOTCASK_SEG_MAX_BYTES` (128MB, defined in `slotcask.h`) is the correct sanity bound.

First, add the missing include. In `src/db/io_direct.c`, find this exact block (the top of the includes list):

```c
#include "io_direct.h"
#include "btree.h"    /* BtFileHeader, BtPageHeader, BT_MAGIC*, bt_page_size,
                         BT_PAGE_DATA_START, BT_LEAF_RESTART_K,
                         BT_MAX_VAL_LEN, BT_HASH_SIZE               */
#include "simd.h"    /* simd_memmem */
```

Replace it with:

```c
#include "io_direct.h"
#include "btree.h"    /* BtFileHeader, BtPageHeader, BT_MAGIC*, bt_page_size,
                         BT_PAGE_DATA_START, BT_LEAF_RESTART_K,
                         BT_MAX_VAL_LEN, BT_HASH_SIZE               */
#include "slotcask.h"
#include "simd.h"    /* simd_memmem */
```

Then, in `src/db/io_direct.c`, find this exact block inside `seg_scan_o_direct_varlen`:

```c
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            int need = (int)rec_size - carry_len;
```

Replace it with:

```c
            size_t rec_size = od_varlen_rec_size(klen, (uint32_t)vlen);

            /* rec_size is derived from an on-disk, unvalidated vlen.
               A corrupted vlen can make rec_size enormous; narrowing
               it into `int` below would silently wrap and produce a
               small or negative `need`, skipping the "need more data"
               branch and passing a huge vlen straight to cb() against
               the small carry buffer (CID 1696466). Reject anything
               past the largest a legitimate segment record could be. */
            if (rec_size > SLOTCASK_SEG_MAX_BYTES) {
                ret = -EIO;
                goto done;
            }

            int need = (int)rec_size - carry_len;
```

If `grep -n "SLOTCASK_SEG_MAX_BYTES" src/db/slotcask.h` returns nothing, stop and write `PLAN_NOTES.md` instead of inventing a value or including a different header.

### Regression test

Add `src/test/cases/test_coverity_seg_scan_varlen_overflow.c`:

```c
/* src/test/cases/test_coverity_seg_scan_varlen_overflow.c
 *
 * CID 1696466: seg_scan_o_direct_varlen narrowed a size_t rec_size
 * (derived from an on-disk, unvalidated vlen field) into an int before
 * computing `need`. A corrupted vlen near the top of the uint32_t range
 * makes the truncated int small or negative, skipping the "need more
 * data" carry-buffer growth entirely and passing the huge vlen straight
 * to the scan callback.
 *
 * This is exercised through a directly-crafted on-disk segment file (not
 * a live daemon + corruption, since this scan path is only reachable via
 * slotcask's internal VARLEN recovery/migration machinery, not a public
 * JSON mode). od_varlen_rec_size(klen=0, vlen=0xFFFFFFF0) = 24 (header)
 * + 0 (klen) + 0xFFFFFFF0 = 4294967304, which truncates as an int to 8
 * (4294967304 mod 2^32 = 8) — reproducing the exact wrap described in
 * the finding.
 *
 * seg_scan_o_direct_varlen reads in odirect_buf_size-sized chunks
 * (default 32 MiB = 33554432 bytes, set once lazily and shared across
 * the whole test-runner process — see io_direct.c). To make this
 * deterministic regardless of what earlier-run tests may have done to
 * that global, the crafted file's split point is computed relative to
 * the well-known 32 MiB default: 1,398,101 padding records of 24 bytes
 * each (tombstones: klen=0, vlen=0, flag=2) = 33,554,424 bytes, leaving
 * exactly 8 bytes before the 33,554,432-byte chunk boundary. The
 * malicious record's 24-byte header (klen=0, vlen=0xFFFFFFF0, flag=1) is
 * placed there, split 8 bytes into chunk 1 / 16 bytes into chunk 2 — so
 * Stage 1 (header completion) and Stage 2 (rec_size computation) both
 * run while processing chunk 2, deterministically hitting the narrowing
 * bug. Total file size: 33,554,448 bytes.
 *
 * Records here are 24-byte headers with klen=0 and no value bytes
 * (matches od_varlen_rec_size's minimum shape); vlen=0 padding records
 * are valid empty-value VARLEN tombstone-shaped records for this raw
 * scan (the callback is never invoked for flag=2 padding — see
 * seg_scan_o_direct_varlen's flag handling — so their content doesn't
 * matter, only their header bytes).
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include "test_runner.h"
#include "test_assert.h"
#include "io_direct.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

static int g_cb_calls = 0;

static int capture_cb(const uint8_t *rec, size_t vlen,
                       const uint8_t hash16[16], void *ctx) {
    (void)rec; (void)vlen; (void)hash16; (void)ctx;
    g_cb_calls++;
    return 0;
}

/* Write a 24-byte VARLEN record header: 16B hash + 2B klen + 1B flag +
   1B reserved + 4B vlen. No key/value bytes follow (klen=0, and for the
   malicious record vlen is a lie the header makes but no bytes back it). */
static void write_header(FILE *f, uint16_t klen, uint8_t flag, uint32_t vlen) {
    uint8_t hdr[24];
    memset(hdr, 0, sizeof(hdr));
    memcpy(hdr + 16, &klen, 2);
    hdr[18] = flag;
    hdr[19] = 0;
    memcpy(hdr + 20, &vlen, 4);
    fwrite(hdr, 1, sizeof(hdr), f);
}

static int test_coverity_seg_scan_varlen_overflow_run(void) {
    char path[256];
    snprintf(path, sizeof(path), "/tmp/shard-db-cov-varlen-%d.dat", (int)getpid());

    FILE *f = fopen(path, "wb");
    ASSERT_NOT_NULL(f, "open crafted file for writing");
    if (!f) return 1;

    const size_t CHUNK = 33554432; /* 32 MiB default odirect_buf_size */
    const size_t PAD_RECORD = 24;
    size_t n_pad = (CHUNK - 8) / PAD_RECORD; /* 1,398,101 */

    for (size_t i = 0; i < n_pad; i++) {
        write_header(f, 0, 2 /* tombstone */, 0);
    }
    long pos_before_bad = ftell(f);
    ASSERT_EQ_INT((int)((size_t)pos_before_bad % CHUNK), (int)(CHUNK - 8),
                  "padding lands exactly 8 bytes before the chunk boundary");

    /* Malicious record: klen=0, flag=1 (live), vlen=0xFFFFFFF0. */
    write_header(f, 0, 1, 0xFFFFFFF0u);

    fclose(f);

    struct stat st;
    ASSERT_EQ_INT(stat(path, &st), 0, "crafted file exists");
    ASSERT_EQ_INT((int)st.st_size, (int)(n_pad * PAD_RECORD + 24),
                  "crafted file is the expected size");

    g_cb_calls = 0;
    int rc = seg_scan_o_direct_varlen(path, capture_cb, NULL);

    /* Pre-fix: rec_size (4294967304) truncates to int(8); with carry_len
       already at 24 by the time Stage 2 runs, need = 8 - 24 = -16 <= 0,
       so the "need more data" branch is skipped and capture_cb is called
       with vlen=0xFFFFFFF0 — g_cb_calls would go to 1 and rc would be 0.
       Post-fix: rec_size > SLOTCASK_SEG_MAX_BYTES is caught before the
       narrowing cast, the scan aborts with -EIO, and capture_cb is never
       reached for the malicious record. */
    ASSERT_TRUE(rc != 0, "scan reports an error for the corrupted record");
    ASSERT_EQ_INT(g_cb_calls, 0, "callback never invoked with the lying vlen");

    unlink(path);
    return 0;
}

TEST_REGISTER("test-coverity-seg-scan-varlen-overflow", test_coverity_seg_scan_varlen_overflow_run);
```

The test above includes `io_direct.h` directly (matching the pattern already used by `src/test/cases/test_o_direct_scan.c`) rather than hand-declaring a local typedef/extern, so `capture_cb` is checked against the real `od_record_cb` typedef (`typedef int (*od_record_cb)(const uint8_t *rec, size_t vlen, const uint8_t hash16[16], void *ctx);`) and `seg_scan_o_direct_varlen`'s real prototype at compile time — a signature mismatch will be a compile error, not a silent bug. If it fails to compile, run `grep -n "od_record_cb\|seg_scan_o_direct_varlen" src/db/io_direct.h` and fix the call site to match — do not add a competing typedef.

Now append this fourth file to `build.sh`. Find this exact block (as left by Task 5's edit):

```
    src/test/cases/test_secure_random_keys.c \
    src/test/cases/test_coverity_disk_corruption_segments.c \
    src/test/cases/test_coverity_disk_corruption_btree.c \
    src/test/cases/test_coverity_disk_corruption_bitmap.c \
```

Replace it with:

```
    src/test/cases/test_secure_random_keys.c \
    src/test/cases/test_coverity_disk_corruption_segments.c \
    src/test/cases/test_coverity_disk_corruption_btree.c \
    src/test/cases/test_coverity_disk_corruption_bitmap.c \
    src/test/cases/test_coverity_seg_scan_varlen_overflow.c \
```

---

## Task 7 — `bt_alloc_page` NULL/MAP_FAILED handling on grow failure (CID 1696467)

### The bug

`src/db/btree.c`'s `bt_alloc_page` handles a failed `mremap` (Linux) by setting `bt->map = NULL` "to force SIGBUS on subsequent write," but the non-Linux `mmap` branch doesn't check for `MAP_FAILED` at all — and even on the Linux branch, letting execution continue with `bt->map == NULL` means `fh = (BtFileHeader *)bt->map;` immediately below dereferences NULL instead of actually stopping:

```c
static uint32_t bt_alloc_page(BtFile *bt) {
    BtFileHeader *fh = (BtFileHeader *)bt->map;
    uint32_t new_id = fh->page_count;
    size_t needed = (size_t)(new_id + 1) * bt_page_size;

    if (needed > bt->map_size) {
        size_t new_size = bt->map_size * 2;
        if (new_size < bt->map_size + 1024 * 1024)
            new_size = bt->map_size + 1024 * 1024;
        if (new_size < needed) new_size = needed;
#ifdef __linux__
        bt->map = mremap(bt->map, bt->map_size, new_size, MREMAP_MAYMOVE);
        if (bt->map == MAP_FAILED) {
            fprintf(stderr, "btree: mremap(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
            bt->map = NULL; /* force SIGBUS on subsequent write */
        }
#else
        munmap(bt->map, bt->map_size);
        bt->map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, bt->fd, 0);
#endif
        if (ftruncate(bt->fd, (off_t)new_size) < 0) {
            fprintf(stderr, "btree: ftruncate(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
        }
        bt->map_size = new_size;
        fh = (BtFileHeader *)bt->map;
        if (bt->slot >= 0) {
            BtCacheEntry *e = &bt_cache[bt->slot];
            e->map = bt->map;
            e->map_size = bt->map_size;
        }
    }

    fh->page_count = new_id + 1;

    uint8_t *pg = bt_page(bt, new_id);
    memset(pg, 0, bt_page_size);
    return new_id;
}
```

11 call sites across the codebase call `bt_alloc_page` without checking any return-value error sentinel (there isn't one — it always returns a `uint32_t` page id). There is no fault-injection mechanism to safely and deterministically simulate an `mremap`/`mmap` failure in a test. Given that, the least-bad fix is to fail loudly and immediately via `abort()` rather than silently corrupting the B+tree by continuing with a NULL or partially-updated map.

### The fix

In `src/db/btree.c`, find this exact block:

```c
#ifdef __linux__
        bt->map = mremap(bt->map, bt->map_size, new_size, MREMAP_MAYMOVE);
        if (bt->map == MAP_FAILED) {
            fprintf(stderr, "btree: mremap(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
            bt->map = NULL; /* force SIGBUS on subsequent write */
        }
#else
        munmap(bt->map, bt->map_size);
        bt->map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, bt->fd, 0);
#endif
        if (ftruncate(bt->fd, (off_t)new_size) < 0) {
            fprintf(stderr, "btree: ftruncate(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
        }
        bt->map_size = new_size;
        fh = (BtFileHeader *)bt->map;
```

Replace it with:

```c
#ifdef __linux__
        bt->map = mremap(bt->map, bt->map_size, new_size, MREMAP_MAYMOVE);
        if (bt->map == MAP_FAILED) {
            fprintf(stderr, "btree: mremap(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
            bt->map = NULL;
        }
#else
        munmap(bt->map, bt->map_size);
        bt->map = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, bt->fd, 0);
        if (bt->map == MAP_FAILED) bt->map = NULL;
#endif
        /* No caller of bt_alloc_page (11 sites) checks an error sentinel —
           there isn't one, it always returns a uint32_t page id. Silently
           continuing with a NULL or stale map here would corrupt the
           B+tree on the next access. Fail loudly instead (CID 1696467). */
        if (!bt->map) {
            fprintf(stderr, "btree: page allocation failed, aborting\n");
            abort();
        }
        if (ftruncate(bt->fd, (off_t)new_size) < 0) {
            fprintf(stderr, "btree: ftruncate(grow %zu→%zu) failed: %s\n",
                    bt->map_size, new_size, strerror(errno));
        }
        bt->map_size = new_size;
        fh = (BtFileHeader *)bt->map;
```

### Regression test

No new dedicated test. There is no safe, deterministic way in this codebase to force `mremap`/`mmap` to fail (no `LD_PRELOAD` fault-injection harness, and deliberately exhausting address space or memory in a CI-run unit test is unsafe/flaky). Rely on the full test suite continuing to pass — this fix only changes behavior on the already-fatal failure path; all successful-growth behavior is untouched.

---

## Task 8 — `mf_append_field` unvalidated varchar length before trigram extraction (CID 1696428)

### The bug

`src/db/index.c`'s `mf_append_field`, in its `STREAM_TRIGRAM` branch, reads an on-disk varchar length prefix and passes it straight to `tg_extract_distinct` without clamping it to the field's actual declared on-disk size:

```c
static void mf_append_field(MFWorkerField *f, const MFFieldDesc *d,
                            const uint8_t hash16[16], const uint8_t *value,
                            TypedSchema *ts, int splits, int idx_n) {
    if (d->type == STREAM_TRIGRAM) {
        int tidx = d->field_indices[0];
        const TypedField *tf = &ts->fields[tidx];
        if (tf->type != FT_VARCHAR) return;
        const uint8_t *vb = value + tf->offset;
        uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
        if (al == 0) return;
        uint8_t tg[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
        if (n == 0) return;
        ...
```

`al` (the "actual length" read from the 2-byte on-disk prefix) is not validated against `tf->size` (the field's fixed on-disk width, `content + 2` bytes). A corrupted on-disk length prefix larger than `tf->size - 2` sends `tg_extract_distinct` reading `al` bytes starting at `vb + 2`, past the end of the field's actual allocated region in `value`.

### The fix

In `src/db/index.c`, find this exact block:

```c
        const uint8_t *vb = value + tf->offset;
        uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
        if (al == 0) return;
        uint8_t tg[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
```

Replace it with:

```c
        const uint8_t *vb = value + tf->offset;
        uint16_t al = ((uint16_t)vb[0] << 8) | (uint16_t)vb[1];
        if (al == 0) return;
        /* al is an on-disk length prefix; clamp it to the field's actual
           declared content size before using it to read past vb + 2
           (CID 1696428). */
        size_t max_content = tf->size > 2 ? (size_t)tf->size - 2 : 0;
        if ((size_t)al > max_content) al = (uint16_t)max_content;
        if (al == 0) return;
        uint8_t tg[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vb + 2, al, tg, TG_MAX_DISTINCT);
```

### Regression test

Covered by `test-coverity-reindex-trigram-overflow` in `test_coverity_disk_corruption_segments.c` (Task 5 above).

---

## Task 9 — `tg_estimate_cb` unvalidated varchar length before trigram extraction (CID 1696427)

### The bug

`src/db/query_maint.c`'s `tg_estimate_cb` (used by the `estimate-index` JSON mode) has the identical unvalidated-length pattern as CID 1696428, on a different call path (per-record sampling rather than streaming reindex):

```c
static int tg_estimate_cb(uint32_t slot, const uint8_t hash16[16],
                          const void *key, size_t klen,
                          const void *value, size_t vlen,
                          void *ctx) {
    (void)slot; (void)hash16; (void)key; (void)klen; (void)vlen;
    TgEstimateCtx *c = (TgEstimateCtx *)ctx;
    if (c->sampled >= c->max_sample) return -1;
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
    uint16_t actual_len = ((uint16_t)vbase[0] << 8) | (uint16_t)vbase[1];
    if (actual_len > 0) {
        uint8_t trigrams[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vbase + 2, actual_len,
                                       trigrams, TG_MAX_DISTINCT);
        c->distinct_sum += n;
    }
    c->sampled++;
    return 0;
}
```

### The fix

In `src/db/query_maint.c`, find this exact block:

```c
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
    uint16_t actual_len = ((uint16_t)vbase[0] << 8) | (uint16_t)vbase[1];
    if (actual_len > 0) {
        uint8_t trigrams[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vbase + 2, actual_len,
                                       trigrams, TG_MAX_DISTINCT);
        c->distinct_sum += n;
    }
```

Replace it with:

```c
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
    uint16_t actual_len = ((uint16_t)vbase[0] << 8) | (uint16_t)vbase[1];
    /* actual_len is an on-disk length prefix; clamp it to the field's
       actual declared content size before reading past vbase + 2
       (CID 1696427), same pattern as mf_append_field in index.c. */
    size_t max_content = f->size > 2 ? (size_t)f->size - 2 : 0;
    if ((size_t)actual_len > max_content) actual_len = (uint16_t)max_content;
    if (actual_len > 0) {
        uint8_t trigrams[TG_MAX_DISTINCT][3];
        size_t n = tg_extract_distinct(vbase + 2, actual_len,
                                       trigrams, TG_MAX_DISTINCT);
        c->distinct_sum += n;
    }
```

### Regression test

Covered by `test-coverity-estimate-index-overflow` in `test_coverity_disk_corruption_segments.c` (Task 5 above).

---

## Task 10 — `bm_dict_add` missing hard-ceiling guard (CID 1696430)

### The bug

`src/db/bitmap.c`'s `bm_dict_add` computes a new dictionary size from a caller-supplied `vlen` with no check against `BM_HARD_CEILING` (65535), unlike the read-side `bm_dict_lookup`/`bm_dict_used_bytes`, which both bound-check against the mapped region:

```c
static int bm_dict_add(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    const uint8_t *old = (const uint8_t *)bm->mmap_ptr;
    uint32_t old_n = bm->hdr.n_values;
    if (old_n >= bm->hdr.max_values) return -1;
    uint32_t old_dict_used = bm_dict_used_bytes(bm);
    uint32_t new_dict_used = old_dict_used + 2u + (uint32_t)vlen;
    ...
```

As established in the Background section, this is currently unreachable in practice — varchar's on-disk max content (65535 bytes, from its uint16 length prefix) exactly coincides with `BM_HARD_CEILING`, so no legitimately-encoded value can ever arrive here with `vlen > 0xffff`. This is a defense-in-depth fix for future field types or callers that might not share that coincidental ceiling.

### The fix

In `src/db/bitmap.c`, find this exact block:

```c
static int bm_dict_add(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    const uint8_t *old = (const uint8_t *)bm->mmap_ptr;
    uint32_t old_n = bm->hdr.n_values;

    /* Enforce the per-file cardinality contract. Past `max_values`,
       bitmap isn't the right index for this dataset — btree is, or the
       operator can declare a higher cap at create-object via
       `field:bitmap(N)`. The wire layer translates this -1 into an
       actionable error pointing them at the override. */
    if (old_n >= bm->hdr.max_values) return -1;
```

Replace it with:

```c
static int bm_dict_add(BitmapShard *bm, const uint8_t *value, size_t vlen) {
    const uint8_t *old = (const uint8_t *)bm->mmap_ptr;
    uint32_t old_n = bm->hdr.n_values;

    /* Enforce the per-file cardinality contract. Past `max_values`,
       bitmap isn't the right index for this dataset — btree is, or the
       operator can declare a higher cap at create-object via
       `field:bitmap(N)`. The wire layer translates this -1 into an
       actionable error pointing them at the override. */
    if (old_n >= bm->hdr.max_values) return -1;
    /* Defense-in-depth: the dict's uint16 length-prefix format can't
       represent a value longer than 65535 bytes. Today every real caller
       already stays under this via varchar's own 65535-byte on-disk
       ceiling, but don't rely on that coincidence holding for future
       field types (CID 1696430). */
    if (vlen > 0xffff) return -1;
```

### Regression test

No new dedicated test — confirmed unreachable via any current real caller given the varchar/bitmap 65535-byte ceiling coincidence described above. Rely on the full test suite continuing to pass.

---

## Task 11 — `bm_dict_used_bytes` unbounded dictionary walk (CID 1696403)

### The bug

`src/db/bitmap.c`'s `bm_dict_used_bytes` walks the on-disk dictionary using `bm->hdr.n_values` (an on-disk header field) as the loop bound, with no check that each entry's `[len][bytes]` pair actually stays within the mapped region — unlike its sibling `bm_dict_lookup`, which does check:

```c
static uint32_t bm_dict_used_bytes(const BitmapShard *bm) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) return 6;
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    uint32_t off = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        uint16_t len = (uint16_t)p[off] | ((uint16_t)p[off + 1] << 8);
        off += 2u + len;
    }
    return off;
}

static int bm_dict_lookup(const BitmapShard *bm, const uint8_t *value, size_t vlen) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) { ... }
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    const uint8_t *end = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        if (p + 2 > end) return -1;
        uint16_t len = (uint16_t)p[0] | ((uint16_t)p[1] << 8);
        if (p + 2 + len > end) return -1;
        if (len == vlen && memcmp(p + 2, value, vlen) == 0) return (int)i;
        p += 2 + len;
    }
    return -1;
}
```

`bm_dict_used_bytes` is called by `bm_dict_add` using `bm->hdr.n_values` read directly from the mmap'd file header. This is independently reachable via corruption of just the `.bm` file's `n_values` header field (offset 12) — no oversized single value is needed (unlike CID 1696430 above), so this is a genuinely reachable, testable bug, not just defense-in-depth.

### The fix

In `src/db/bitmap.c`, find this exact block:

```c
static uint32_t bm_dict_used_bytes(const BitmapShard *bm) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) return 6; /* [01 00 00][01 00 01] */
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    uint32_t off = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        uint16_t len = (uint16_t)p[off] | ((uint16_t)p[off + 1] << 8);
        off += 2u + len;
    }
    return off;
}
```

Replace it with:

```c
static uint32_t bm_dict_used_bytes(const BitmapShard *bm) {
    if (bm->hdr.flags & BM_FLAG_BOOL_FASTPATH) return 6; /* [01 00 00][01 00 01] */
    const uint8_t *p = (const uint8_t *)bm->mmap_ptr + bm->hdr.dict_off;
    const uint8_t *end = (const uint8_t *)bm->mmap_ptr + bm->hdr.bitmaps_off;
    uint32_t off = 0;
    for (uint32_t i = 0; i < bm->hdr.n_values; i++) {
        /* n_values is an on-disk header field with no inherent bound;
           mirror bm_dict_lookup's bounds-checked walk instead of trusting
           it blindly (CID 1696403). A corrupted/oversized n_values now
           just truncates the walk at the mapped region's edge instead of
           reading past it. */
        if (p + off + 2 > end) break;
        uint16_t len = (uint16_t)p[off] | ((uint16_t)p[off + 1] << 8);
        if (p + off + 2 + len > end) break;
        off += 2u + len;
    }
    return off;
}
```

### Regression test

Covered by `test-coverity-bitmap-nvalues-corruption` in `test_coverity_disk_corruption_bitmap.c` (Task 5 above).

---

## Final verification

After all 11 tasks above are complete and `SKIP_TESTS=1 ./build.sh` succeeds, run:

```bash
./build/bin/shard-db-test run-all --filter coverity
./build/bin/shard-db-test run-all
```

Paste the **real terminal output** of both commands. The full-suite run must show `# total: N passed, 0 failed` with N equal to the previous total plus the 7 new test cases added in this plan (`test-coverity-seg-klen-corruption`, `test-coverity-reindex-trigram-overflow`, `test-coverity-estimate-index-overflow`, `test-coverity-btree-nextleaf-cycle`, `test-coverity-btree-leafcount-overflow`, `test-coverity-bitmap-nvalues-corruption`, `test-coverity-seg-scan-varlen-overflow` — these 7 cover 8 of the 12 CIDs since Task 4's single test covers both CID 1696448 and CID 1696465; the pre-existing `test-variable-length` continues to pass unchanged as Task 1's regression guard, not as a new case). Do not consider this plan done, and do not report success, without pasting this real output.
