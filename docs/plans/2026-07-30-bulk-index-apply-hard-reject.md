# Bulk-insert index-apply failures: hard-reject + revert instead of silent partial commit

**Status: SUPERSEDED — do NOT execute.** The revised, authoritative plan is
[`2026-07-31-index-integrity-revision.md`](2026-07-31-index-integrity-revision.md).
This draft remains only as investigation history. It was superseded because
its Task 3 discarded the only recovery evidence after a best-effort inverse
index update, and because it did not address destructive B-tree replacement.
Per explicit instruction, the revised plan still needs additional review
("more eyes") before any branch is cut. Plan
`2026-07-30-btree-bulk-merge-tie-duplicate.md` is being executed separately
first; this plan is independent of that one (different bug, different code
paths within the same two files) but touches an adjacent region of
`btree.c` — rebase onto whatever lands from that plan before executing this
one, and re-verify the anchors below still match.

## Motivation / invariant being enforced

User directive (verbatim), which is the binding correctness requirement for
this plan:

> but if index updates fail, thats a much bigger problem than fixing some
> symptom, or even showing an error in a log, if an error happens, the
> records cant be accessed. Bigger issue, every single record must be
> having its required indexes. if we hit failure, it should be a hard
> failure and reject everything.

And, on the chosen remediation shape (Option B — hard reject, no
synchronous replay-retry):

> option B makes sense. lets just reject but we make sure to revert the
> insert if kf is already committed.

Concretely: **no record may ever become visible (kf-committed) without
every one of its required index entries also being committed**, and a
bulk-insert window that fails partway through index application must leave
**zero** trace — no kf entry, no partial index entries, no retained replay
marker — rather than a mix of "some records indexed, some not" or "record
visible but under-indexed."

## Root cause

Two independent defects combine to violate that invariant today, both in
the indexed bulk-insert path (`v2` slotcask backend, `has_indexed_fields=1`,
i.e. `slotcask_bulk_upsert_in_kfshard`'s two-phase window protocol).

### Defect A — `bt_extract_all` conflates "empty tree" with "unreadable tree"

`src/db/btree.c:3068-3136`, `bt_extract_all`, is the read-side of
`btree_bulk_merge`'s extract-merge-rebuild strategy. It returns `NULL` /
`*out_count = 0` for **four different conditions**, indistinguishably:

```c
static BtEntry *bt_extract_all(const char *path, size_t *out_count) {
    *out_count = 0;

    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return NULL;                         // (1) transient acquire failure
    if (bt.map_size < (size_t)bt_page_size * 2) { bt_release(&bt); return NULL; }  // (2) truncated/corrupt file

    BtFileHeader *fh = (BtFileHeader *)bt.map;
    if (fh->magic != BT_MAGIC || fh->entry_count == 0) {
        bt_release(&bt); return NULL;                                       // (3) bad magic, or (4) genuinely empty
    }
    ...
    if (!entries) {
        LOG_ERROR(...);
        bt_release(&bt); return NULL;                                       // malloc failure — also folded in
    }
    ...
    while (leaf_iter_next(&it)) {
        if (count >= cap) {
            cap *= 2;
            BtEntry *tmp = realloc(entries, cap * sizeof(BtEntry));
            if (!tmp) goto extract_done;                                    // mid-scan OOM — returns a TRUNCATED
        }                                                                   // non-NULL array with count < entry_count,
        char *vcopy = malloc(it.key_len + 1);                               // no signal that it's short
        if (!vcopy) goto extract_done;
        ...
    }
extract_done:
    bt_release(&bt);
    *out_count = count;
    return entries;
}
```

`bt_acquire` can genuinely fail under load — real `open()`/`mmap()` syscall
failures (EMFILE/ENFILE, mmap ENOMEM) and, more subtly, the cache's bounded
4-attempt eviction-slot-reuse retry (`bt_acquire`, `src/db/btree.c`
~lines 683-840) can still exhaust and fall through to a genuine miss under
concurrent parallel bulk-insert pressure. None of that is "the file doesn't
exist yet" — it's "the file exists, has entries, and we couldn't read them
right now."

`btree_bulk_merge` (`src/db/btree.c:3175-3274`) treats `exist_count == 0`
as unconditional proof of an empty/new tree:

```c
    existing = bt_extract_all(path, &exist_count);

    btree_test_after_extract();

    if (exist_count == 0) {
        rc = btree_bulk_build_locked(path, new_entries, new_count);   // <-- rebuilds the file from
        goto done;                                                    //     new_entries ONLY, returns rc=0
    }
```

If the tree was **not** actually empty — a transient `bt_acquire` failure,
a truncated file, or a mid-scan OOM that only got a partial prefix — this
silently rebuilds the shard's `.idx` file to contain **only the new
batch**, discarding every pre-existing entry, and reports **success**
(`rc = 0`). This is a stronger candidate for "missing index entries under
parallel stress" than a logged failure would be, precisely because it
produces no error signal at all.

### Defect B — kf is committed unconditionally, regardless of `apply_window`'s outcome

`src/db/slotcask.c`, inside `slotcask_bulk_upsert_in_kfshard`'s two-phase
window loop (the branch used whenever
`opts->has_indexed_fields && opts->pre_commit`, i.e. every indexed
bulk-insert):

```c
                if (napply_active > 0) {
                    if (opts->apply_window(recs, apply_active, napply_active, opts->bulk_hook_ctx) != 0) {
                        keep_marker = 1;
                        if (opts->out_durability_degraded)
                            *opts->out_durability_degraded = 1;
                    }
                } else if (nactive > 0 && opts->abort_window) {
                    opts->abort_window(opts->bulk_hook_ctx);
                }

                durability_test_pause(db->data_dir, "bulk-window-applied");

                size_t vslots[BULK_COMMIT_MAX_RECORDS];
                size_t nvslots = 0;
                for (size_t a = 0; a < napply_active; a++) {
                    size_t j = apply_active[a];
                    SlotcaskBulkRec *r = &recs[j];
                    size_t pub_slot;
                    if (st[j].old_found) {
                        pub_slot = st[j].old_kf_slot;
                        kf_repoint_at_slot(&kh, st[j].old_kf_slot, ...);
                    } else {
                        ...
                        kf_commit_planned_slot(&kh, &plan, ...);
                    }
                    r->kf_shard = kf_shard_id;
                    r->kf_slot  = (uint32_t)pub_slot;
                    vslots[nvslots++] = pub_slot;
                }
```

`apply_window`'s failure sets `keep_marker`/`out_durability_degraded` but
does **not** gate the kf-commit loop below it — `vslots`/`kf_commit_planned_slot`/
`kf_repoint_at_slot` run unconditionally over the same `apply_active`
records regardless of whether `apply_window` reported success. So a record
whose indexing failed still gets published (kf entry committed, visible to
readers) — the opposite of the required invariant.

Compounding this: `v2_bulk_ins_apply_window` (`src/db/query_bulk.c:833-903`)
documents and implements a **"keep going, report at the end"** policy —
trigram ops, B-tree deletes, and every indexed field's `btree_bulk_merge`
all run unconditionally in sequence even after an earlier one fails,
aggregating into one `rc`:

```c
/* apply_window fires once the window's kf marker is durable, before kf is
 * committed for the surviving records. Performs the real trigram, B-tree,
 * and bitmap mutations staged by prepare_window, in that order, always
 * running every staged op even if an earlier one fails (mirrors
 * bitmap_prepare_window_apply's own "keep going, report at the end"
 * pattern) so a partial apply doesn't leave some of this window's
 * surviving records indexed and others not. ...
```

So a failed window is **not** "zero index entries applied" — it's
routinely a **partial** apply (some fields/shards succeeded, one failed).
Combined with Defect B's unconditional kf-commit, today's failure mode is:
kf published for every record in the window, with an unpredictable subset
of that window's index entries actually present. This is exactly the
wrong-count / missing-index-entry symptom under parallel stress.

## Design (Option B: hard reject + revert, no synchronous replay)

On `apply_window` failure for a window:

1. **Do not run the kf-commit loop** for that window's `apply_active`
   records — restructure so kf-commit is conditional on `apply_window`
   succeeding. `apply_window` is called once for the whole window, strictly
   before kf-commit begins for any record in it, so this alone makes kf
   commit structurally unreachable on failure.
2. **Revert whatever `apply_window` partially applied.** Since it may have
   durably written some index entries before failing (the documented
   "keep going" policy), call the same index-diff machinery crash recovery
   already uses (`g_recovery_index_diff_fn` → `apply_index_diff`,
   `src/db/storage.c:1143-1267`), in the **reverse** direction: diff FROM
   the content that was just attempted (`recs[j].value`/`vlen`, the NEW
   content) TO the record's actual surviving content
   (`recs[j].old_value`/`old_vlen` — the pre-existing content for an
   update, or `NULL` for a fresh insert). `apply_index_diff` is generic
   over index type (btree/trigram/bitmap all dispatch through
   `update_idx_fn`, selected by `IndexDiffApplyArgs.type`), so one call
   per record reverts every index type uniformly — no per-type undo logic
   needed. It's also naturally idempotent/safe against fields
   `apply_window` never actually reached (diffing "no change" for an
   untouched field produces no op).
3. **Reclaim the segment slot** for each reverted record using the exact
   idiom already used elsewhere in this same function for prepare-phase
   rejections: `seg_write_flag(..., flag=2)` (tombstone) +
   `pool_push_free(...)`.
4. **Discard the batch marker** — reuse the existing discard idiom
   (`unlink(bpath)` + `fsync_dir(dpath)`, `kf_marker_fail_closed` on
   failure to durably discard) rather than retaining it for replay. There
   is no deferred-replay path for this failure class per Option B.
5. **Mark every record in the window `status = -1`** so the bulk-insert
   caller reports real failures to the client instead of silent success.

### Why revert instead of "just skip kf-commit"

An orphaned index entry (present in a `.idx`/trigram/bitmap shard, no
matching kf entry) is not inert — `PRIMARY_KEYSET`-style pure-OR count
paths and `PRIMARY_INTERSECT` return counts straight from the index
without a per-record kf fetch in some paths, and even where a fetch does
happen, a hash with no kf entry is exactly the "missing record, index says
it's there" corruption class this whole investigation started from.
Skipping kf-commit without reverting the index side would still leave
orphans — the revert step is required, not optional, to satisfy "every
single record must be having its required indexes" (which implies the
converse: no dangling indexes for records that don't exist).

### Insert vs. update revert semantics

- **Fresh insert** (`st[j].old_found == 0`): `recs[j].old_value == NULL`.
  Revert diffs NEW → NULL for every indexed field, i.e. removes anything
  `apply_window` partially wrote. Nothing else to restore — the key never
  existed.
- **Update** (`st[j].old_found == 1`): `recs[j].old_value` holds the
  pre-existing content (auto-populated by the bulk primitive at prepare
  time — see `slotcask.c` prepare-window loop). Revert diffs
  attempted-NEW → OLD, which both removes anything wrongly added for the
  new value AND is a no-op for fields that didn't change (same value on
  both sides of the diff → `changed == 0` in `apply_index_diff` → no op).
  kf is never repointed (Defect B fix means `kf_repoint_at_slot` doesn't
  run), so the OLD kf entry — and whatever index state matched it before
  this bulk call started — remains exactly as it was. The revert only
  needs to undo what `apply_window` attempted to add for the NEW value;
  it must not touch anything that already correctly reflected OLD.

### Physical slot addressing for the revert call

Bitmap-type indexes address entries by physical `(kf_shard, kf_slot)`, not
by hash (per `V2UpdateCtx`'s doc comment: "bitmap addresses records by
physical slot, not by hash"). Since prepare_window plans (but does not
commit) a slot for every record before the marker is written
(`st[j].plan_slot`, via `kf_plan_window_insert_slot`), and
`bitmap_prepare_window_apply` inside `apply_window` already writes bits
against that planned slot number (not a committed one — kf commit hasn't
happened yet even in the success path at that point in the call sequence),
the revert call must address the same planned slot:
`st[j].old_found ? st[j].old_kf_slot : st[j].plan_slot`.

## Call sites (confirmed via grep, 2026-07-30)

- `bt_extract_all` — **1 call site**, `btree_bulk_merge` (same file,
  `static` function, no external callers). Signature change is internal-only.
- `btree_bulk_merge` — production call sites: `src/db/query_bulk.c:27`
  (`idx_build_worker`, currently unused/`__attribute__((unused))`) and
  `src/db/query_bulk.c:95` (`idx_build_field_worker`, the live bulk-insert
  path). Return type/signature unchanged — callers already check `!= 0`.
  Test call sites (signature-compatible, no changes needed):
  `test_bt_cache_writer_starvation.c`, `test_btree_bulk_merge_delete_race.c`,
  `test_btree_value_hash_sort.c`.
- `slotcask_bulk_upsert_in_kfshard` — only reachable from the v2 bulk-insert
  worker (`bulk_insert_shard_worker_v2` → `query_bulk.c`); no other
  production callers.
- `SlotcaskBulkOpts.apply_window` contract doc (`slotcask.h:573-578`) —
  read by any future implementer of the hook; only one implementation
  exists today (`v2_bulk_ins_apply_window`, `query_bulk.c:843-903`), whose
  own doc comment already independently describes the "keep going" policy
  this plan relies on — no code change needed there, only cross-referenced.

## Task 1 — Fix `bt_extract_all`/`btree_bulk_merge` to hard-fail on unreadable trees

### Test first (must fail before the fix, pass after)

New file `src/test/cases/test_btree_extract_corrupt_shard.c`, modeled on
the existing single-threaded `mkstemp`+`.idx` pattern in
`src/test/cases/test_btree_bulk_merge_delete_race.c`:

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int test_btree_bulk_merge_rejects_corrupt_existing_tree(void) {
    char path[] = "/tmp/shard-db-extract-corrupt-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_TRUE(fd >= 0, "create temp .idx file");
    if (fd < 0) return 1;
    close(fd);
    unlink(path); /* btree_bulk_build_locked creates it fresh */

    BtEntry seed[5];
    char seed_vals[5][8];
    for (int i = 0; i < 5; i++) {
        snprintf(seed_vals[i], sizeof(seed_vals[i]), "s%04d", i);
        seed[i].value = seed_vals[i];
        seed[i].vlen = strlen(seed_vals[i]);
        memset(seed[i].hash, (uint8_t)i, BT_HASH_SIZE);
    }
    ASSERT_EQ_INT(btree_bulk_merge(path, seed, 5), 0,
                  "seed a valid 5-entry tree");

    /* Corrupt the file: truncate well below 2*bt_page_size so
       bt_extract_all cannot distinguish this from "genuinely empty" by
       (NULL, 0) alone — this is the exact ambiguity Defect A exploits. */
    ASSERT_EQ_INT(truncate(path, 8), 0, "truncate .idx file to simulate corruption");

    BtEntry batch[2];
    char batch_vals[2][8] = {"n0001", "n0002"};
    for (int i = 0; i < 2; i++) {
        batch[i].value = batch_vals[i];
        batch[i].vlen = strlen(batch_vals[i]);
        memset(batch[i].hash, (uint8_t)(10 + i), BT_HASH_SIZE);
    }

    int rc = btree_bulk_merge(path, batch, 2);
    ASSERT_TRUE(rc != 0,
                "btree_bulk_merge on a corrupt-but-not-empty tree must hard-fail, "
                "not silently rebuild with only the new batch");

    unlink(path);
    return t_ctx->failed ? 1 : 0;
}

TEST_REGISTER("test-btree-bulk-merge-rejects-corrupt-existing-tree",
              test_btree_bulk_merge_rejects_corrupt_existing_tree)
```

Register in `build.sh` — insert right after this exact anchor line (chosen
to avoid the insertion point used by the separately-executing
`2026-07-30-btree-bulk-merge-tie-duplicate.md` plan, which inserts after
`test_btree_bulk_merge_delete_race.c`):

Quoted anchor:
```
    src/test/cases/test_btree_value_hash_sort.c \
```
New text (insert immediately after):
```
    src/test/cases/test_btree_value_hash_sort.c \
    src/test/cases/test_btree_extract_corrupt_shard.c \
```

Run it before the fix and confirm it fails (proves the bug): with today's
code, `btree_bulk_merge` returns `0` because `bt_extract_all` returns
`(NULL, 0)` for the truncated file, indistinguishable from empty, so
`btree_bulk_build_locked(path, batch, 2)` runs and succeeds — the assertion
`rc != 0` fails. Paste that failing run's output before proceeding to the
fix.

### Fix

`src/db/btree.c` — quoted anchor (current `bt_extract_all` signature and
body, `src/db/btree.c:3068-3136`):

```c
static BtEntry *bt_extract_all(const char *path, size_t *out_count) {
    *out_count = 0;

    /* Use the unified btree open path — same rdlock that every other
       reader takes, so a concurrent btree_insert blocks briefly on the
       per-file wrlock rather than racing this MAP_PRIVATE view. The
       caller (btree_bulk_merge) holds the per-path bulk-merge mutex and
       runs under objlock, but going through bt_acquire keeps the access
       pattern uniform with the rest of the read path. */
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) return NULL;
    if (bt.map_size < (size_t)bt_page_size * 2) { bt_release(&bt); return NULL; }

    BtFileHeader *fh = (BtFileHeader *)bt.map;
    if (fh->magic != BT_MAGIC || fh->entry_count == 0) {
        bt_release(&bt); return NULL;
    }

    size_t cap = (size_t)fh->entry_count + 64;
    /* CID 1693855 - header value from trusted index file, triage */
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_extract_all %s: malloc(entries, cap=%zu) failed", path, cap);
        bt_release(&bt); return NULL;
    }
    size_t count = 0;

    /* Walk down to leftmost leaf via next_leaf (= leftmost child for internal) */
    uint32_t page_id = fh->root_page;
    while (1) {
        if ((size_t)page_id * bt_page_size + bt_page_size > bt.map_size) break;
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }

    /* Scan leaf chain — sequential decode via LeafIter */
    while (page_id != 0 && (size_t)page_id * bt_page_size + bt_page_size <= bt.map_size) {
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type != 1) break;

        LeafIter it;
        leaf_iter_init(&it, pg);
        while (leaf_iter_next(&it)) {
            if (count >= cap) {
                cap *= 2;
                BtEntry *tmp = realloc(entries, cap * sizeof(BtEntry));
                if (!tmp) goto extract_done;
                entries = tmp;
            }
            char *vcopy = malloc(it.key_len + 1);
            if (!vcopy) goto extract_done;
            memcpy(vcopy, it.key_buf, it.key_len);
            vcopy[it.key_len] = '\0';
            entries[count].value = vcopy;
            entries[count].vlen = it.key_len;
            memcpy(entries[count].hash, it.hash, BT_HASH_SIZE);
            count++;
        }
        page_id = ph->next_leaf;
    }

extract_done:
    bt_release(&bt);
    *out_count = count;
    return entries;
}
```

Replace with (new `out_failed` output parameter distinguishes "genuinely
empty" from every other reason the scan didn't come back complete):

```c
static BtEntry *bt_extract_all(const char *path, size_t *out_count, int *out_failed) {
    *out_count = 0;
    *out_failed = 0;

    /* Use the unified btree open path — same rdlock that every other
       reader takes, so a concurrent btree_insert blocks briefly on the
       per-file wrlock rather than racing this MAP_PRIVATE view. The
       caller (btree_bulk_merge) holds the per-path bulk-merge mutex and
       runs under objlock, but going through bt_acquire keeps the access
       pattern uniform with the rest of the read path. */
    BtFile bt;
    if (bt_acquire(&bt, path, 0) != 0) { *out_failed = 1; return NULL; }
    if (bt.map_size < (size_t)bt_page_size * 2) {
        /* Too small to even hold a header + one page — either a torn
           create or genuine corruption. A brand-new file that
           btree_bulk_build_locked never touched simply doesn't exist
           (bt_acquire above would have failed to open it), so reaching
           here means a file exists but isn't a valid, complete tree —
           never treat this the same as "no file / empty tree". */
        bt_release(&bt);
        *out_failed = 1;
        return NULL;
    }

    BtFileHeader *fh = (BtFileHeader *)bt.map;
    if (fh->magic != BT_MAGIC) {
        bt_release(&bt);
        *out_failed = 1;
        return NULL;
    }
    if (fh->entry_count == 0) {
        /* Genuinely empty tree — the only condition allowed to report
           success with zero entries. */
        bt_release(&bt);
        return NULL;
    }
    size_t expected_count = fh->entry_count;

    size_t cap = expected_count + 64;
    /* CID 1693855 - header value from trusted index file, triage */
    BtEntry *entries = malloc(cap * sizeof(BtEntry));
    if (!entries) {
        LOG_ERROR(LOG_SUB_BTREE, "bt_extract_all %s: malloc(entries, cap=%zu) failed", path, cap);
        bt_release(&bt);
        *out_failed = 1;
        return NULL;
    }
    size_t count = 0;
    int truncated = 0;

    /* Walk down to leftmost leaf via next_leaf (= leftmost child for internal) */
    uint32_t page_id = fh->root_page;
    while (1) {
        if ((size_t)page_id * bt_page_size + bt_page_size > bt.map_size) break;
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type == 1) break;
        page_id = ph->next_leaf;
    }

    /* Scan leaf chain — sequential decode via LeafIter */
    while (page_id != 0 && (size_t)page_id * bt_page_size + bt_page_size <= bt.map_size) {
        uint8_t *pg = bt.map + (size_t)page_id * bt_page_size;
        BtPageHeader *ph = (BtPageHeader *)pg;
        if (ph->page_type != 1) break;

        LeafIter it;
        leaf_iter_init(&it, pg);
        while (leaf_iter_next(&it)) {
            if (count >= cap) {
                cap *= 2;
                BtEntry *tmp = realloc(entries, cap * sizeof(BtEntry));
                if (!tmp) { truncated = 1; goto extract_done; }
                entries = tmp;
            }
            char *vcopy = malloc(it.key_len + 1);
            if (!vcopy) { truncated = 1; goto extract_done; }
            memcpy(vcopy, it.key_buf, it.key_len);
            vcopy[it.key_len] = '\0';
            entries[count].value = vcopy;
            entries[count].vlen = it.key_len;
            memcpy(entries[count].hash, it.hash, BT_HASH_SIZE);
            count++;
        }
        page_id = ph->next_leaf;
    }

extract_done:
    bt_release(&bt);
    *out_count = count;
    /* A complete, uninterrupted scan of a consistent tree always visits
       exactly entry_count leaves. Either an explicit mid-scan OOM
       (truncated) or a leaf chain that came up short (broken next_leaf
       pointers, count < expected_count without ever hitting the OOM
       path) means the result can't be trusted as the full tree. */
    if (truncated || count != expected_count) *out_failed = 1;
    return entries;
}
```

Quoted anchor (current call site + rebuild decision,
`src/db/btree.c:3226-3236`):

```c
    /* Large batch (or empty tree) — use the rebuild path. */
    qsort(new_entries, new_count, sizeof(BtEntry), bt_cmp_entry);

    existing = bt_extract_all(path, &exist_count);

    btree_test_after_extract();

    if (exist_count == 0) {
        rc = btree_bulk_build_locked(path, new_entries, new_count);
        goto done;
    }
```

Replace with:

```c
    /* Large batch (or empty tree) — use the rebuild path. */
    qsort(new_entries, new_count, sizeof(BtEntry), bt_cmp_entry);

    int extract_failed = 0;
    existing = bt_extract_all(path, &exist_count, &extract_failed);

    btree_test_after_extract();

    if (extract_failed) {
        /* bt_extract_all could not prove the existing tree (if any) was
           read completely and correctly — acquire failure, truncated/
           corrupt header, or a mid-scan allocation failure that only
           got a partial prefix. Every one of those is indistinguishable
           from "genuinely empty" by (NULL, 0) alone; treating it as
           empty would silently rebuild this shard with ONLY
           new_entries, discarding whatever pre-existing entries were
           unreadable. Every record's required indexes must survive, or
           the whole merge must fail loudly — never emit a smaller tree
           than what was already durable. */
        rc = -1;
        errno = EIO;
        goto done;
    }

    if (exist_count == 0) {
        rc = btree_bulk_build_locked(path, new_entries, new_count);
        goto done;
    }
```

No other change needed in `btree_bulk_merge` — the `done:` cleanup already
frees `existing`/`exist_count` uniformly (`exist_count` reflects exactly
how many entries in `existing[]` have a live `.value` allocation,
regardless of `extract_failed`, since `count` only increments after a
successful per-entry allocation).

### Verify test-first proof

After applying the fix, re-run
`./build/bin/shard-db-test run test-btree-bulk-merge-rejects-corrupt-existing-tree`
and paste the passing output next to the earlier failing-before-fix output.

## Task 2 — Log the (now much rarer, but still possible) `idx_build_field_worker` failure

Small, low-risk addition — the two alloc-failure branches in this function
already `LOG_ERROR`; the `btree_bulk_merge` failure branch does not.

Quoted anchor (`src/db/query_bulk.c:90-100`):

```c
    /* Serial per-shard bulk_merge — same ops as before, just in one thread. */
    for (int s = 0; s < idx_n; s++) {
        if (counts[s] == 0) continue;
        char path[PATH_MAX];
        build_idx_path(path, sizeof(path), fa->db_root, fa->object, fa->field, s);
        if (btree_bulk_merge(path, parted + offsets[s], counts[s]) != 0) {
            fa->out_error = -1;
            fa->out_errno = errno;
            break;
        }
    }
```

Replace with:

```c
    /* Serial per-shard bulk_merge — same ops as before, just in one thread. */
    for (int s = 0; s < idx_n; s++) {
        if (counts[s] == 0) continue;
        char path[PATH_MAX];
        build_idx_path(path, sizeof(path), fa->db_root, fa->object, fa->field, s);
        if (btree_bulk_merge(path, parted + offsets[s], counts[s]) != 0) {
            LOG_ERROR(LOG_SUB_QUERY,
                      "idx_build_field_worker: btree_bulk_merge failed for field %s shard %d/%d (errno=%d)",
                      fa->field, s, idx_n, errno);
            fa->out_error = -1;
            fa->out_errno = errno;
            break;
        }
    }
```

No dedicated regression test for this task alone — Task 3's end-to-end
test (below) exercises this exact code path and would show a missing log
line under manual inspection if this regressed; a dedicated test asserting
on log output isn't this codebase's pattern (no existing test greps daemon
logs) and isn't worth adding infrastructure for a one-line diagnostic.

## Task 3 — Hard-reject + revert in `slotcask_bulk_upsert_in_kfshard`

### Fix

`src/db/slotcask.h` — quoted anchor (current `apply_window` doc comment,
part of the larger comment block at `slotcask.h:573-578`):

```c
 * apply_window — fires once per window, AFTER the batch marker is durable,
 *   BEFORE kf is committed for the window's surviving records. Performs
 *   the actual index mutations for every record in active[]. A non-zero
 *   return is always a genuine failure (I/O/OOM), never a policy
 *   rejection — routed through the existing degraded/replay path,
 *   unchanged.
```

Replace with:

```c
 * apply_window — fires once per window, AFTER the batch marker is durable,
 *   BEFORE kf is committed for the window's surviving records. Performs
 *   the actual index mutations for every record in active[]. A non-zero
 *   return is always a genuine failure (I/O/OOM), never a policy
 *   rejection. The caller treats it as a hard reject for the whole
 *   window: kf is never committed for active[]'s records, whatever
 *   index mutations apply_window did manage to apply before failing are
 *   reverted via g_recovery_index_diff_fn, the window's segment slots
 *   are reclaimed, and the batch marker is discarded rather than kept
 *   for replay — every record either ends up fully indexed and visible,
 *   or not committed at all, never partially indexed and visible.
```

`src/db/slotcask.c` — quoted anchor (current window-apply + kf-commit
block, `src/db/slotcask.c:5823-5888`):

```c
                size_t apply_active[BULK_COMMIT_MAX_RECORDS];
                size_t napply_active = 0;
                for (size_t a = 0; a < nsurvive; a++) {
                    size_t j = survive[a];
                    if (recs[j].status == 0) apply_active[napply_active++] = j;
                }

                /* ---- apply_window: fires once the window's batch marker
                   is durable, before kf is committed for the surviving
                   records. A nonzero return is always a genuine I/O/OOM
                   failure — routed through the same degraded/replay path
                   as a kfcache_sync_slots_locked failure below, never a
                   policy rejection (those are handled by prepare_window,
                   above, before the marker existed). */
                if (napply_active > 0) {
                    if (opts->apply_window(recs, apply_active, napply_active, opts->bulk_hook_ctx) != 0) {
                        keep_marker = 1;
                        if (opts->out_durability_degraded)
                            *opts->out_durability_degraded = 1;
                    }
                } else if (nactive > 0 && opts->abort_window) {
                    /* prepare_window staged resources (open bitmap writer
                       handles, tracked buffers, queued ops) for this window
                       but every record was rejected before or during marker
                       write — apply_window will never run, so release them
                       here instead of leaking them. */
                    opts->abort_window(opts->bulk_hook_ctx);
                }

                durability_test_pause(db->data_dir, "bulk-window-applied");

                size_t vslots[BULK_COMMIT_MAX_RECORDS];
                size_t nvslots = 0;
                for (size_t a = 0; a < napply_active; a++) {
                    size_t j = apply_active[a];
                    SlotcaskBulkRec *r = &recs[j];
                    size_t pub_slot;
                    if (st[j].old_found) {
                        pub_slot = st[j].old_kf_slot;
                        kf_repoint_at_slot(&kh, st[j].old_kf_slot,
                                            st[j].target_stream,
                                            st[j].target_fid, st[j].target_off);
                    } else {
                        KfInsertPlan plan;
                        memcpy(plan.hash, st[j].hash, sizeof(plan.hash));
                        plan.target_slot = st[j].plan_slot;
                        plan.reused_tomb = st[j].plan_reused_tomb;
                        plan.key = r->key;
                        plan.klen = r->klen;
                        size_t used_delta = 0;
                        pub_slot = 0;
                        kf_commit_planned_slot(&kh, &plan, st[j].target_stream,
                                                st[j].target_fid, st[j].target_off,
                                                &used_delta, &pub_slot);
                    }
                    r->kf_shard = kf_shard_id;
                    r->kf_slot  = (uint32_t)pub_slot;
                    vslots[nvslots++] = pub_slot;
                }

                if (nvslots > 0 &&
                    kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                    keep_marker = 1;
                    if (opts->out_durability_degraded)
                        *opts->out_durability_degraded = 1;
                }
```

Replace with:

```c
                size_t apply_active[BULK_COMMIT_MAX_RECORDS];
                size_t napply_active = 0;
                for (size_t a = 0; a < nsurvive; a++) {
                    size_t j = survive[a];
                    if (recs[j].status == 0) apply_active[napply_active++] = j;
                }

                /* ---- apply_window: fires once the window's batch marker
                   is durable, before kf is committed for the surviving
                   records. A nonzero return is always a genuine I/O/OOM
                   failure. Every record indexing was attempted for must
                   end up with every one of its required indexes — there
                   is no "commit now, catch up later" for this class of
                   failure. A failed window is hard-rejected: apply_window
                   may have partially applied some fields before failing
                   (its own "keep going, report at the end" policy — see
                   its header comment in query_bulk.c), so whatever it did
                   apply is reverted via the same diff-and-apply path
                   crash recovery uses, the window's kf commit never runs,
                   its segment slots go back to the free pool, and its
                   batch marker is discarded rather than retained for
                   replay. */
                int window_rejected = 0;
                if (napply_active > 0) {
                    if (opts->apply_window(recs, apply_active, napply_active, opts->bulk_hook_ctx) != 0) {
                        window_rejected = 1;
                    }
                } else if (nactive > 0 && opts->abort_window) {
                    /* prepare_window staged resources (open bitmap writer
                       handles, tracked buffers, queued ops) for this window
                       but every record was rejected before or during marker
                       write — apply_window will never run, so release them
                       here instead of leaking them. */
                    opts->abort_window(opts->bulk_hook_ctx);
                }

                durability_test_pause(db->data_dir, "bulk-window-applied");

                if (window_rejected) {
                    /* Revert whatever apply_window partially applied:
                       diff FROM the attempted new content TO the record's
                       actual surviving content (old_value for an update,
                       NULL for a fresh insert) — the same
                       g_recovery_index_diff_fn used by crash-recovery
                       marker replay (kf_marker_replay_entry_locked,
                       above in this file), run in the opposite
                       direction. Naturally a no-op for any field
                       apply_window never actually reached (diffing
                       "no change" produces no index op), and naturally
                       correct for updates (fields that didn't change
                       diff to themselves — no op — while anything wrongly
                       added for the new value gets removed). kf was never
                       repointed for these records (the commit loop below
                       is skipped entirely in this branch), so there is no
                       committed kf state to separately roll back — this
                       is the "revert if kf is already committed" case the
                       plan calls for, made structurally unreachable by
                       ordering rather than needing its own undo path. */
                    char eff_root[PATH_MAX], object_name[256];
                    split_data_dir(db->data_dir, eff_root, sizeof(eff_root),
                                   object_name, sizeof(object_name));
                    for (size_t a = 0; a < napply_active; a++) {
                        size_t j = apply_active[a];
                        SlotcaskBulkRec *r = &recs[j];
                        if (g_recovery_index_diff_fn) {
                            char err_buf[256] = {0};
                            uint32_t revert_slot = st[j].old_found
                                ? (uint32_t)st[j].old_kf_slot
                                : (uint32_t)st[j].plan_slot;
                            /* Best-effort: a failure here means an index
                               revert didn't fully land, but the record
                               itself is still rejected below (status=-1,
                               kf never committed, marker discarded) — it
                               will never become visible, so a stray
                               orphan index entry pointing at a hash with
                               no kf entry is the same class of artifact
                               the corrupt-marker recovery path already
                               tolerates, not a new failure mode. */
                            g_recovery_index_diff_fn(eff_root, object_name, kf_shard_id,
                                                     revert_slot, st[j].hash,
                                                     r->value, r->vlen,
                                                     r->old_value, r->old_vlen,
                                                     err_buf, sizeof(err_buf));
                        }
                        seg_write_flag(db, st[j].target_stream, st[j].target_fid,
                                       st[j].target_off, 2);
                        pool_push_free(&db->streams[st[j].target_stream],
                                       st[j].target_fid, st[j].target_off, db->slot_size);
                        r->status = -1;
                    }
                    if (fd >= 0) {
                        if (unlink(bpath) != 0 || fsync_dir(dpath) != 0)
                            kf_marker_fail_closed(db->data_dir, kf_shard_id,
                                                  "could not durably discard rejected bulk marker");
                    }
                } else {
                    size_t vslots[BULK_COMMIT_MAX_RECORDS];
                    size_t nvslots = 0;
                    for (size_t a = 0; a < napply_active; a++) {
                        size_t j = apply_active[a];
                        SlotcaskBulkRec *r = &recs[j];
                        size_t pub_slot;
                        if (st[j].old_found) {
                            pub_slot = st[j].old_kf_slot;
                            kf_repoint_at_slot(&kh, st[j].old_kf_slot,
                                                st[j].target_stream,
                                                st[j].target_fid, st[j].target_off);
                        } else {
                            KfInsertPlan plan;
                            memcpy(plan.hash, st[j].hash, sizeof(plan.hash));
                            plan.target_slot = st[j].plan_slot;
                            plan.reused_tomb = st[j].plan_reused_tomb;
                            plan.key = r->key;
                            plan.klen = r->klen;
                            size_t used_delta = 0;
                            pub_slot = 0;
                            kf_commit_planned_slot(&kh, &plan, st[j].target_stream,
                                                    st[j].target_fid, st[j].target_off,
                                                    &used_delta, &pub_slot);
                        }
                        r->kf_shard = kf_shard_id;
                        r->kf_slot  = (uint32_t)pub_slot;
                        vslots[nvslots++] = pub_slot;
                    }

                    if (nvslots > 0 &&
                        kfcache_sync_slots_locked(&kh, vslots, nvslots, 0) != 0) {
                        keep_marker = 1;
                        if (opts->out_durability_degraded)
                            *opts->out_durability_degraded = 1;
                    }
                }
```

Notes on this replacement:

- `keep_marker`/`out_durability_degraded` are now reserved exclusively for
  the `kfcache_sync_slots_locked` failure case (kf committed, index
  applied, only the *marker clear* is uncertain — a genuinely different,
  already-handled degraded state, unchanged from today). `apply_window`
  failure no longer sets either — it is a hard reject, not a degraded
  state.
- `fd >= 0` is structurally guaranteed true whenever `window_rejected` can
  become true: `window_rejected` is only set inside the
  `if (napply_active > 0)` branch, and `apply_active`/`napply_active` are
  built from `survive[]` entries with `recs[j].status == 0`, which by
  construction excludes every record already rejected by the `fd < 0`
  branch a few lines above (that branch unconditionally sets
  `recs[j].status = -1` for every surviving record when the marker write
  itself failed). The `if (fd >= 0)` guard is kept anyway, matching this
  file's existing defensive style at the other two marker-discard call
  sites.
- `r->value`/`r->vlen` is `SlotcaskBulkRec`'s NEW content (what the caller
  attempted to write); `r->old_value`/`r->old_vlen` is the OLD content,
  populated during the prepare-window loop earlier in this same function.
  Both are guaranteed populated by the time this code runs (prepare
  already completed for every record reaching `apply_active`).

### Test first (must fail before the fix, pass after)

New file `src/test/cases/test_bulk_index_apply_reject.c`, daemon-based,
modeled on `test_durability_ordering.c`'s `create_indexed_object_*` /
`trigger_bulk_insert` / `request_count` helpers, plus a new helper that
picks keys by target **index** shard (not kf shard — a different hash byte
order per `idx_shard_for_hash`, confirmed via code read of
`src/db/types.h`):

```c
#define _GNU_SOURCE
#include "test_runner.h"
#include "test_assert.h"
#include "types.h"
#include "fixtures.h"
#include "test_client.h"

#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

/* Two indexed fields ("score" btree, "tag" btree) so we can corrupt only
   one field's index shard and prove the OTHER field's already-applied
   entries get reverted too — apply_window dispatches per-field via
   parallel_for(idx_build_field_worker), so "score" and "tag" fail/succeed
   independently within the same window. */
static int create_two_index_object(TestEnv *env, const char *object) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;

    char *resp = NULL;
    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"default\"}", &resp);
    free(resp); resp = NULL;

    char req[768];
    snprintf(req, sizeof(req),
        "{\"mode\":\"create-object\",\"dir\":\"default\","
        "\"object\":\"%s\",\"splits\":8,\"streams\":1,\"max_key\":16,"
        "\"fields\":[\"score:int\",\"tag:varchar:16\"],"
        "\"indexes\":[\"score\",\"tag\"]}", object);
    int ok = tc_request(tc, req, &resp) == 0 && SAFE_STRSTR(resp, "\"status\":\"created\"");
    free(resp);
    tc_close(tc);
    return ok ? 0 : -1;
}

/* Mirrors pick_same_shard_keys (test_durability_ordering.c) but targets an
   INDEX shard for a single-field, non-composite index via
   idx_shard_for_hash — NOT compute_record_shard, which uses the opposite
   hash-byte order and would pick the wrong keys. */
static int pick_idx_shard_keys(int splits, int target_idx_shard, int *next_candidate,
                                char out_keys[][32], int need) {
    int found = 0;
    int candidate = *next_candidate;
    int guard = candidate + need * 64 + 4096;
    while (found < need && candidate < guard) {
        char key[32];
        snprintf(key, sizeof(key), "ridx%08d", candidate);
        uint8_t hash16[16];
        compute_hash_raw(key, strlen(key), hash16);
        if (idx_shard_for_hash(hash16, splits) == target_idx_shard) {
            snprintf(out_keys[found], 32, "%s", key);
            found++;
        }
        candidate++;
    }
    *next_candidate = candidate;
    return found == need ? 0 : -1;
}

static int request_count_field_eq(TestEnv *env, const char *object,
                                  const char *field, const char *value) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512];
    snprintf(req, sizeof(req),
        "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\","
        "\"criteria\":[{\"field\":\"%s\",\"op\":\"eq\",\"value\":\"%s\"}]}",
        object, field, value);
    char *resp = NULL;
    int result = -1;
    if (tc_request(tc, req, &resp) == 0) result = tu_parse_count(resp);
    free(resp);
    tc_close(tc);
    return result;
}

static int request_count(TestEnv *env, const char *object) {
    TestClientCfg cfg = { .port = env->port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) return -1;
    char req[512];
    snprintf(req, sizeof(req), "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"%s\"}", object);
    char *resp = NULL;
    int result = -1;
    if (tc_request(tc, req, &resp) == 0) result = tu_parse_count(resp);
    free(resp);
    tc_close(tc);
    return result;
}

/* Corrupts one field's idx shard 0 file by truncating it below the
   minimum valid size — same mechanism Task 1's unit test uses, here
   exercised end-to-end through the real bulk-insert wire path. */
static int corrupt_idx_shard(const char *db_root, const char *object, const char *field) {
    char path[PATH_MAX];
    snprintf(path, sizeof(path), "%s/default/%s/indexes/%s/000.idx",
             db_root, object, field);
    return truncate(path, 8);
}

/* 1) A bulk-insert window whose "tag" field index write hits a corrupted
   shard must reject every record in that window outright: none of them
   become gettable, and the OTHER field's ("score") index entries that
   apply_window already wrote before failing on "tag" must be reverted —
   not left as orphans with no matching kf entry. */
static int test_bulk_insert_reverts_partial_index_on_reject(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    const char *object = "bulkrejmix";
    ASSERT_EQ_INT(create_two_index_object(&env, object), 0,
                  "create two-index fixture");

    /* splits=8 -> index_splits_for(8)=2 shards (000.idx, 001.idx) for
       each of "score" and "tag". Seed shard 0 for BOTH fields with one
       valid entry first so bt_extract_all's rebuild-vs-corrupt path is
       exercised against a genuinely non-empty tree (matches the real
       production scenario, not merely "file never existed"). */
    char seed_key[1][32];
    int seed_next = 0;
    ASSERT_EQ_INT(pick_idx_shard_keys(8, 0, &seed_next, seed_key, 1), 0,
                  "pick one key routing to idx shard 0");
    {
        TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
        TestClient *tc = tc_connect(&cfg);
        ASSERT_NOT_NULL(tc, "connect to seed shard 0");
        if (tc) {
            char req[256], *resp = NULL;
            snprintf(req, sizeof(req),
                "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"%s\","
                "\"key\":\"%s\",\"value\":{\"score\":1,\"tag\":\"seed\"}}",
                object, seed_key[0]);
            tc_request(tc, req, &resp);
            ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed insert for idx shard 0 succeeds");
            free(resp);
            tc_close(tc);
        }
    }

    ASSERT_EQ_INT(corrupt_idx_shard(env.db_root, object, "tag"), 0,
                  "corrupt tag's idx shard 0 file");

    char keys[3][32];
    int next_candidate = seed_next;
    ASSERT_EQ_INT(pick_idx_shard_keys(8, 0, &next_candidate, keys, 3), 0,
                  "pick 3 more keys routing to idx shard 0 for both fields");

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect for rejected bulk-insert");
    if (tc) {
        char req[1024], *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
            "{\"key\":\"%s\",\"value\":{\"score\":42,\"tag\":\"reject-me-0\"}},"
            "{\"key\":\"%s\",\"value\":{\"score\":42,\"tag\":\"reject-me-1\"}},"
            "{\"key\":\"%s\",\"value\":{\"score\":42,\"tag\":\"reject-me-2\"}}]}",
            object, keys[0], keys[1], keys[2]);
        int rc = tc_request(tc, req, &resp);
        ASSERT_EQ_INT(rc, 0, "bulk-insert request completes (server responds, doesn't hang/crash)");
        ASSERT_TRUE(resp && !SAFE_STRSTR(resp, "\"inserted\":3"),
                    "bulk-insert does NOT silently report full success for a window "
                    "whose index apply failed");
        free(resp); resp = NULL;

        for (int i = 0; i < 3; i++) {
            char greq[128];
            snprintf(greq, sizeof(greq),
                "{\"mode\":\"exists\",\"dir\":\"default\",\"object\":\"%s\",\"key\":\"%s\"}",
                object, keys[i]);
            tc_request(tc, greq, &resp);
            ASSERT_CONTAINS(resp, "false",
                            "rejected record never became visible (kf not committed)");
            free(resp); resp = NULL;
        }
        tc_close(tc);
    }

    /* The "score" field's idx shard 0 was NOT corrupted, so apply_window's
       per-field parallel dispatch may well have written all three
       "score":42 entries successfully before "tag" failed. Prove those
       got reverted too — zero matches, not orphaned index entries with no
       backing kf record. */
    ASSERT_EQ_INT(request_count_field_eq(&env, object, "score", "42"), 0,
                  "score index has no orphaned entries for the rejected records "
                  "(apply_window's partial success on the OTHER field was reverted)");

    ASSERT_EQ_INT(request_count(&env, object), 1,
                  "object count is exactly the one earlier seed record — "
                  "the rejected window contributed nothing");

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

/* 2) A window whose records route to an UNCORRUPTED idx shard in the same
   object must still succeed normally — the reject path must not be
   overbroad and reject unrelated shards. */
static int test_bulk_insert_unaffected_shard_still_succeeds(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) return 1;

    const char *object = "bulkrejother";
    ASSERT_EQ_INT(create_two_index_object(&env, object), 0,
                  "create two-index fixture");
    ASSERT_EQ_INT(corrupt_idx_shard(env.db_root, object, "tag"), 0,
                  "corrupt tag's idx shard 0 (object created but shard 0 not yet written, "
                  "simulating corruption discovered on first write)");

    char keys[2][32];
    int next_candidate = 0;
    ASSERT_EQ_INT(pick_idx_shard_keys(8, 1, &next_candidate, keys, 2), 0,
                  "pick 2 keys routing to idx shard 1 (untouched by the corruption)");

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    ASSERT_NOT_NULL(tc, "connect for unaffected-shard bulk-insert");
    if (tc) {
        char req[512], *resp = NULL;
        snprintf(req, sizeof(req),
            "{\"mode\":\"bulk-insert\",\"dir\":\"default\",\"object\":\"%s\",\"records\":["
            "{\"key\":\"%s\",\"value\":{\"score\":7,\"tag\":\"ok-0\"}},"
            "{\"key\":\"%s\",\"value\":{\"score\":8,\"tag\":\"ok-1\"}}]}",
            object, keys[0], keys[1]);
        tc_request(tc, req, &resp);
        ASSERT_CONTAINS(resp, "\"inserted\":2",
                        "bulk-insert on an unrelated, uncorrupted idx shard succeeds normally");
        free(resp);
        tc_close(tc);
    }

    ASSERT_EQ_INT(request_count(&env, object), 2, "both unaffected records committed");

    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bulk-insert-reverts-partial-index-on-reject",
              test_bulk_insert_reverts_partial_index_on_reject)
TEST_REGISTER("test-bulk-insert-unaffected-shard-still-succeeds",
              test_bulk_insert_unaffected_shard_still_succeeds)
```

Register in `build.sh` — insert right after this exact anchor line:

Quoted anchor:
```
    src/test/cases/test_durability_ordering.c \
```
New text (insert immediately after):
```
    src/test/cases/test_durability_ordering.c \
    src/test/cases/test_bulk_index_apply_reject.c \
```

Run both new cases before the Task 1 + Task 3 fixes and confirm they fail
for the expected reason:
- `test_bulk_insert_reverts_partial_index_on_reject` fails today because
  (a) Defect A means the corrupted `tag` shard's `btree_bulk_merge` may
  itself silently "succeed" by discarding whatever was there (masking the
  failure this test wants to trigger) — this is exactly why Task 1 must
  land first, so `apply_window` actually observes a failure — and (b) even
  once Defect A is fixed and `apply_window` does fail, Defect B still
  commits kf unconditionally, so `exists` returns `true` for the rejected
  keys and the `score` orphan-count assertion fails.
- `test_bulk_insert_unaffected_shard_still_succeeds` should already pass
  today (sanity check that the fix doesn't overreject) — include its
  before-fix output anyway to confirm it wasn't accidentally broken by
  Task 1/3 changes.

Paste both the before-fix (failing) and after-fix (passing) runs.

## Edge cases / invariants

- **Empty `apply_active`** (`napply_active == 0`): unchanged — falls into
  the `abort_window` branch, no marker, nothing to revert.
- **`opts->apply_window == NULL`**: not reachable for the indexed path —
  `SlotcaskBulkOpts`'s doc comment states `prepare_window`/`apply_window`
  are "required together for a fresh indexed bulk insert window
  (`has_indexed_fields=1` and `pre_commit != NULL`)"; this branch is only
  entered under that condition.
- **`g_recovery_index_diff_fn == NULL`** (kf-layer-only test builds that
  never register `storage_recovery_index_diff`): revert becomes a no-op
  for index state (matches existing `kf_marker_replay_entry_locked`
  behavior at the same guard) — segment reclaim, marker discard, and
  `status = -1` still happen unconditionally, so the record is still
  correctly rejected; only orphan-index cleanup is skipped, consistent
  with those builds not having index logic linked in at all.
  `apply_window` itself has no implementation without index logic linked,
  so this combination cannot occur in a build where `apply_window` is
  even set.
- **`r->old_value` is `NULL` but `st[j].old_found`**: cannot happen — the
  prepare-window loop auto-populates `old_value`/`old_vlen` from
  `st[j].old_buf`/`old_vlen` whenever `old_found` is set, before this code
  runs.
- **Composite indexes** (`field1+field2`): unaffected by this plan's
  logic — `apply_index_diff` iterates `idx_fields[]` as loaded by
  `load_index_fields`, which already includes composite entries by their
  on-disk directory name; the revert diff treats a composite index exactly
  like any other field for change-detection purposes.
- **Mid-revert crash** (process killed while reverting a rejected window):
  not a new risk surface — the batch marker is still on disk and durable
  at that point (it was written and fsynced before `apply_window` ran),
  so a crash here is indistinguishable from the existing
  apply-boundary crash-recovery case (`test_durability_bulk_window_applied_recovers`)
  and gets replayed forward by the existing recovery sweep on restart —
  i.e. the record ends up committed via replay rather than reverted. This
  is a pre-existing, already-tested behavior of the marker/recovery
  system, not something this plan changes or needs to re-prove.
- **`window_rejected` and `keep_marker` are mutually exclusive** by
  construction in the new code (the marker-discard branch runs instead of,
  never together with, the kf-commit/`kfcache_sync_slots_locked` branch
  that can set `keep_marker`).

## Definition of done

- [ ] Task 1's new unit test fails before the fix (pasted output) and
      passes after (pasted output).
- [ ] Task 3's two new daemon tests fail before Task 1+3 land (pasted
      output, both cases) and pass after (pasted output, both cases).
- [ ] `SKIP_TESTS=1 ./build.sh` clean, then
      `./build/bin/shard-db-test run-all` fully clean (no regressions in
      any pre-existing case, including the two `test_durability_ordering.c`
      bulk-window-boundary tests that exercise `apply_window` on the
      success path — must confirm they still pass unmodified against the
      restructured kf-commit-conditional-on-success code).
- [ ] Per this repo's standing AGENTS.md exception (diff touches locks,
      shared/cached state, and the bulk-commit core in `slotcask.c`) — run
      both sanitizer gates locally before calling this done, not deferred
      to CI:
      - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh` then
        `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1" ./build/bin/shard-db-test run-all --jobs 2`
        (minimum: the new test cases plus every `test_durability_*` and
        `test_slotcask_v2_*` case).
      - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh` then
        `TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp" ./build/bin/shard-db-test run-all --jobs 1`
        (same minimum set).
      - Any new finding gets root-caused and fixed now, or written up as
        its own `docs/plans/<date>-<slug>.md` and added to `.tsan.supp`
        with a named-function suppression and full rationale — never a
        blanket suppression.
- [ ] No new compiler/linter warnings.
- [ ] No leftover debug prints / commented-out code.
- [ ] Diff contains only Task 1 + Task 2 + Task 3's changes — no drive-by
      refactors.
- [ ] No new dependencies.
- [ ] Documentation sync check: `AGENTS.md`'s "Storage model" /
      "Crash safety" prose doesn't currently describe `apply_window`
      failure handling at that level of detail, so no doc update is
      required there; the two doc comments updated in Task 3
      (`slotcask.h`'s `apply_window` contract, and the inline comment in
      `slotcask.c`) ARE the documentation for this behavior and are
      included in the task itself.
- [ ] Left uncommitted per this repo's execution mode — human + reviewing
      agent review the raw `git diff` before anything is committed.

## Execution rules

- Branch off `main`: `fix/bulk-index-apply-hard-reject` (only once this
  plan is explicitly approved for execution — it is currently a draft).
- Do tasks in order: Task 1 before Task 3 (Task 3's regression test
  depends on Defect A being fixed first, or `apply_window` never actually
  observes the corrupted-shard failure it needs to trigger the reject
  path). Task 2 can land in either order relative to the others — it's
  independent.
- Exact build/test commands: `SKIP_TESTS=1 ./build.sh` to build,
  `./build/bin/shard-db-test run-all` / `run <name>` to test, per this
  repo's `AGENTS.md`.
- If a quoted anchor isn't found exactly as quoted (e.g. because
  `2026-07-30-btree-bulk-merge-tie-duplicate.md` landed changes nearby in
  `btree.c` first and shifted surrounding text), write `PLAN_NOTES.md`
  describing the exact mismatch and halt the entire execution run
  immediately — do not guess, reinterpret, or continue to any further
  task, even Task 2 which looks unrelated. Resuming requires the human (or
  the planning model, re-engaged) to read `PLAN_NOTES.md` and hand back
  either a patched or a fresh plan.
- If you hit a decision this plan doesn't cover, stop and ask — do not
  improvise.
