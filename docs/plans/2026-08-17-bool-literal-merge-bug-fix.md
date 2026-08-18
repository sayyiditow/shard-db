# Implementation plan: fix partial-update literal drop + index-key OOB read

Follow-up to `docs/plans/2026-08-17-bool-literal-merge-bug.md` (bug brief).
That document is evidence/symptom-only and is not modified by this plan.

Status: **plan only — no code has been written**. Execution requires
explicit human go-ahead per CORE-PROCESS.md step 2.

## Root cause 1: partial-update field writes past `old->vlen` never reach `*out_vlen`

**File:** `src/db/storage.c`, function `v2_update_new_from_old` (used as the
`new_from_old` hook for `cmd_update_v2`'s single-record partial update —
confirmed via prior investigation to be the *only* `new_from_old` hook
registration in the codebase; bulk-update uses a structurally different
`value_compute` hook and is immune by construction).

Mechanism, traced end to end:

1. `upsert_slow_path` (`src/db/slotcask.c`, ~line 5190) allocates
   `callback_value` via **uninitialized** `malloc(out_capacity)` (not
   `calloc`), where `out_capacity = db->slot_size - 24 - klen` (the
   object's full max record size). It sets `write_vlen = 0`, then calls the
   hook: `opts->new_from_old(old_ptr, callback_value, out_capacity,
   &write_vlen, ctx)`.
2. After the hook returns, it re-derives the persisted length via
   `write_vlen = trim_fn(callback_value, write_vlen, db->trim_ctx)` —
   **the trim scan only looks at `callback_value[0 .. write_vlen)`**, i.e.
   exactly the range the hook told it was valid via `*out_vlen`.
3. `v2_update_new_from_old` today:
   ```c
   memcpy(out_value, old->value, old->vlen);
   *out_vlen = old->vlen;
   ```
   then, in the per-field loop, applies each JSON-supplied field via
   `encode_field(&c->idx_ts->fields[i], field_vals[i], out_value +
   c->idx_ts->fields[i].offset)` — **unconditionally, even when
   `fields[i].offset >= old->vlen`** (i.e. the field falls in the region
   that was trimmed off the OLD record because it held the schema's zero/
   default value). `*out_vlen` is never touched again after step 3's
   initial assignment.
4. Consequence: when a trim-compacted OLD record is partially updated and
   the touched field's offset is `>= old->vlen`, `encode_field` writes the
   new bytes into `out_value` at the correct offset — but `*out_vlen`
   still equals `old->vlen`, so `trim_fn` in step 2 never sees those bytes,
   and the field never reaches the "minimum length to persist" computation.
   The record is written **without** the update, `slotcask_upsert_with_hooks`
   reports success (the write itself succeeds), and the change is silently
   lost. This reproduces the brief's exact sequence: insert `flag:true`
   (135B, byte present) → update `{"flag":false}` (134B — `false` and
   default-`false` are indistinguishable, so trimming the flag byte off is
   *correct* for that step and masks the bug) → update `{"flag":true}` solo
   (flag's offset is now `>= old->vlen=134`; `encode_field` writes byte
   `0x01` into `out_value[134]`, but `*out_vlen` stays `134`, so the write
   never survives trim) → `get` still returns `flag:false`.

This is a **field-write-visibility** bug, not a JSON-parsing bug — the
brief's "literal drop" framing is about symptom (bare `true`/`false`
tokens are the only values compact enough that this is easy to trigger:
a string field long enough to always be non-default trims less
aggressively). `encode_field_len`'s `FT_BOOL` case
(`src/db/config.c:1698-1701`) was independently re-verified in the parent
investigation and is correct; it is not part of the fix.

### Fix 1

Zero-fill the untouched tail of the output buffer between `old->vlen` and
the schema's full field region *before* applying field writes, and extend
`*out_vlen` to cover that full region. This mirrors the zero-pad pattern
`reindex_seg_cb` already uses for the same trim-compaction hazard
(`src/db/index.c`, `SegScanWorker.padded_value`, comment: "fields beyond
vlen read back as zero"). After this change, `out_value[0 ..
idx_ts->total_size)` is always fully defined (real OLD bytes, or a written
field, or zero-default) by the time the field-write loops finish, so
`trim_fn` — called by the caller immediately after this hook returns — can
correctly re-derive the true minimal persisted length from real data
instead of being handed a truncated view.

**Anchor:** `memcpy(out_value, old->value, old->vlen);\n    *out_vlen = old->vlen;`
in `v2_update_new_from_old` (`src/db/storage.c`).

Replace with:

```c
    if ((size_t)c->idx_ts->total_size > out_capacity) return -1;
    memcpy(out_value, old->value, old->vlen);
    /* old->value may be a trim-compacted record shorter than
       idx_ts->total_size (trailing default-valued fields dropped on the
       prior write). Zero-fill the untouched tail before applying field
       writes below: (a) a write to a field at/after old->vlen then lands
       in defined (zero) memory instead of uninitialized malloc'd bytes
       from upsert_slow_path's callback_value allocation, and (b) *out_vlen
       spans the schema's full field region so trim_fn (invoked by the
       caller immediately after this hook returns) re-derives the true
       minimal persisted length from real bytes. Previously *out_vlen
       stayed at old->vlen, so any field write at/after old->vlen was
       applied to the buffer but silently excluded from what trim_fn — and
       therefore the persisted record — ever saw. */
    size_t full_len = (size_t)c->idx_ts->total_size;
    if (full_len > old->vlen)
        memset(out_value + old->vlen, 0, full_len - old->vlen);
    *out_vlen = full_len;
```

Also add the guard `if ((size_t)c->idx_ts->total_size > out_capacity)
return -1;` right after the existing entry guard (`if (!old || !out_value
|| !out_vlen || old->vlen > out_capacity) return -1;`) — defensive, mirrors
that guard's style; `out_capacity` is always `>= idx_ts->total_size` in
practice (`db->slot_size - 24 - klen >= max_value` whenever `klen <=
max_key`, which is enforced elsewhere), but this keeps the function's own
invariants self-checking rather than assuming a caller-side guarantee.

No other line in `v2_update_new_from_old` needs to change — the two
field-write loops (explicit JSON fields, then `auto_update` fields) are
unchanged; they now simply write into pre-zeroed memory instead of
uninitialized memory when the target offset is past the old record's
length.

## Root cause 2: index-key builders read past the OLD record's actual length

**Files:** `src/db/config.c` (`typed_field_to_index_key`), `src/db/index.c`
(`build_index_key_from_record`, `build_index_key_from_record_into`).

None of these three functions take a length parameter for the record
buffer they read from. `typed_field_to_index_key` does:

```c
void typed_field_to_index_key(const TypedSchema *ts, const uint8_t *data,
                              int field_idx, uint8_t *out, size_t *out_len) {
    const TypedField *f = &ts->fields[field_idx];
    const uint8_t *src = data + f->offset;
    switch (f->type) { ... reads up to f->size bytes from src ... }
}
```

unconditionally, regardless of how many bytes `data` actually has. The
other two functions call it in a loop (composite specs) or once (single
field) with no bound either.

**Why this is a real out-of-bounds read, not just a style issue:**
`slotcask_get` (`src/db/slotcask.c:4173`) allocates the `SlotcaskOldRecord`
snapshot passed into every commit hook as `malloc(v_stored ? v_stored : 1)`
— **exactly `vlen` bytes, no padding to the schema's full record size**.
So for a trim-compacted OLD record (the same kind root cause 1 involves),
`old->value` is a heap buffer sized to `old->vlen`, and any index-key build
on a field at or beyond `old->vlen` reads past the end of that malloc'd
allocation. This is a heap-buffer-overflow READ, deterministically caught
by ASan (exact-sized allocation, no slack) — it is not merely "reads
garbage that might be wrong," it is undefined behavior on every trim-
compacted record with a trimmed field that's part of an index diff.

Consequences already observed in the bug brief: a phantom "changed" or
"unchanged" verdict in `apply_index_diff`'s old-vs-new key comparison,
i.e. **secondary index state can diverge from record state**, in addition
to the OOB read itself.

**Existing correct precedent for the same hazard, already in the
codebase** — `typed_get_field_str` (`src/db/config.c:3139`):

```c
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data,
                           int data_len, int field_idx) {
    ...
    static const uint8_t zero_field[65537];
    const uint8_t *src = (data_len >= 0 &&
                            (size_t)f->offset + (size_t)f->size > (size_t)data_len)
        ? zero_field
        : data;
    ...
    switch (f->type) {
    case FT_VARCHAR: {
        const uint8_t *p = src + f->offset;   /* <-- offset re-applied here */
        ...
```

**Important deviation from this precedent, and why:** `typed_get_field_str`
swaps in `zero_field` *unshifted*, then re-applies `+ f->offset` inside
each switch case (see the `FT_VARCHAR` case above). That means it actually
reads from `zero_field + f->offset`, not `zero_field[0..]`. `zero_field` is
sized `65537` (the largest single field: 65535-byte varchar content + 2
byte length prefix) — but `f->offset` is the field's offset **within the
whole record**, which is unbounded by 65537 for a schema with many fields
(`MAX_FIELDS = 256`; a schema of large varchar fields can have
`total_size` far past 65537). So `typed_get_field_str`'s existing
sentinel-swap can itself read past the end of its own static buffer for a
late field in a large schema. This is a pre-existing latent hazard in
`typed_get_field_str`, structurally identical in kind to root cause 2 but
in a different function — **now in scope, fixed below as root cause 3 /
fix 3** (originally flagged as a deferred follow-up during the first pass
of this investigation; folded into this plan and branch per explicit
human direction rather than left as a separate future ticket).

`typed_field_to_index_key` avoids replicating that flaw because it already
computes `src = data + f->offset` **once**, at the top, before the
type switch — every case then reads from `src` directly, never re-adding
`f->offset`. So the fix here computes the *already-offset* pointer once:
either `data + f->offset` (in-bounds) or `zero_field` **unshifted** (out of
bounds) — never `zero_field + f->offset`. A `zero_field[65537]` sized to
the single largest field extent (65535-byte varchar content + 2-byte
prefix; every other type reads <= 16 bytes from `src`) is then always
large enough, independent of the field's absolute offset in the record.

### Fix 2

#### New signatures

```c
/* src/db/config.c + types.h:845 */
void typed_field_to_index_key(const TypedSchema *ts, const uint8_t *data,
                              size_t data_len, int field_idx,
                              uint8_t *out, size_t *out_len);

/* src/db/index.c + types.h:854 AND types.h:1453 (duplicate declaration —
   see "types.h duplicate declaration" note below; both must be updated
   identically, neither removed) */
int build_index_key_from_record(const TypedSchema *ts, const uint8_t *record,
                                size_t record_len,
                                const char *spec,
                                uint8_t **out_val, size_t *out_len);

/* src/db/index.c + types.h:1456 */
int build_index_key_from_record_into(const TypedSchema *ts, const uint8_t *record,
                                      size_t record_len,
                                      const char *spec,
                                      uint8_t *out, size_t out_cap,
                                      size_t *out_len);
```

`data_len`/`record_len` inserted immediately after the record pointer in
every case, matching `typed_get_field_str`'s existing `(ts, data, data_len,
field_idx)` parameter order convention.

#### `typed_field_to_index_key` body change

**Anchor:**
```c
    const TypedField *f = &ts->fields[field_idx];
    const uint8_t *src = data + f->offset;
    switch (f->type) {
```

Replace with:

```c
    const TypedField *f = &ts->fields[field_idx];
    /* data may be a trim-compacted record shorter than the field's offset
       (see slotcask_get / SlotcaskOldRecord — the OLD snapshot passed into
       commit hooks is malloc'd to exactly vlen bytes, no padding). Treat
       an out-of-range field as its zero/default encoding rather than
       reading past the buffer — mirrors reindex_seg_cb's padded_value
       zero-pad for the same hazard. zero_field is sized to the largest
       single field this function ever reads (65535-byte varchar content +
       2-byte length prefix); every other type reads <= 16 bytes from src.
       Unlike typed_get_field_str's zero_field, this is used UNSHIFTED
       (src itself, not src + f->offset) because f->offset is already
       folded in below before the sentinel-vs-data choice is made, so the
       buffer only ever needs to cover a single field's width, not the
       field's absolute offset in the record. */
    static const uint8_t zero_field[65537];
    const uint8_t *src = ((size_t)f->offset + (size_t)f->size > data_len)
        ? zero_field
        : data + f->offset;
    switch (f->type) {
```

No other line in the function changes — every `case` already reads from
`src` (not `data`), so the sentinel swap is transparent to every type
branch, including the `FT_VARCHAR` case's own internal `src[0]`/`src[1]`/
`src+2` reads.

#### `build_index_key_from_record_into` body change

**Anchor (composite branch):**
```c
            size_t blen = 0;
            typed_field_to_index_key(ts, record, fi, out + cp, &blen);
            if (blen == 0) return 0;
```
→
```c
            size_t blen = 0;
            typed_field_to_index_key(ts, record, record_len, fi, out + cp, &blen);
            if (blen == 0) return 0;
```

**Anchor (single-field branch):**
```c
    size_t blen = 0;
    typed_field_to_index_key(ts, record, fi, out, &blen);
    if (blen == 0) return 0;
```
→
```c
    size_t blen = 0;
    typed_field_to_index_key(ts, record, record_len, fi, out, &blen);
    if (blen == 0) return 0;
```

#### `build_index_key_from_record` body change

Same two-site edit, threading `record_len` into both
`typed_field_to_index_key` calls (composite-loop and single-field), no
other logic change.

#### `types.h` duplicate declaration

`build_index_key_from_record` is declared twice, byte-identically, at
`types.h:854` and `types.h:1453`; `build_index_key_from_record_into` is
declared once, at `types.h:1456`. Both `build_index_key_from_record`
declarations must be edited identically (add `size_t record_len`) — this
plan does **not** remove the duplicate (that would be an unrelated
drive-by cleanup outside the approved fix scope); it is flagged here so
the executor doesn't "fix" only one and leave a stale prototype mismatch.

### Edge cases (explicit)

- **`FT_VARCHAR` length prefix straddling the boundary.** The bounds
  check is on the *whole field's* `[offset, offset+size)` range, not on
  the 2-byte length prefix specifically. A record trimmed mid-field (e.g.
  `data_len` lands between the prefix and the content, or anywhere inside
  the field) is treated as **fully** out of range and gets the zero/empty
  encoding — never a partial/garbage read of a half-present length prefix.
  This matches the trim function's own granularity: `typed_encode_trim_len`
  (`src/db/config.c:3111`) only ever trims at whole-field boundaries
  (`end = f->offset + f->size`), so a record's `vlen` can never actually
  land mid-field on a correctly-trimmed record — but the bounds check must
  still be defensive against it (e.g. a future encoder bug, or corruption)
  rather than assume it can't happen.
- **Composite specs (`field1+field2`), partial out-of-range.** Each
  sub-field is bounds-checked independently inside
  `typed_field_to_index_key` itself (called once per sub-field in the
  composite loop). A sub-field beyond `record_len` degrades to its
  zero-encoding for that sub-field only; the concatenation proceeds
  normally with that sub-field's bytes replaced by zeros — it does not
  abort the whole composite key (existing `blen == 0` → `return 0` only
  fires for genuinely-missing fields, i.e. `typed_field_index` returning
  `-1`, which is unrelated to this bounds check; a zero-encoded FT_BOOL/
  FT_ENUM/FT_INT/etc. field still returns a non-zero `blen`, e.g. 1 byte
  for FT_BOOL — see next bullet).
- **`FT_VARCHAR` out-of-range still yields `blen == 0` deliberately.**
  When `zero_field` is selected for a VARCHAR field, `src[0]`/`src[1]` (the
  length prefix) read as `0`, so `len = 0` and `*out_len = 0` — which is
  exactly the existing "missing/empty field" signal both wrapper functions
  already check for (`if (blen == 0) return 0` / `ok = 0`). This is
  correct: an absent/trimmed varchar field's logical value is an empty
  string, which is indistinguishable from "not present" for index-key
  purposes today (pre-existing behavior for a *present-but-empty* varchar
  field too — not a new edge case introduced by this fix).
- **Non-VARCHAR out-of-range fields must NOT collapse to "missing."** For
  every other type (`FT_BOOL`, `FT_INT`, `FT_ENUM`, `FT_DOUBLE`, ...), the
  zero-byte encoding is a **real, indexable value** (bool `false`, int
  `0`, enum entry 0, `+0.0`, ...) — not "absence." This is why the fix
  swaps in a zero-filled buffer and lets every existing `case` run
  unmodified (producing `*out_len = f->size` as normal, just with `src`
  containing zeros), rather than special-casing "out of range → *out_len =
  0" at the top of the function. The latter would silently drop a real
  default-valued field out of the index for every trim-compacted record —
  strictly worse than today's behavior, not a fix.
- **`record == NULL` / `record_len == 0`.** Both wrapper functions already
  null-check `record` at entry (`if (!ts || !record || ...) return 0`) —
  unchanged. Every existing call site that conditionally calls these
  functions only when a value pointer is non-NULL (e.g. `a->old_value ?
  build_index_key_from_record(...) : 0` in `apply_index_diff`) keeps that
  same NULL-guard discipline unchanged; this fix only adds the length
  argument alongside the existing pointer.
- **Recovery path (`storage_recovery_index_diff`) already carries the
  needed lengths.** `old_vlen`/`new_vlen` are existing parameters,
  currently discarded via `(void)old_vlen; (void)new_vlen;`
  (`src/db/storage.c:1422`) purely because `IndexDiffApplyArgs` had
  nowhere to put them. This fix adds `old_vlen`/`new_vlen` fields to
  `IndexDiffApplyArgs` and stops discarding the parameters — no new
  plumbing, no format change, this data was already being computed and
  handed to this function on every call.

## `IndexDiffApplyArgs` change

**Anchor** (`src/db/storage.c`):
```c
typedef struct {
    const char *db_root, *object;
    int nidx;
    char (*idx_fields)[256];
    enum IndexType *idx_types;
    int splits;
    const uint8_t *hash;
    int kf_shard;
    uint32_t kf_slot;
    TypedSchema *idx_ts;
    const uint8_t *old_value;
    const uint8_t *new_value;
    char *err_buf;
    size_t err_buf_len;
} IndexDiffApplyArgs;
```
→ add two fields after `new_value`:
```c
typedef struct {
    const char *db_root, *object;
    int nidx;
    char (*idx_fields)[256];
    enum IndexType *idx_types;
    int splits;
    const uint8_t *hash;
    int kf_shard;
    uint32_t kf_slot;
    TypedSchema *idx_ts;
    const uint8_t *old_value;
    size_t old_vlen;
    const uint8_t *new_value;
    size_t new_vlen;
    char *err_buf;
    size_t err_buf_len;
} IndexDiffApplyArgs;
```

`apply_index_diff`'s four `build_index_key_from_record[_into]` calls (on
`a->old_value` / `a->new_value`) each get `a->old_vlen` / `a->new_vlen`
threaded in as the new `record_len` argument, matching which buffer
(`old_value` vs `new_value`) each call targets.

Every `IndexDiffApplyArgs` literal in `storage.c` gets its two new fields
populated from data already in scope at that call site (no new plumbing):

| Call site | `.old_vlen =` | `.new_vlen =` |
|---|---|---|
| `storage_recovery_index_diff` (builds `IndexDiffApplyArgs args = {...}`, ~line 1435) | `old_vlen` (existing param, currently `(void)`-discarded) | `new_vlen` (existing param, currently `(void)`-discarded) |
| `v2_update_pre_commit` (~line 1457) | `old->vlen` | `new_vlen` (existing param, currently `(void)`-discarded) |
| `v2_update_apply_commit` (~line 1483) | `c->saved_old_vlen` (already stashed by `v2_update_new_from_old`) | `new_vlen` (existing param, currently `(void)`-discarded) |

`v2_update_pre_commit` and `v2_update_apply_commit` can each drop their
`(void)new_vlen;` line once the parameter is actually used.

## Full call-site table

Every call site of the three changed functions, and the length expression
each site already has in scope (verified by reading the surrounding
function — no site requires new plumbing; every length is either an
existing struct field or a currently-discarded parameter).

### `typed_field_to_index_key` direct calls (add `data_len` argument)

| File:Line | Context | `data` expression | `data_len` expression |
|---|---|---|---|
| `index.c:1183` | `build_index_key_from_record_into`, composite loop | `record` (function param) | `record_len` (new function param) |
| `index.c:1200` | `build_index_key_from_record_into`, single-field | `record` | `record_len` |
| `index.c:1222` | `build_index_key_from_record`, composite loop | `record` | `record_len` |
| `index.c:1242` | `build_index_key_from_record`, single-field | `record` | `record_len` |
| `index.c:3550` | `mf_append_field`, composite branch (reindex multi-field worker) | `value` | `(size_t)ts->total_size` — `value` is always either the raw untrimmed segment record (`vlen >= ts->total_size`, so reading up to `total_size` is always in-bounds) or `w->padded_value`, zero-filled and copied to exactly `ts->total_size` bytes by the caller (`reindex_seg_cb`, `src/db/index.c:3687-3699`) before `mf_append_field` is invoked |
| `index.c:3564` | `mf_append_field`, single-field branch | `value` | `(size_t)ts->total_size` (same reasoning as above) |
| `query.c:5931` | `d2_batch_cb` (double-dispatch batch resolve callback) | `value` | `vlen` — already a callback parameter (`static int d2_batch_cb(const uint8_t hash16[16], const void *key, size_t klen, const void *value, size_t vlen, void *ctx_ptr)`), just not threaded through today |
| `query.c:6019` | `cursor_fetch_cb` | `value` | `vlen` — already a callback parameter, same shape as above |
| `query.c:6236` | `fetch_sort_batch_cb` | `value` | `vlen` — already a callback parameter, same shape as above |
| `query.c:7166` | join sort-key extraction fallback loop (`read_record_ref` path) | `rr.val` | `rr.vlen` — `RecordRef.vlen` (`types.h:1161`), already populated by `read_record_ref` |
| `query.c:7749` | second (materially identical) join sort-key extraction fallback loop | `rr.val` | `rr.vlen` (same as above) |
| `query_bulk.c:666` | `v2_bulk_ins_prepare_window`, composite-index branch, NEW value | `new_value` (`= (const uint8_t *)rec->value`, function-local, declared line 641) | `rec->vlen` — `SlotcaskBulkRec.vlen` (`slotcask.h:555`), the NEW record's own length, already populated by the bulk primitive before `prepare_window` fires |
| `query_bulk.c:683` | `v2_bulk_ins_prepare_window`, single-field-index branch, NEW value | `new_value` | `rec->vlen` (same as above) |
| `query_join.c:208` | `extract_local_key`, composite-field branch | `driver_raw` (function param) | new `driver_len` parameter — see `extract_local_key` signature change below |

### `extract_local_key` signature change (needed to supply `query_join.c:208`'s new `data_len` argument)

`extract_local_key` (`src/db/query_join.c:196`, declared
`src/db/query_internal.h:404`) itself takes `driver_raw` with no length.
Both its callers already have the length in scope:

| Caller | `driver_len` expression |
|---|---|
| `query_join.c:599` (`adv_search_cb`) | `hdr->value_len` — `SlotHeader.value_len` (`types.h:220`), `hdr` is the callback's own `const SlotHeader *hdr` parameter |
| `query.c:4454` | `value_len` — local `uint32_t value_len = (uint32_t)fvlens[fi];` already computed a few lines above (`query.c:4436`) |

New signature:
```c
int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                       size_t driver_len,
                       const TypedSchema *driver_ts,
                       char *buf, size_t bufsz);
```
Update both the definition (`query_join.c:196`) and the declaration
(`query_internal.h:404`). Inside `extract_local_key`'s composite branch
(the `typed_field_to_index_key` call at line 208), thread `driver_len`
straight through as the new argument. The non-composite branch (calls
`typed_field_to_buf_raw`, not `typed_field_to_index_key`) is unrelated to
*this* fix and left unchanged **here** — it gets its own guard, reusing
this same new `driver_len` parameter, under root cause 4 / fix 4 below
("`extract_local_key` body change").

### `build_index_key_from_record` / `build_index_key_from_record_into` calls (add `record_len` argument)

All of the following are in `src/db/storage.c` or `src/db/query_bulk.c`.
Every length expression is either an existing `SlotcaskOldRecord.vlen` /
`SlotcaskBulkRec.vlen` / `SlotcaskBulkRec.old_vlen` struct field, or (for
`storage.c`'s `apply_index_diff`) the new `IndexDiffApplyArgs.old_vlen` /
`.new_vlen` fields added above.

| File:Line | Function | Record expression | Length expression |
|---|---|---|---|
| `storage.c:1321` | `apply_index_diff`, arena path, OLD (`_into`) | `a->old_value` | `a->old_vlen` |
| `storage.c:1326` | `apply_index_diff`, arena path, NEW (`_into`) | `a->new_value` | `a->new_vlen` |
| `storage.c:1336` | `apply_index_diff`, arena-path OLD fallback (malloc'd variant, on `rc == -1`) | `a->old_value` | `a->old_vlen` |
| `storage.c:1343` | `apply_index_diff`, arena-path NEW fallback | `a->new_value` | `a->new_vlen` |
| `storage.c:1351` | `apply_index_diff`, no-arena path, OLD | `a->old_value` | `a->old_vlen` |
| `storage.c:1354` | `apply_index_diff`, no-arena path, NEW | `a->new_value` | `a->new_vlen` |
| `storage.c:1721` | `v2_delete_apply_commit`, arena path (`_into`) | `old->value` | `old->vlen` |
| `storage.c:1726` | `v2_delete_apply_commit`, arena-path fallback | `old->value` | `old->vlen` |
| `storage.c:1732` | `v2_delete_apply_commit`, no-arena path | `old->value` | `old->vlen` |
| `query_bulk.c:620` | `v2_bulk_ins_prepare_window`, OLD, arena (`_into`) | `old->value` | `old->vlen` (`old_rec.vlen` populated from `rec->old_vlen` a few lines above, line 604) |
| `query_bulk.c:627` | `v2_bulk_ins_prepare_window`, OLD, arena fallback | `old->value` | `old->vlen` |
| `query_bulk.c:633` | `v2_bulk_ins_prepare_window`, OLD, no-arena | `old->value` | `old->vlen` |
| `query_bulk.c:2593` | `v2_bulk_del_apply_window` | `r->old_value` | `r->old_vlen` (`SlotcaskBulkRec`) |
| `query_bulk.c:3105` | `v2_bulk_upd_pre_commit_bulk`, OLD, arena (`_into`) | `old->value` | `old->vlen` |
| `query_bulk.c:3110` | `v2_bulk_upd_pre_commit_bulk`, OLD, arena fallback | `old->value` | `old->vlen` |
| `query_bulk.c:3116` | `v2_bulk_upd_pre_commit_bulk`, OLD, no-arena | `old->value` | `old->vlen` |
| `query_bulk.c:3124` | `v2_bulk_upd_pre_commit_bulk`, NEW, arena (`_into`) | `new_value` | `rec->vlen` (`SlotcaskBulkRec`; `new_value = (const uint8_t *)rec->value` a few lines above, line 3087) |
| `query_bulk.c:3129` | `v2_bulk_upd_pre_commit_bulk`, NEW, arena fallback | `new_value` | `rec->vlen` |
| `query_bulk.c:3135` | `v2_bulk_upd_pre_commit_bulk`, NEW, no-arena | `new_value` | `rec->vlen` |
| `query_bulk.c:3208` | `v2_bulk_upd_apply_window`, OLD, arena (`_into`) | `r->old_value` | `r->old_vlen` |
| `query_bulk.c:3213` | `v2_bulk_upd_apply_window`, OLD, arena fallback | `r->old_value` | `r->old_vlen` |
| `query_bulk.c:3219` | `v2_bulk_upd_apply_window`, OLD, no-arena | `r->old_value` | `r->old_vlen` |
| `query_bulk.c:3227` | `v2_bulk_upd_apply_window`, NEW, arena (`_into`) | `r->value` | `r->vlen` |
| `query_bulk.c:3232` | `v2_bulk_upd_apply_window`, NEW, arena fallback | `r->value` | `r->vlen` |
| `query_bulk.c:3238` | `v2_bulk_upd_apply_window`, NEW, no-arena | `r->value` | `r->vlen` |
| `query_bulk.c:3597` | `v2_bulk_upd_delim_pre_commit_bulk`, OLD | `old->value` | `old->vlen` |
| `query_bulk.c:3600` | `v2_bulk_upd_delim_pre_commit_bulk`, NEW | `new_value` | `rec->vlen` |
| `query_bulk.c:3661` | `v2_bulk_upd_delim_apply_window`, OLD | `r->old_value` | `r->old_vlen` |
| `query_bulk.c:3664` | `v2_bulk_upd_delim_apply_window`, NEW | `r->value` | `r->vlen` |
| `query_bulk.c:4165` | `v2_bulk_upd_json_pre_commit_bulk`, OLD | `old->value` | `old->vlen` |
| `query_bulk.c:4168` | `v2_bulk_upd_json_pre_commit_bulk`, NEW | `new_value` | `rec->vlen` |
| `query_bulk.c:4226` | `v2_bulk_upd_json_apply_window`, OLD | `r->old_value` | `r->old_vlen` |
| `query_bulk.c:4229` | `v2_bulk_upd_json_apply_window`, NEW | `r->value` | `r->vlen` |
| `query_bulk.c:5157` | `v2_bulk_del_crit_apply_window` | `r->old_value` | `r->old_vlen` — `SlotcaskBulkRec.old_vlen` (`slotcask.h:570`), the same struct field this file's sibling `v2_bulk_del_crit_prepare_window` already reads at `query_bulk.c:5128` for its CAS check |

Every `build_index_key_from_record_into` call additionally needs its
`out_cap`/return-value handling left untouched — only the new
`record_len` argument is inserted (immediately after the record pointer,
before `spec`), per the new signature above.

**Total: 14 `typed_field_to_index_key` direct call sites + 2
`extract_local_key` callers (indirect) + 31
`build_index_key_from_record`/`_into` call sites = 47 call sites**, plus
the 3 changed declarations (`typed_field_to_index_key` once,
`build_index_key_from_record` twice — duplicate — ,
`build_index_key_from_record_into` once) in `types.h`, plus
`extract_local_key`'s own declaration/definition pair.

**Completeness note:** an independent review pass on an earlier version of
this plan (2026-08-18) found the two `query_bulk.c:666`/`683`
`typed_field_to_index_key` calls and the `query_bulk.c:5157`
`build_index_key_from_record` call missing from the tables above — all
three are grep-confirmed (`grep -rn "typed_field_to_index_key(\|build_index_key_from_record("
src/db/*.c`) and now included. This grep is the authoritative
completeness check for these three functions' call sites; re-run it
verbatim before starting execution and diff against the tables above —
any new hit means the plan is stale and must be updated before
proceeding, per this repo's "if a quoted anchor isn't found exactly...
halt" rule extended to call-site coverage.

### Finding since folded into scope: `typed_field_to_buf_raw`

`extract_local_key`'s non-composite branch (`src/db/query_join.c:217-221`)
and several other sites (`query_join.c:353`, `357`, `495`;
`query_aggregate.c:2024`) call `typed_field_to_buf_raw(f, p, buf, bufsz)` —
a sibling function with the **same** unbounded-read shape (`p` is read for
up to `f->size` bytes with no caller-supplied bound on how many bytes are
actually valid at `p`). This is the same hazard class as root cause 2.
Originally flagged here as out-of-scope pending independent verification
that it's reachable with a trim-compacted buffer; that verification is now
done (two of the five call sites are confirmed reachable via ordinary
GROUP BY aggregate queries and ordinary join queries against
trim-compacted records) — **now in scope, fixed below as root cause 4 /
fix 4**, which supersedes this note with a full call-site audit.

## Root cause 3: `typed_get_field_str` re-applies `f->offset` to its own
zero sentinel, overrunning it for late fields in wide schemas

**File:** `src/db/config.c`, function `typed_get_field_str`
(`src/db/config.c:3139-3292`).

Already documented as a correct **precedent** for root cause 2's fix (see
above): the function computes a bounds check and substitutes a
`static const uint8_t zero_field[65537]` sentinel for an out-of-range
field. But unlike the fix this plan writes for
`typed_field_to_index_key` (which folds `+ f->offset` into the pointer
*before* branching, so the sentinel is used unshifted), the existing
`typed_get_field_str` swaps in `zero_field` **unshifted at the top**, then
every `switch` case re-applies `+ f->offset` itself:

```c
    static const uint8_t zero_field[65537];
    const uint8_t *src = (data_len >= 0 &&
                            (size_t)f->offset + (size_t)f->size > (size_t)data_len)
        ? zero_field
        : data;
    ...
    switch (f->type) {
    case FT_VARCHAR: {
        const uint8_t *p = src + f->offset;   /* reads zero_field + f->offset */
```

`zero_field` is sized to the single largest field (65535-byte varchar
content + 2-byte length prefix = 65537). `f->offset` is the field's offset
**within the whole record**, unbounded by 65537 for a schema with many
fields. `MAX_FIELDS = 256`; a schema whose fields are dominated by large
varchar columns (e.g. multiple `varchar:65535` fields) reaches a
cumulative offset past 65537 well within that cap — two `varchar:65535`
fields already push the third field's offset to `2 * 65537 = 131074`. Any
such field, on a trim-compacted record short enough to fail the bounds
check, causes `zero_field + f->offset` — a static-storage read
`131074 - 65537 = 65537` bytes past the end of `zero_field`. This is a
**global-buffer-overflow READ** (ASan flags static/global arrays the same
way as heap allocations), independent of and in addition to root cause 2's
heap-buffer-overflow reads on `old->value`.

Reachability: any `find`/`fetch`/CSV/dict-format read of a trim-compacted
record through a schema with `total_size > 65537` reaches this — e.g. the
`typed_get_field_str` call in `query_join.c:666` and `query.c:4524`
(`rows_fmt` JSON emission) already pass a real `data_len`
(`hdr->value_len` / `value_len`) that correctly identifies the record as
trim-compacted; the bug is purely inside the function's own use of its
sentinel, not in how callers invoke it.

### Fix 3

Fold `+ f->offset` into the ternary itself, exactly as fix 2 does for
`typed_field_to_index_key`, so `src` already points at the correct read
position for both branches (`zero_field` at its own base — never
re-offset — or `data + f->offset`), then remove the now-redundant
`+ f->offset` from every switch-case body. Self-contained to one function;
no signature change, no external call-site changes. Because several
switch-case bodies use identical-looking local variable declarations
(e.g. `const uint8_t *d = src + f->offset;` appears verbatim for
`FT_DATE`, `FT_DATETIME`, `FT_DATETIMEMS`, and `FT_TIME`), a small
per-line anchor would not be unique within the file — the anchor and
replacement below are therefore the complete function body.

**Anchor:**
```c
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data,
                           int data_len, int field_idx) {
    if (!ts || field_idx < 0 || field_idx >= ts->nfields) return NULL;
    const TypedField *f = &ts->fields[field_idx];
    if (f->removed) return NULL;

    /* For trim-encoded records: any field that ends past stored bytes → treat as zero/empty.
       Uses the same condition as the original typed_decode break, which is
       f->offset + f->size > data_len rather than f->offset >= data_len — this is
       correct for both field-boundary trim and any hypothetical byte-level trim. */
    static const uint8_t zero_field[65537];
    const uint8_t *src = (data_len >= 0 &&
                            (size_t)f->offset + (size_t)f->size > (size_t)data_len)
        ? zero_field
        : data;

    char buf[512];
    int len;

    switch (f->type) {
    case FT_VARCHAR: {
        const uint8_t *p = src + f->offset;
        int slen = ((int)p[0] << 8) | (int)p[1];
        int content_max = f->size - 2;
        if (slen > content_max) slen = content_max;
        if (slen == 0) return NULL;
        char *out = malloc(slen + 1);
        memcpy(out, p + 2, slen);
        out[slen] = '\0';
        return out;
    }
    case FT_BOOL:
        return strdup(src[f->offset] ? "true" : "false");
    case FT_DATE: {
        const uint8_t *d = src + f->offset;
        int32_t v = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                    ((int32_t)d[2] << 8) | d[3];
        if (v == 0) return NULL;
        char *out = malloc(9);
        snprintf(out, 9, "%08d", v);
        return out;
    }
    case FT_DATETIME: {
        const uint8_t *d = src + f->offset;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint16_t t = ((uint16_t)d[4] << 8) | d[5];
        if (dv == 0 && t == 0) return NULL;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        char *out = malloc(15);
        snprintf(out, 15, "%08d%02d%02d%02d", dv, hh, mm, ss);
        return out;
    }
    case FT_DATETIMEMS: {
        const uint8_t *d = src + f->offset;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint32_t ms = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                      ((uint32_t)d[6] << 8) | d[7];
        if (dv == 0 && ms == 0) return NULL;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        char *out = malloc(18);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(18) failed for field [%s]", f->name);
            return NULL;
        }
        snprintf(out, 18, "%08d%02d%02d%02d%03d", dv, hh, mm, ss, fff);
        return out;
    }
    case FT_TIME: {
        const uint8_t *d = src + f->offset;
        uint32_t secs = ((uint32_t)d[0] << 16) | ((uint32_t)d[1] << 8) | d[2];
        if (secs == 0 && d[0]==0 && d[1]==0 && d[2]==0) return NULL;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        char *out = malloc(9);
        snprintf(out, 9, "%02d:%02d:%02d", hh, mm, ss);
        return out;
    }
    case FT_UUID: {
        const uint8_t *b = src + f->offset;
        if (uuid_is_zero(b)) return NULL;
        char *out = malloc(37);
        uuid_format_canonical(out, 37, b);
        return out;
    }
    case FT_IPV4: {
        const uint8_t *ip = src + f->offset;
        if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
            return NULL;
        char *out = malloc(INET_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET, ip, out, INET_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_IPV6: {
        const uint8_t *ip = src + f->offset;
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (ip[bi] != 0) { allzero = 0; break; }
        if (allzero) return NULL;
        char *out = malloc(INET6_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET6_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET6, ip, out, INET6_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET6) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_ENUM: {
        /* typed_get_field_str is a raw display-string API.  Do not use the
           JSON-fragment returned by decode_field_to_buf here: projection
           emitters escape this result and add the one required JSON layer
           themselves. */
        if (!f->enum_values || f->n_enum_values <= 0) return NULL;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)src[f->offset] << 8) |
                            (uint16_t)src[f->offset + 1])
                    : (int)src[f->offset];
        if (idx < 0 || idx >= f->n_enum_values)
            return strdup("");
        return strdup(f->enum_values[idx] ? f->enum_values[idx] : "");
    }
    case FT_LONG:
    case FT_TIMESTAMP:
    case FT_INT:
    case FT_SHORT:
    case FT_DOUBLE:
    case FT_FLOAT:
    case FT_BYTE:
    case FT_NUMERIC:
        /* decode_field_to_buf already renders these as bare numeric text.
           Make that text the raw value returned by this API, rather than
           allowing projection callers to receive a JSON fragment or quote
           the number as a string. */
        len = decode_field_to_buf(f, src + f->offset, buf, sizeof(buf));
        if (len <= 0) return NULL;
        return strdup(buf);
    default:
        len = decode_field_to_buf(f, src + f->offset, buf, sizeof(buf));
        if (len <= 0) return NULL;
        return strdup(buf);
    }
}
```

Replace with:

```c
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data,
                           int data_len, int field_idx) {
    if (!ts || field_idx < 0 || field_idx >= ts->nfields) return NULL;
    const TypedField *f = &ts->fields[field_idx];
    if (f->removed) return NULL;

    /* For trim-encoded records: any field that ends past stored bytes → treat as zero/empty.
       Uses the same condition as the original typed_decode break, which is
       f->offset + f->size > data_len rather than f->offset >= data_len — this is
       correct for both field-boundary trim and any hypothetical byte-level trim.
       f->offset is folded in HERE, once, before the sentinel-vs-data choice —
       every case below then reads directly from src with no further offset
       arithmetic. This differs from the pre-fix version, which selected
       zero_field unshifted and re-applied "+ f->offset" inside each case —
       correct only while f->offset stayed under sizeof(zero_field) (65537);
       a schema with several large varchar fields (MAX_FIELDS = 256) can push
       a later field's offset well past that, which walked zero_field +
       f->offset off the end of the 65537-byte static array (a
       global-buffer-overflow READ, same hazard class as root cause 2 but on
       static storage instead of a heap allocation). zero_field only ever
       needs to cover one field's width (<= 65537 bytes for the largest
       varchar), never the field's absolute offset in the record, so using it
       unshifted removes the bound on schema size entirely. */
    static const uint8_t zero_field[65537];
    const uint8_t *src = (data_len >= 0 &&
                            (size_t)f->offset + (size_t)f->size > (size_t)data_len)
        ? zero_field
        : data + f->offset;

    char buf[512];
    int len;

    switch (f->type) {
    case FT_VARCHAR: {
        const uint8_t *p = src;
        int slen = ((int)p[0] << 8) | (int)p[1];
        int content_max = f->size - 2;
        if (slen > content_max) slen = content_max;
        if (slen == 0) return NULL;
        char *out = malloc(slen + 1);
        memcpy(out, p + 2, slen);
        out[slen] = '\0';
        return out;
    }
    case FT_BOOL:
        return strdup(src[0] ? "true" : "false");
    case FT_DATE: {
        const uint8_t *d = src;
        int32_t v = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                    ((int32_t)d[2] << 8) | d[3];
        if (v == 0) return NULL;
        char *out = malloc(9);
        snprintf(out, 9, "%08d", v);
        return out;
    }
    case FT_DATETIME: {
        const uint8_t *d = src;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint16_t t = ((uint16_t)d[4] << 8) | d[5];
        if (dv == 0 && t == 0) return NULL;
        int hh = t / 3600, mm = (t % 3600) / 60, ss = t % 60;
        char *out = malloc(15);
        snprintf(out, 15, "%08d%02d%02d%02d", dv, hh, mm, ss);
        return out;
    }
    case FT_DATETIMEMS: {
        const uint8_t *d = src;
        int32_t dv = ((int32_t)d[0] << 24) | ((int32_t)d[1] << 16) |
                     ((int32_t)d[2] << 8) | d[3];
        uint32_t ms = ((uint32_t)d[4] << 24) | ((uint32_t)d[5] << 16) |
                      ((uint32_t)d[6] << 8) | d[7];
        if (dv == 0 && ms == 0) return NULL;
        int hh = ms / 3600000, mm = (ms % 3600000) / 60000,
            ss = (ms % 60000) / 1000, fff = ms % 1000;
        char *out = malloc(18);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(18) failed for field [%s]", f->name);
            return NULL;
        }
        snprintf(out, 18, "%08d%02d%02d%02d%03d", dv, hh, mm, ss, fff);
        return out;
    }
    case FT_TIME: {
        const uint8_t *d = src;
        uint32_t secs = ((uint32_t)d[0] << 16) | ((uint32_t)d[1] << 8) | d[2];
        if (secs == 0 && d[0]==0 && d[1]==0 && d[2]==0) return NULL;
        int hh = secs / 3600, mm = (secs % 3600) / 60, ss = secs % 60;
        char *out = malloc(9);
        snprintf(out, 9, "%02d:%02d:%02d", hh, mm, ss);
        return out;
    }
    case FT_UUID: {
        const uint8_t *b = src;
        if (uuid_is_zero(b)) return NULL;
        char *out = malloc(37);
        uuid_format_canonical(out, 37, b);
        return out;
    }
    case FT_IPV4: {
        const uint8_t *ip = src;
        if (ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0)
            return NULL;
        char *out = malloc(INET_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET, ip, out, INET_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_IPV6: {
        const uint8_t *ip = src;
        int allzero = 1;
        for (int bi = 0; bi < 16; bi++) if (ip[bi] != 0) { allzero = 0; break; }
        if (allzero) return NULL;
        char *out = malloc(INET6_ADDRSTRLEN);
        if (!out) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: malloc(INET6_ADDRSTRLEN) failed for field [%s]", f->name);
            return NULL;
        }
        if (!inet_ntop(AF_INET6, ip, out, INET6_ADDRSTRLEN)) {
            LOG_ERROR(LOG_SUB_CONFIG, "typed_get_field_str: inet_ntop(AF_INET6) failed for field [%s]: %s", f->name, strerror(errno));
            free(out);
            return NULL;
        }
        return out;
    }
    case FT_ENUM: {
        /* typed_get_field_str is a raw display-string API.  Do not use the
           JSON-fragment returned by decode_field_to_buf here: projection
           emitters escape this result and add the one required JSON layer
           themselves. */
        if (!f->enum_values || f->n_enum_values <= 0) return NULL;
        int idx = (f->enum_width == 2)
                    ? (int)(((uint16_t)src[0] << 8) |
                            (uint16_t)src[1])
                    : (int)src[0];
        if (idx < 0 || idx >= f->n_enum_values)
            return strdup("");
        return strdup(f->enum_values[idx] ? f->enum_values[idx] : "");
    }
    case FT_LONG:
    case FT_TIMESTAMP:
    case FT_INT:
    case FT_SHORT:
    case FT_DOUBLE:
    case FT_FLOAT:
    case FT_BYTE:
    case FT_NUMERIC:
        /* decode_field_to_buf already renders these as bare numeric text.
           Make that text the raw value returned by this API, rather than
           allowing projection callers to receive a JSON fragment or quote
           the number as a string. */
        len = decode_field_to_buf(f, src, buf, sizeof(buf));
        if (len <= 0) return NULL;
        return strdup(buf);
    default:
        len = decode_field_to_buf(f, src, buf, sizeof(buf));
        if (len <= 0) return NULL;
        return strdup(buf);
    }
}
```

### Edge cases (Fix 3, explicit)

- **Sentinel sizing is independent of schema width.** Because `src` is now
  either `data + f->offset` (in range) or `zero_field` **unshifted**, the
  static array only ever needs to be as large as the single widest field
  this function reads (65535-byte varchar content + 2-byte length prefix =
  65537; every other type reads <= 16 bytes from `src`). No change to
  `zero_field`'s size is needed — only to how it's addressed.
- **`FT_VARCHAR` straddling the boundary.** Same reasoning as fix 2's
  first edge case: the bounds check is on the whole field's
  `[offset, offset+size)` range, so a record trimmed mid-field is treated
  as fully out of range (empty varchar), never a partial read of a
  half-present length prefix.
- **`data_len < 0`.** The existing `data_len >= 0 && ...` guard is
  unchanged — a negative `data_len` (meaning "no bound, trust `data`
  fully" — used by call sites that already know they're passing a full,
  untrimmed record) still selects `data + f->offset` unconditionally, same
  as before this fix.
- **No behavior change for any in-range field.** For every field where
  `f->offset + f->size <= data_len`, `src` is exactly what it was before
  (`data + f->offset`) — the fix only changes which bytes are read when
  the *zero* branch is taken.

## Root cause 4: `typed_field_to_buf_raw` callers read past
trim-compacted record buffers with no bounds check at all

**Function (unchanged):** `typed_field_to_buf_raw`
(`src/db/query_aggregate.c:939-...`) takes an already-positioned pointer
`p` and has no length awareness by design — like `encode_field`, it trusts
the caller to have positioned `p` validly and to only ask it to read
`f->size` bytes starting there. That contract is fine; the bug is that
several **callers** pass `record_base + tf->offset` (or an
already-offset pointer) into it with **no bounds check at all** — not
even the "trust `data_len`" pattern root causes 2 and 3 already use, just
a raw unconditional pointer arithmetic. `typed_field_to_buf_raw` itself is
not modified by this fix; every change below is at a call site (or at a
function that forwards a caller-supplied pointer to it).

A full grep of `typed_field_to_buf_raw(` across `src/db/` turns up exactly
five call sites, in exactly two files — `src/db/query_aggregate.c` and
`src/db/query_join.c` (its own definition and the one declaration in
`query_internal.h:394-395` aside). No call sites exist in `query.c`,
`query_bulk.c`, or anywhere else in the tree.

| File:Line | Calling function | Pointer expression |
|---|---|---|
| `query_aggregate.c:2024-2026` | `agg_scan_cb` | `raw + ctx->group_tfs[i]->offset` |
| `query_join.c:218-220` | `extract_local_key` (non-composite branch) | `driver_raw + j->local_tf->offset` |
| `query_join.c:353` | `buf_field_value`, `FT_DATE`/`FT_DATETIME`/`FT_DATETIMEMS`/`FT_IPV4`/`FT_IPV6`/`FT_ENUM` branch | `field_ptr` (caller-supplied, unmodified) |
| `query_join.c:357` | `buf_field_value`, default branch | `field_ptr` (caller-supplied, unmodified) |
| `query_join.c:495-497` | `build_joined_csv_row` | `rraw + j->proj_tfs[k]->offset` |

**Sixth site, same root cause, different function — found live at the
pre-fix-4 checkpoint after the above five were fixed and reverified.**
`agg_scan_cb` extracts each group field **twice**: once into the string
buffer via `typed_field_to_buf_raw` (guarded above), and — when
`ctx->use_int_keys` is set — a second time into a raw integer hash key
via a sibling helper, `typed_field_to_raw` (`query_aggregate.c:1848-1854`,
defined ~170 lines above `agg_scan_cb`; not a grep match for
`typed_field_to_buf_raw(` since it's a differently-named function, which
is why the original five-site grep didn't surface it). Its `memcpy(buf,
p, (size_t)w)` at line 1852 has no bounds check on `p` at all, and its
caller passes the same unguarded pointer expression as the original bug:

| File:Line | Calling function | Pointer expression |
|---|---|---|
| `query_aggregate.c:2058-2059` | `agg_scan_cb` (integer-key fast-path loop) | `raw + ctx->group_tfs[i]->offset` |

`ctx->use_int_keys` is set whenever every group field's `type_desc(...)
->int_width > 0` (`typed_field_int_width`, `query_aggregate.c:1837-1840`)
— `FT_BYTE` is in that set (`type_desc.c:16`, width 1), so any group-by
query on a `byte` (or `short`/`int`/`long`/`numeric`/`date`) field runs
**both** extraction loops unconditionally on the same trimmed `raw`
buffer; guarding only the string-buffer loop leaves this one fully
exposed. Confirmed live via ASan against the actual
`test-trim-compact-oob-field4` fixture (`f2:byte`, the same one fix 4's
first five sites were verified against) after fix 4's first five sites
were applied and the checkpoint's original finding was resolved:
```
==PID==ERROR: AddressSanitizer: heap-buffer-overflow ... READ of size 1
    #0 memcpy
    #1 typed_field_to_raw src/db/query_aggregate.c:1852
    #2 agg_scan_cb src/db/query_aggregate.c:2058
    #3 keyset_emit_agg_cb src/db/query_aggregate.c:49
    ... keyset_emit_agg -> keyset_agg_from_or -> agg_run_plan
    ... -> cmd_aggregate_do -> cmd_aggregate -> dispatch_json_query
0x... is located 0 bytes after 2556-byte region [...]
allocated by ... keyset_emit_agg_cb src/db/query_aggregate.c:44
SUMMARY: AddressSanitizer: heap-buffer-overflow src/db/query_aggregate.c:1852 in typed_field_to_raw
```
Same 2556-byte allocation, same producer (`keyset_emit_agg_cb`'s
`malloc(klen+vlen)` fallback) as the already-fixed string-path site —
this is the identical buffer, read out of bounds a second, independent
way. Unlike the string-path overrun (which reliably aborted the daemon
even under `halt_on_error=0`), this integer-path overrun did **not**
abort the process — the request completed and returned `[{"f2":"0","cnt":1}]`,
i.e. the functional assertion passes while ASan still correctly flags
the read as unsafe. This is expected: with only one record in the
fixture, the single garbage byte folded into the raw hash key can't
produce a wrong *count* (there's nothing for the lone group to collide
or fail to collide with) — the bug is real regardless of whether this
particular fixture's assertions happen to still pass around it.
**Category: genuine heap-buffer-overflow, same producer/reachability as
site 1** (`agg_scan_cb`'s `raw`, point 1 below).

`buf_field_value` (lines 353/357) forwards whatever pointer it's handed —
it is not itself a source of the bug, but the fix must guard **its own**
three call sites (below) since that's where `record_raw + tf->offset` is
first computed.

### Where each buffer actually comes from, and how tight it is

This determines whether an out-of-range read is a genuine
heap/global-buffer-overflow (ASan-catchable) or an in-bounds read of
stale/wrong data (a correctness bug, not a memory-safety violation). Each
was traced to its producer:

1. **`agg_scan_cb`'s `raw`** (`raw = block + hdr->key_len`, `hdr->value_len`
   is the record's real vlen). `block` comes from exactly two producers,
   both **tight, unpadded allocations, never zero-initialized past
   `klen+vlen`**:
   - `keyset_emit_agg_cb` (`query_aggregate.c:42-44`):
     `uint8_t *block = (klen + vlen + 1 < sizeof(stk)) ? stk : malloc(klen + vlen);`
     — the stack fallback `stk[2048]` is used for small records (reads past
     `vlen` within it are in-bounds but uninitialized stack garbage, not
     memory-unsafe); `malloc(klen + vlen)` (no `+1` slack, no zero-init) is
     used once `klen + vlen + 1 >= 2048` — a **genuine heap-buffer-overflow
     READ** for any field at/after `vlen` on such a record.
   - `keyset_emit_agg`'s sequential fallback (`query_aggregate.c:81-88`),
     same `stk[2048]`-or-`malloc(rr.klen + rr.vlen)` pattern, same
     conclusion.
   - **Category: genuine heap-buffer-overflow** (reachable whenever the
     record is large enough to route through either `malloc` branch — not
     a rare corner case; any record with `klen + vlen >= 2047` does).
   - Reachable via: any GROUP BY aggregate query whose primary plan shape
     is OR (`PRIMARY_KEYSET`) or AND-intersection (`PRIMARY_INTERSECT`),
     since both route through `keyset_emit_agg`/`keyset_emit_agg_cb` — an
     ordinary aggregate query, no update or trim-adjacent operation
     required beyond the record being trim-compacted in the first place.

2. **`extract_local_key`'s `driver_raw`** — traced through both its
   callers (per fix 2's `extract_local_key` table above):
   - `query_join.c:599` (`adv_search_cb`) — `driver_raw = raw = block +
     hdr->key_len`, where `block` comes from the O_DIRECT variable-length
     segment scan (`seg_scan_o_direct`, `src/db/io_direct.c`). On-disk
     segment records are **variable-length** (`od_varlen_rec_size(klen,
     vlen) = round8(24 + klen + vlen)`, `src/db/io_direct.c:541-544`) —
     **not** padded to the schema's `total_size` or to `slot_size`; a
     trim-compacted record's on-disk footprint is exactly its trimmed
     size. The scanner's callback (`cb`, `src/db/io_direct.c:767` and
     `:818`) is invoked with one of two different backing buffers
     depending on whether the record straddles an O_DIRECT chunk read
     boundary:
     - **Fast path** (`io_direct.c:818`, record fully inside the current
       chunk): `rec` points into the double-buffered chunk (`dc.buf[...]`,
       many MB). Reading past the record's own `rec_size` here lands on
       the *next* record's on-disk bytes (or trailing chunk padding) —
       still in-bounds of the chunk allocation, but **wrong data**
       (another record's header/key/value bytes reinterpreted as this
       record's field), not a memory-safety violation by itself.
     - **Boundary-straddle path** (`io_direct.c:757-767`): `carry` is
       `realloc`'d to **exactly** `rec_size = round8(24+klen+vlen)` (no
       slack for the schema's `total_size`), and `cb` is invoked with
       `carry` as both `rec` and the hash pointer. Reading past `rec_size`
       here is a **genuine heap-buffer-overflow READ** on the `realloc`'d
       `carry` buffer. Straddling is an ordinary, data-dependent
       occurrence (any record whose start offset lands close enough to a
       chunk boundary) — not a rare edge case, so this call site must be
       treated as reachable heap-buffer-overflow, not merely
       stale-data-read, even though the fast path alone would only be the
       weaker class.
     - **Category: mixed / must treat as reachable heap-buffer-overflow**
       (chunk-boundary case dominates).
   - `query.c:4454` — `driver_raw = raw = fvals[fi]`, populated by
     `kef_fetch_cb` (`query.c:4331`): `c->vals[i] = malloc(vlen); ...
     memcpy(c->vals[i], value, vlen);` — **tight, unpadded, never
     zero-initialized**.
     - **Category: genuine heap-buffer-overflow.**
   - Reachable via: ordinary indexed `find`/`join` queries (both
     `adv_search_cb`'s full-scan path and `keyset_emit_find`'s
     indexed/keyset path serve ordinary join queries).

3. **`buf_field_value`'s three callers** (`buf_join_values:378`,
   `buf_driver_values:398` and `:411`) — traced by tracing *their* own
   callers' `remote_raw`/`driver_raw`:
   - `buf_join_values`'s `remote_raw` (called from `query_join.c:646` and
     `query.c:4499`) is always `join_refs[i].val` / `jrr[i].val` — a
     `RecordRef` populated by `lookup_remote` → `read_record_ref`
     (`query_find.c:352-367`). `read_record_ref` **`memset`s the whole
     `RecordRef` (including `inline_buf[2048]`) to zero before
     populating it** (`query_find.c:355`), then `v2_record_capture_cb`
     (`query_find.c:333-350`) picks `v2_buf = inline_buf` when
     `klen+vlen+1 <= 2048` (reads past `vlen` within `inline_buf` are
     in-bounds **and deterministically zero** — not a bug at all for
     small records, an accidental preexisting safety net) or `v2_buf =
     malloc(total)` (`total = klen+vlen+1`) for larger records — **tight,
     not zero-initialized beyond `vlen`**.
     - **Category: mixed by record size** — safe (zero-filled, in-bounds)
       for records whose `klen+vlen+1 <= 2048`; **genuine
       heap-buffer-overflow** for larger records (reachable — `MAX_FIELDS
       = 256` schemas routinely exceed 2048 bytes).
   - `buf_driver_values`'s `driver_raw` (called from `query_join.c:641`
     and `query.c:4482`) is the same `raw`/`fvals[fi]` traced in point 2
     above (`adv_search_cb`'s scan buffer, or `kef_fetch_cb`'s tight
     `malloc(vlen)`).
     - **Category: same as point 2** (mixed/heap-buffer-overflow via the
       carry-straddle path, or genuine heap-buffer-overflow via
       `kef_fetch_cb`).
   - Reachable via: ordinary join queries with either JSON tabular or
     CSV output (`buf_join_values`/`buf_driver_values` back the tabular
     JSON path; `build_joined_csv_row`, point 4 below, backs CSV).

4. **`build_joined_csv_row`'s `rraw`** (`query_join.c:487`,
   `rraw = jraws ? jraws[i] : NULL`) — same `RecordRef.val` source as
   `buf_join_values` in point 3 (`join_refs[i].val` / `jrr[i].val`,
   passed through the `jraws`/`jraws[i]` derived-pointer array). Same
   mixed classification as point 3's `buf_join_values` case.
   - **`build_joined_csv_row`'s *driver*-field cells — same function,
     separate hazard, now folded into fix 4's scope:**
     (`query_join.c:468` and `:478`) call
     `typed_get_field_str(driver_fs->ts, driver_raw,
     driver_fs->ts->total_size, idx)` — passing the schema's
     **`total_size`** (the field region's full capacity) as `data_len`,
     not the driver record's actual length. Since `total_size` is by
     definition `>=` every field's `offset+size`, fix 3's bounds check at
     that call site can never trigger — `typed_get_field_str` always reads
     from the real (possibly trim-compacted-short) `driver_raw` there,
     regardless of how short it actually is. This is the same hazard
     class as root causes 2/3, and unlike `typed_field_to_buf_raw`
     (which has no bounds check of its own), the fix here needs no new
     zero-sentinel guard at all — `typed_get_field_str` already does its
     own bounds check correctly (fix 3), it just needs the *right*
     `data_len` argument. Because fix 4 is already adding a driver-length
     parameter to this same function's signature (see "Design decision"
     below — originally to carry `rlen` for the *joined*-field cells),
     threading that same length through to the *driver*-field cells too
     is a one-line-per-call-site change, not a second signature bump.
     Folded into fix 4's `build_joined_csv_row` edit below rather than
     kept as a separate deferred item, per explicit human direction
     ("for item 2, we should add it to the plan if its not too big of a
     change. One time fix all bugs.", 2026-08-18 review response) — this
     is not too big a change; see the `driver_len` parameter and the new
     driver-fields-loop anchor/replacement under "Fix 4" below.

### Fix 4

#### Shared zero-sentinel — explicit design decision

Root causes 2 and 3 each already carry their own function-local
`static const uint8_t zero_field[65537]`. Fix 4 needs the same sentinel in
**six** call sites (`agg_scan_cb`'s string-buffer loop, `agg_scan_cb`'s
integer-key loop — same function, two separate reads of the same `raw`,
see the sixth-site writeup above — plus `extract_local_key`,
`buf_join_values`, `buf_driver_values`, `build_joined_csv_row`). Six
function-local copies would add `6 * 65537 ≈ 384 KB` of duplicated BSS
and six duplicated definitions to maintain. **Decision: introduce one
shared, canonical symbol for fix 4's new sites** — `const uint8_t
g_zero_field_65537[65537]`, defined once in `src/db/util.c` and declared
`extern` in `types.h` — rather than repeating the function-local-static
pattern six times over (originally decided as "a fifth and sixth time"
when only five sites were known; the sixth site found later reuses the
same symbol, not a second one).
Root causes 2 and 3's existing local statics are **left untouched** (not
in scope to revisit per this plan's own "do not change Fix 1 or Fix 2's
content... unless directly inconsistent" instruction) — this intentionally
leaves the codebase with two conventions side by side (2 function-local
statics from fix 2/3, 1 shared symbol from fix 4) rather than unifying
them, which would mean re-touching already-settled fix 2/3 code as an
unrelated drive-by change. Flagging this explicitly per the instruction to
surface such decisions rather than silently pick one.

**Anchor** (`src/db/types.h`, immediately after `typed_get_field_str`'s
declaration):
```c
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data,
                           int data_len, int field_idx);
void encode_field(const TypedField *f, const char *val, uint8_t *out);
```
Replace with:
```c
char *typed_get_field_str(const TypedSchema *ts, const uint8_t *data,
                           int data_len, int field_idx);
/* Shared zero-fill sentinel for fix-4 call sites that guard
   typed_field_to_buf_raw against reading past a trim-compacted record's
   actual length (agg_scan_cb, extract_local_key, buf_join_values,
   buf_driver_values, build_joined_csv_row — see
   docs/plans/2026-08-17-bool-literal-merge-bug-fix.md root cause 4).
   Sized to the single largest field these call sites ever read
   (65535-byte varchar content + 2-byte length prefix); every other type
   reads far fewer bytes. Root causes 2 and 3 use their own
   function-local static zero_field arrays instead of this symbol — see
   that plan's "shared zero-sentinel" note for why the two conventions
   were left to coexist rather than unified. */
extern const uint8_t g_zero_field_65537[65537];
void encode_field(const TypedField *f, const char *val, uint8_t *out);
```

**Anchor** (`src/db/util.c`, immediately before the FT_UUID helpers
comment block):
```c
/* ========== FT_UUID helpers ==========
 * Shared by config.c (decode_field_to_buf, typed_get_field_str) and
 * query.c (typed_field_to_buf_raw, decode_idx_to_buf). The all-zero
 * sentinel is the on-disk "unset" marker for a UUID column. */
```
Replace with:
```c
/* Shared zero-fill sentinel — see the declaration in types.h for what
   uses this and why. Zero-initialized static storage (BSS), not heap;
   never written after program start. */
const uint8_t g_zero_field_65537[65537];

/* ========== FT_UUID helpers ==========
 * Shared by config.c (decode_field_to_buf, typed_get_field_str) and
 * query.c (typed_field_to_buf_raw, decode_idx_to_buf). The all-zero
 * sentinel is the on-disk "unset" marker for a UUID column. */
```

#### `agg_scan_cb` body change

**Anchor** (`src/db/query_aggregate.c`):
```c
    for (int i = 0; i < ctx->ngroups; i++) {
        gbuf[i][0] = '\0';
        if (ctx->group_tfs[i]) {
            typed_field_to_buf_raw(ctx->group_tfs[i],
                                   raw + ctx->group_tfs[i]->offset,
                                   gbuf[i], sizeof(gbuf[i]));
        } else {
```
Replace with:
```c
    for (int i = 0; i < ctx->ngroups; i++) {
        gbuf[i][0] = '\0';
        if (ctx->group_tfs[i]) {
            const TypedField *gtf = ctx->group_tfs[i];
            /* raw may be backed by a trim-compacted record's tight
               allocation (keyset_emit_agg_cb's malloc(klen+vlen), or the
               sequential fallback's malloc(rr.klen+rr.vlen) — neither has
               slack past hdr->value_len). A field at/after hdr->value_len
               is a heap-buffer-overflow READ on that allocation; substitute
               the shared zero sentinel instead of reading past it. */
            const uint8_t *fp = ((size_t)gtf->offset + (size_t)gtf->size >
                                  (size_t)hdr->value_len)
                ? g_zero_field_65537
                : raw + gtf->offset;
            typed_field_to_buf_raw(gtf, fp, gbuf[i], sizeof(gbuf[i]));
        } else {
```

#### `agg_scan_cb` integer-key loop body change (sixth site)

Same function, a few lines further down, guarding the parallel
`typed_field_to_raw` extraction used when `ctx->use_int_keys` is set —
see the sixth-site writeup above for why this is a separate read of the
same `raw` buffer that the string-buffer loop's fix above does not cover.

**Anchor** (`src/db/query_aggregate.c`):
```c
    uint8_t raw_key[AGG_INT_KEY_CAP];
    int raw_key_len = 0;
    if (ctx->use_int_keys) {
        int kp = 0;
        for (int i = 0; i < ctx->ngroups && kp < AGG_INT_KEY_CAP; i++) {
            if (ctx->group_tfs[i]) {
                int len = typed_field_to_raw(ctx->group_tfs[i],
                                             raw + ctx->group_tfs[i]->offset,
                                             raw_key + kp,
                                             (size_t)(AGG_INT_KEY_CAP - kp));
                if (len > 0) kp += len;
            }
        }
        raw_key_len = kp;
    }
```
Replace with:
```c
    uint8_t raw_key[AGG_INT_KEY_CAP];
    int raw_key_len = 0;
    if (ctx->use_int_keys) {
        int kp = 0;
        for (int i = 0; i < ctx->ngroups && kp < AGG_INT_KEY_CAP; i++) {
            if (ctx->group_tfs[i]) {
                const TypedField *gtf = ctx->group_tfs[i];
                /* Same trim-compacted-buffer hazard as the string-buffer
                   loop above, and the same fix: a field at/after
                   hdr->value_len is a heap-buffer-overflow READ on
                   keyset_emit_agg_cb's/keyset_emit_agg's tight malloc'd
                   block — substitute the shared zero sentinel instead. */
                const uint8_t *fp = ((size_t)gtf->offset + (size_t)gtf->size >
                                      (size_t)hdr->value_len)
                    ? g_zero_field_65537
                    : raw + gtf->offset;
                int len = typed_field_to_raw(gtf, fp,
                                             raw_key + kp,
                                             (size_t)(AGG_INT_KEY_CAP - kp));
                if (len > 0) kp += len;
            }
        }
        raw_key_len = kp;
    }
```

#### `extract_local_key` body change

Combined with fix 2's signature change to `extract_local_key` (new
`size_t driver_len` parameter — see "`extract_local_key` signature
change" under fix 2 above; this is the **same** signature change, not a
second one, per this plan's instruction to treat both call sites inside
`extract_local_key` as one combined change).

**Anchor** (`src/db/query_join.c`):
```c
    if (j->local_tf) {
        return typed_field_to_buf_raw(j->local_tf,
                                      driver_raw + j->local_tf->offset,
                                      buf, bufsz);
    }
    return 0;
}
```
Replace with:
```c
    if (j->local_tf) {
        const TypedField *tf = j->local_tf;
        /* driver_raw may be a trim-compacted record read from a tight
           allocation (see this plan's root cause 4) — guard the same way
           the composite branch above already guards its
           typed_field_to_index_key call via driver_len. */
        const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
            ? g_zero_field_65537
            : driver_raw + tf->offset;
        return typed_field_to_buf_raw(tf, fp, buf, bufsz);
    }
    return 0;
}
```

#### `buf_join_values` signature + body change

**Anchor** (`src/db/query_join.c`):
```c
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        else
            pos += buf_field_value(j->proj_tfs[k],
                                   remote_raw + j->proj_tfs[k]->offset,
                                   buf + pos, bufsz - pos);
    }
    return pos;
}
```
Replace with:
```c
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw, size_t remote_len,
                           char *buf, size_t bufsz) {
    int pos = 0;
    if (j->include_remote_key) {
        /* v1: emit null — local field gives the value; extend later if needed */
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",null");
    }
    for (int k = 0; k < j->proj_count; k++) {
        if (pos >= (int)bufsz - 1) break;
        pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
        if (!remote_raw || !j->proj_tfs[k])
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        else {
            const TypedField *tf = j->proj_tfs[k];
            /* remote_raw is a RecordRef.val — safe (zero-filled) for
               records that fit read_record_ref's inline_buf, but a tight,
               non-zero-initialized malloc for larger records (see this
               plan's root cause 4, point 3). Guard the same way. */
            const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > remote_len)
                ? g_zero_field_65537
                : remote_raw + tf->offset;
            pos += buf_field_value(tf, fp, buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```

#### `buf_driver_values` signature + body change

**Anchor** (`src/db/query_join.c`):
```c
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    pos += buf_field_value(&driver_fs->ts->fields[idx],
                                           driver_raw + driver_fs->ts->fields[idx].offset,
                                           buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            pos += buf_field_value(&driver_fs->ts->fields[i],
                                   driver_raw + driver_fs->ts->fields[i].offset,
                                   buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```
Replace with:
```c
int buf_driver_values(const uint8_t *driver_raw, size_t driver_len, FieldSchema *driver_fs,
                             const char **driver_proj, int driver_proj_count,
                             char *buf, size_t bufsz) {
    int pos = 0;
    if (driver_proj_count > 0) {
        for (int i = 0; i < driver_proj_count; i++) {
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            if (driver_fs && driver_fs->ts) {
                int idx = typed_field_index(driver_fs->ts, driver_proj[i]);
                if (idx >= 0) {
                    const TypedField *tf = &driver_fs->ts->fields[idx];
                    /* driver_raw may be a trim-compacted record read from
                       a tight allocation (see this plan's root cause 4,
                       points 2 and 3). */
                    const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
                        ? g_zero_field_65537
                        : driver_raw + tf->offset;
                    pos += buf_field_value(tf, fp, buf + pos, bufsz - pos);
                    continue;
                }
            }
            pos += snprintf_bounded(buf + pos, bufsz - pos, "null");
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos >= (int)bufsz - 1) break;
            pos += snprintf_bounded(buf + pos, bufsz - pos, ",");
            const TypedField *tf = &driver_fs->ts->fields[i];
            const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > driver_len)
                ? g_zero_field_65537
                : driver_raw + tf->offset;
            pos += buf_field_value(tf, fp, buf + pos, bufsz - pos);
        }
    }
    return pos;
}
```

#### `build_joined_csv_row` signature + body change

Design decision: replace the `const uint8_t **jraws` parameter with
`const RecordRef *jrefs`. `RecordRef` already carries both `.val` and
`.vlen` for each join (populated by `lookup_remote` at both call sites,
per the table below) — passing the struct array instead of a
separately-derived raw-pointer array avoids introducing a second new
parallel array (e.g. a `jrlens` array) purely to carry the length
`build_joined_csv_row` now needs. `jraws`/`join_raws` themselves are
**not removed** — they're still used, unchanged, at the
`buf_join_values` call sites (which pass a pointer + a scalar length,
not an array).

At the same time, add a `size_t driver_len` parameter (immediately after
`driver_raw`, matching the `(ptr, len)` convention used everywhere else in
this plan). This is a second, independent fix folded into the same
signature bump: `build_joined_csv_row`'s *driver*-field cells
(`query_join.c:468`/`:478`, see root cause 4 point 4 above) call
`typed_get_field_str(driver_fs->ts, driver_raw, driver_fs->ts->total_size,
idx)`, passing the schema's full `total_size` as `data_len` instead of the
driver record's actual length — which defeats `typed_get_field_str`'s own
fix-3 bounds check for every trim-compacted driver record read through
this path (`total_size` is by construction `>=` every field's
`offset+size`, so the "out of range" branch can never be selected).
Because `typed_get_field_str` already does its own bounds check correctly
once given the right `data_len` (fix 3), no zero-sentinel logic is needed
at this call site — only the length that was already being computed and
then discarded by every caller (see the call-site table below).

**Anchor** (`src/db/query_join.c`):
```c
size_t build_joined_csv_row(const char *key,
                                   const uint8_t *driver_raw, FieldSchema *driver_fs,
                                   const char **proj_fields, int proj_count,
                                   const JoinSpec *joins, int njoins,
                                   const uint8_t **jraws,
                                   char csv_delim,
                                   char *buf, size_t bufsz) {
    size_t pos = 0;
    pos += csv_cell_to_buf(key, csv_delim, buf + pos, bufsz - pos);
    char tmp[1024];
```
Replace with:
```c
size_t build_joined_csv_row(const char *key,
                                   const uint8_t *driver_raw, size_t driver_len, FieldSchema *driver_fs,
                                   const char **proj_fields, int proj_count,
                                   const JoinSpec *joins, int njoins,
                                   const RecordRef *jrefs,
                                   char csv_delim,
                                   char *buf, size_t bufsz) {
    size_t pos = 0;
    pos += csv_cell_to_buf(key, csv_delim, buf + pos, bufsz - pos);
    char tmp[1024];
```

**Anchor** (driver-fields loop, same function):
```c
    /* Driver fields */
    if (proj_count > 0 && driver_fs && driver_fs->ts) {
        for (int i = 0; i < proj_count; i++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            int idx = typed_field_index(driver_fs->ts, proj_fields[i]);
            char *v = (idx >= 0)
                ? typed_get_field_str(driver_fs->ts, driver_raw, driver_fs->ts->total_size, idx)
                : NULL;
            (void)tmp;
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            char *v = typed_get_field_str(driver_fs->ts, driver_raw, driver_fs->ts->total_size, i);
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    }
```
Replace with:
```c
    /* Driver fields */
    if (proj_count > 0 && driver_fs && driver_fs->ts) {
        for (int i = 0; i < proj_count; i++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            int idx = typed_field_index(driver_fs->ts, proj_fields[i]);
            char *v = (idx >= 0)
                ? typed_get_field_str(driver_fs->ts, driver_raw, (int)driver_len, idx)
                : NULL;
            (void)tmp;
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    } else if (driver_fs && driver_fs->ts) {
        for (int i = 0; i < driver_fs->ts->nfields; i++) {
            if (driver_fs->ts->fields[i].removed) continue;
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            char *v = typed_get_field_str(driver_fs->ts, driver_raw, (int)driver_len, i);
            pos += csv_cell_to_buf(v, csv_delim, buf + pos, bufsz - pos);
            free(v);
        }
    }
```
`typed_get_field_str`'s `data_len` parameter is a signed `int` (fix 3
leaves this unchanged); `driver_len` is a `size_t` sourced from
`hdr->value_len` (`uint32_t`) or a local `value_len` at every call site
(see the call-site table below), both well under `INT_MAX` for any record
this engine can store (`max_value` is bounded by `MAX_FIELDS = 256`
typed fields, each `<= 65537` bytes) — the `(int)` cast is lossless in
practice, matching the same cast style already used for `data_len`
elsewhere in this codebase's `typed_get_field_str` callers.

**Anchor** (joined-fields loop, same function):
```c
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        const uint8_t *rraw = jraws ? jraws[i] : NULL;
        if (j->include_remote_key) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            /* Empty cell — local field carries the value (matches JSON's null). */
        }
        for (int k = 0; k < j->proj_count; k++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            if (!rraw || !j->proj_tfs[k]) continue;
            int n = typed_field_to_buf_raw(j->proj_tfs[k],
                                           rraw + j->proj_tfs[k]->offset,
                                           tmp, sizeof(tmp));
            if (n > 0) pos += csv_cell_to_buf(tmp, csv_delim, buf + pos, bufsz - pos);
        }
    }
```
Replace with:
```c
    for (int i = 0; i < njoins; i++) {
        const JoinSpec *j = &joins[i];
        const uint8_t *rraw = jrefs ? jrefs[i].val : NULL;
        size_t rlen = jrefs ? jrefs[i].vlen : 0;
        if (j->include_remote_key) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            /* Empty cell — local field carries the value (matches JSON's null). */
        }
        for (int k = 0; k < j->proj_count; k++) {
            if (pos < bufsz - 1) buf[pos++] = csv_delim;
            if (!rraw || !j->proj_tfs[k]) continue;
            const TypedField *rtf = j->proj_tfs[k];
            /* rraw (RecordRef.val) is safe (zero-filled) for records that
               fit read_record_ref's inline_buf, but a tight,
               non-zero-initialized malloc for larger records (see this
               plan's root cause 4, point 4). */
            const uint8_t *rfp = ((size_t)rtf->offset + (size_t)rtf->size > rlen)
                ? g_zero_field_65537
                : rraw + rtf->offset;
            int n = typed_field_to_buf_raw(rtf, rfp, tmp, sizeof(tmp));
            if (n > 0) pos += csv_cell_to_buf(tmp, csv_delim, buf + pos, bufsz - pos);
        }
    }
```

#### `query_internal.h` declaration updates

**Anchor:**
```c
int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                      const TypedSchema *driver_ts,
                      char *buf, size_t bufsz);
int lookup_remote(const JoinSpec *j, const char *db_root,
                  const char *local_key, size_t local_len,
                  RecordRef *out_rr);
int buf_driver_values(const uint8_t *driver_raw, FieldSchema *driver_fs,
                      const char **driver_proj, int driver_proj_count,
                      char *buf, size_t bufsz);
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw,
                    char *buf, size_t bufsz);
size_t build_joined_csv_row(const char *key,
                            const uint8_t *driver_raw, FieldSchema *driver_fs,
                            const char **proj_fields, int proj_count,
                            const JoinSpec *joins, int njoins,
                            const uint8_t **jraws,
                            char csv_delim,
                            char *buf, size_t bufsz);
```
Replace with:
(`build_joined_csv_row` also gains the `driver_len` parameter shown in its
own signature+body change above — the full pre/post pair is reproduced
here for the declaration.)
```c
int extract_local_key(const JoinSpec *j, const uint8_t *driver_raw,
                      size_t driver_len,
                      const TypedSchema *driver_ts,
                      char *buf, size_t bufsz);
int lookup_remote(const JoinSpec *j, const char *db_root,
                  const char *local_key, size_t local_len,
                  RecordRef *out_rr);
int buf_driver_values(const uint8_t *driver_raw, size_t driver_len, FieldSchema *driver_fs,
                      const char **driver_proj, int driver_proj_count,
                      char *buf, size_t bufsz);
int buf_join_values(const JoinSpec *j, const uint8_t *remote_raw, size_t remote_len,
                    char *buf, size_t bufsz);
size_t build_joined_csv_row(const char *key,
                            const uint8_t *driver_raw, size_t driver_len, FieldSchema *driver_fs,
                            const char **proj_fields, int proj_count,
                            const JoinSpec *joins, int njoins,
                            const RecordRef *jrefs,
                            char csv_delim,
                            char *buf, size_t bufsz);
```
(The `extract_local_key` line here duplicates fix 2's own declaration
update — both fixes touch the same declaration with the same new
`driver_len` parameter; there is only one declaration to update, not two
separate edits.)

#### Call-site table — every caller that must be updated

| File:Line | Call | Change |
|---|---|---|
| `query_join.c:599-600` (`adv_search_cb`) | `extract_local_key(&sc->joins[i], (const uint8_t *)raw, sc->fs ? sc->fs->ts : NULL, lk, sizeof(lk))` | Insert `(size_t)hdr->value_len` after `raw`: already required by fix 2's own table — this is the same edit, now also covering the `typed_field_to_buf_raw` call inside `extract_local_key`. |
| `query.c:4454-4455` | `extract_local_key(&joins[i], raw, fs ? fs->ts : NULL, lk, sizeof(lk))` | Insert `(size_t)value_len` after `raw` — same as above, already in fix 2's table. |
| `query_join.c:641-644` (`adv_search_cb`) | `buf_driver_values((const uint8_t *)raw, sc->fs, sc->proj_count > 0 ? sc->proj_fields : NULL, sc->proj_count, row + pos, sizeof(row) - pos)` | Insert `(size_t)hdr->value_len` after `raw`. |
| `query_join.c:646-647` (`adv_search_cb`) | `buf_join_values(&sc->joins[i], join_raws[i], row + pos, sizeof(row) - pos)` | Insert `join_refs[i].vlen` after `join_raws[i]` — `join_refs` (the `RecordRef` array) is already in scope here (declared line 591, populated line 604, not yet freed). |
| `query_join.c:630-634` (`adv_search_cb`) | `build_joined_csv_row(key, (const uint8_t *)raw, sc->fs, ..., sc->joins, sc->njoins, join_raws, sc->csv_delim, row, sizeof(row))` | Insert `(size_t)hdr->value_len` after `(const uint8_t *)raw`, **and** replace `join_raws` with `join_refs` (the `RecordRef *` array itself). Same `hdr->value_len` already threaded into the sibling `buf_driver_values` call at `query_join.c:641-644` in this function. |
| `query.c:4482-4486` | `buf_driver_values(raw, fs, proj_count > 0 ? proj_fields : NULL, proj_count, buf + pos, sizeof(buf) - pos)` | Insert `(size_t)value_len` after `raw` (`value_len` already a local, `query.c:4436`). |
| `query.c:4499-4500` | `buf_join_values(&joins[i], jraws[i], buf + pos, sizeof(buf) - pos)` | Insert `jrr[i].vlen` after `jraws[i]` — `jrr` (the `RecordRef` array) is already in scope (declared line 4441, populated line 4459, not yet freed). |
| `query.c:4472-4476` | `build_joined_csv_row(keybuf, raw, fs, ..., joins, njoins, jraws, csv_delim, buf, sizeof(buf))` | Insert `(size_t)value_len` after `raw`, **and** replace `jraws` with `jrr` (the `RecordRef *` array itself). Same `value_len` already threaded into the sibling `buf_driver_values` call at `query.c:4482-4486` in this function. |

No other call sites of `extract_local_key`, `buf_driver_values`,
`buf_join_values`, or `build_joined_csv_row` exist anywhere in the tree
(each was grepped independently; the sites above are the complete set).
`join_raws`/`jraws` (the derived `uint8_t **` pointer arrays) are **not**
removed by this change — they remain in use, unmodified, at the
`buf_join_values` call sites above and at the pre-existing `!jraws[i]` /
`!join_raws` null checks elsewhere in both functions (e.g. `query.c:4490`).

### Edge cases (Fix 4, explicit)

- **`FT_VARCHAR` out-of-range still yields an empty/zero result.** Same
  reasoning as fix 2's equivalent edge case: `g_zero_field_65537[0]` and
  `[1]` (the length prefix) read as `0`, so `typed_field_to_buf_raw`'s
  `FT_VARCHAR` case returns `0` (its documented "empty" signal) — matching
  existing caller handling (`if (n <= 0) return snprintf_bounded(buf,
  bufsz, "null")` in `buf_field_value`; `if (n > 0) pos += ...` in
  `build_joined_csv_row`; `gbuf[i][0] = '\0'` pre-cleared in `agg_scan_cb`).
- **Non-VARCHAR out-of-range fields are real zero-valued results, not
  "missing."** Same reasoning as fix 2: a zero-filled `FT_BOOL` reads as
  `false`, `FT_INT`/`FT_LONG` as `0`, etc. — genuine default values, not a
  sentinel for absence. This fix does not special-case "out of range" to
  skip the group-by bucket or join column; it lets the zero-filled bytes
  flow through the existing type-specific formatting unchanged, exactly as
  fix 2 and fix 3 already do for their respective read paths.
- **`remote_raw`/`rraw` NULL (unmatched LEFT JOIN).** Unchanged —
  `buf_join_values` and `build_joined_csv_row` both already null-check
  `remote_raw`/`rraw` before computing any field pointer (`if (!remote_raw
  || !j->proj_tfs[k])` / `if (!rraw || !j->proj_tfs[k]) continue;`); the
  new `remote_len`/`rlen` guard only applies once a non-NULL pointer is
  already established, and is `0` in the NULL case only because `jrefs[i]`
  is a `calloc`'d (all-zero) struct entry when a join found no match —
  consistent with the pre-existing `!rraw` short-circuit, not a new path.
- **`RecordRef.inline_buf`'s accidental safety net is not relied upon.**
  Even though `read_record_ref`'s `memset` means small records
  (`klen+vlen+1 <= 2048`) already read zero past `vlen` today, this fix's
  guard is **not** conditioned on record size — it applies the same
  `offset+size > len` check regardless, so behavior does not depend on the
  incidental fact that `inline_buf` happens to be pre-zeroed. This avoids
  a latent dependency on `RecordRef`'s internal buffer-selection threshold
  (2048) ever changing.
- **`g_zero_field_65537` is read-only after program start.** Declared
  `const`, defined once in `util.c`'s BSS/rodata, never written after
  static initialization — no synchronization is needed for concurrent
  reads across worker threads (mirrors `zero_field`'s existing
  function-local-static safety argument in fix 2/3, just as a
  translation-unit-level symbol instead).

## Regression test

New file: `src/test/cases/test_bool_literal_update_roundtrip.c`.

Reproduces the bug brief's exact sequence, plus an index-consistency
assertion:

```c
/* src/test/cases/test_bool_literal_update_roundtrip.c
 *
 * Regression test for docs/plans/2026-08-17-bool-literal-merge-bug.md.
 *
 * Root cause 1 (storage.c v2_update_new_from_old): a partial update that
 * writes a field at/after old->vlen never extends *out_vlen, so the write
 * survives in memory but never reaches trim_fn / the persisted record.
 * Reproduced by: insert flag=true (full record) -> update {flag:false}
 * (trims the flag byte off, since false == default) -> update {flag:true}
 * solo (flag's offset is now >= old->vlen; write is silently dropped
 * pre-fix). Steps 1-4 below; the "flag:true survives" assertion at step 4
 * is the leg that fails on unfixed code.
 *
 * Root cause 2 (config.c typed_field_to_index_key / index.c
 * build_index_key_from_record[_into]): index-key builders read the OLD
 * record's field bytes with no bound, but slotcask_get mallocs OLD
 * snapshots to exactly vlen bytes (no padding) -- so any index diff
 * touching a trimmed field's offset is a heap-buffer-overflow READ,
 * deterministically caught by ASan (exact-sized allocation, no slack).
 * This is NOT reliably provable via a plain functional assertion (the
 * out-of-bounds byte value is undefined, not guaranteed-wrong) -- the
 * count/find assertions below (step 5) are functional coverage for "the
 * final index state is correct", but the authoritative proof that root
 * cause 2 is fixed is the ASan run called out in this plan's execution
 * rules, showing the heap-buffer-overflow report disappear once fix 2 is
 * applied. See this plan's "Regression proof" task for the exact
 * revert/reapply sequence and where each fix's proof comes from.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static int test_bool_literal_update_roundtrip_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        TAP_DIAG("# test-bool-literal-update-roundtrip: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_NOT_NULL(tc, "connect");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"o\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"a:varchar:32\",\"b:varchar:32\",\"flag:bool\"],"
        "\"indexes\":[\"flag\"]}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create obj");
    free(resp); resp = NULL;

    /* Step 1: full-value insert with flag=true. Full-value inserts always
       build a fresh total_size buffer (typed_encode), so this leg is
       unaffected by either root cause -- sanity baseline. */
    tc_request(tc,
        "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"a\":\"x\",\"b\":\"y\",\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed insert");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":true", "insert: flag true");
    free(resp); resp = NULL;

    /* Step 2: partial update {flag:false}. false == default, so the
       persisted record trims the flag byte off entirely -- old->vlen
       shrinks below flag's offset. This step's own get already passes on
       unfixed code (absent field decodes to default false), which is
       exactly why the bug survived undetected. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"flag\":false}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update to false: status");
    free(resp); resp = NULL;

    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":false", "update to false: get reflects false");
    free(resp); resp = NULL;

    /* Step 3: partial update {flag:true} solo. flag's offset is now
       >= old->vlen from step 2's trim. Pre-fix: encode_field writes the
       new byte, but *out_vlen never extends past old->vlen, so trim_fn
       never sees it -- the write is silently dropped. */
    tc_request(tc,
        "{\"mode\":\"update\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\","
        "\"value\":{\"flag\":true}}", &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"updated\"", "update to true: status");
    free(resp); resp = NULL;

    /* Step 4: THE regression assertion. Fails on unfixed code (returns
       flag:false -- the dropped write from step 3). */
    tc_request(tc, "{\"mode\":\"get\",\"dir\":\"d1\",\"object\":\"o\",\"key\":\"k1\"}", &resp);
    ASSERT_CONTAINS(resp, "\"flag\":true", "update to true: get reflects true (regression)");
    free(resp); resp = NULL;

    /* Step 5: index-consistency assertion. Functional coverage for "the
       final index state matches the final record state" -- the
       authoritative proof for root cause 2 (a heap-buffer-overflow READ,
       not a deterministic wrong-value bug) is the ASan run in this plan's
       execution rules, not this assertion alone. */
    tc_request(tc,
        "{\"mode\":\"count\",\"dir\":\"d1\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", &resp);
    ASSERT_EQ_STR(resp, "1", "count(flag=true) == 1 after final update");
    free(resp); resp = NULL;

    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"o\","
        "\"criteria\":[{\"field\":\"flag\",\"op\":\"eq\",\"value\":\"true\"}]}", &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "find(flag=true) returns k1");
    free(resp); resp = NULL;

    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-bool-literal-update-roundtrip", test_bool_literal_update_roundtrip_run)
```

Add `src/test/cases/test_bool_literal_update_roundtrip.c` to the explicit
test-source list in `build.sh`; this repository does not glob
`src/test/cases/`. The case self-registers via `TEST_REGISTER`'s static-init
once linked into `shard-db-test`.

### Regression test for fix 3 and fix 4: `test_trim_compact_oob_fields.c`

**Decision, explicitly flagged:** these need a materially different schema
shape from `test_bool_literal_update_roundtrip.c` (wide multi-field
schemas designed to push a field offset past `zero_field`'s bound, vs.
that test's narrow 3-field bool-trim schema) and a different query
surface (field-projected `find` for fix 3; `aggregate` with OR criteria +
GROUP BY for fix 4, vs. `update`/`count`/`find` for fix 1/2) — extending
the existing file would mix two unrelated schema fixtures in one test
function and blur which assertion proves which root cause. **New file:**
`src/test/cases/test_trim_compact_oob_fields.c`, two registered cases.
Per this plan's own note on root cause 2's regression assertion: the
authoritative proof for both fix 3 (a global-buffer-overflow READ on a
`static` array) and fix 4 (a heap-buffer-overflow READ on a `malloc`'d
block) is the ASan run called out in the task order below, **not** the
functional assertions inside these tests — undefined-behavior reads don't
reliably produce a wrong *value*, only a memory-safety report. The
functional assertions here cover "the feature still works correctly",
same division of labor as the existing test's step 5.

**Correction from the pre-fix-3 checkpoint halt (see `PLAN_NOTES.md`,
resolved 2026-08-18):** an earlier draft of fix 3's test used a plain
`find` with no field projection, and a two-`varchar:65535`-field schema
that pushes the vulnerable field's offset to 131074. Both choices turned
out to defeat the checkpoint: (1) a plain `find` with no `"fields"` and
no `"format"` decodes via `typed_decode`, which already uses the safe
fold-before-ternary pattern and never calls `typed_get_field_str` at all
— the vulnerable function is only reached via field projection
(`"fields":[...]`) or `"format":"rows"`; and (2) even once reached, a
~65 KB overrun past the 65537-byte `zero_field` sentinel landed on other
valid, mapped memory in the real ~15 MB binary rather than on unmapped
memory or ASan's (much narrower) global redzone — confirmed by direct
instrumentation of `typed_get_field_str` against a live ASan-instrumented
daemon, and by a standalone repro compiled with identical sanitizer flags
showing a clean ASan `global-buffer-overflow` report for a 3-byte
overrun of the same pattern but a silent `SIGSEGV`-or-nothing result for
a 65537-byte one. The schema and query below are corrected to use a
**single** `varchar:65535` field followed immediately by the target
field, producing a **1-byte** overrun — small enough to land in ASan's
redzone deterministically — and an explicit `"fields"` projection so the
vulnerable code path is actually exercised.

```c
/* src/test/cases/test_trim_compact_oob_fields.c
 *
 * Regression tests for docs/plans/2026-08-17-bool-literal-merge-bug.md,
 * root causes 3 and 4.
 *
 * Root cause 3 (config.c typed_get_field_str): the function's own
 * zero-sentinel branch re-applies "+ f->offset" after already selecting
 * an unshifted zero_field[65537], so a field whose offset exceeds 65537
 * walks off the end of that static array. Reproduced by: a schema with
 * one varchar:65535 field followed immediately by a bool field (so the
 * bool's offset is exactly 65537 -- sizeof(zero_field)), insert a record
 * with the varchar field filled (non-default) and the bool field left at
 * its default (false, trimmed off entirely), then read the bool field
 * back via an explicit "fields" projection (which routes through
 * typed_get_field_str; a plain unprojected "find" decodes via
 * typed_decode instead, which is already safe and never reaches the
 * vulnerable function -- see PLAN_NOTES.md's pre-fix-3 checkpoint
 * writeup). The 1-byte overrun size is deliberate: a wider schema (e.g.
 * two varchar:65535 fields pushing the target offset to 131074) still
 * reaches the same vulnerable branch, but the resulting ~65KB overrun
 * lands on other valid, mapped memory in the real binary instead of
 * ASan's global redzone or an unmapped page, so it produces no ASan
 * report at all despite the bug firing -- confirmed by direct
 * instrumentation and a standalone repro under identical sanitizer
 * flags. A 1-byte overrun lands in the redzone deterministically. This
 * is a global-buffer-overflow READ, not a deterministic wrong-value bug
 * -- see the task order's ASan checkpoint for the authoritative proof.
 *
 * Root cause 4 (query_aggregate.c agg_scan_cb, via typed_field_to_buf_raw
 * called with no bounds check): reproduced by a schema where the
 * group-by field is the last field and defaults away (trimmed off),
 * while an earlier field is large enough to push the trimmed record's
 * length past 2047 bytes -- forcing keyset_emit_agg_cb's
 * malloc(klen+vlen) fallback (no slack past vlen) instead of its small
 * on-stack buffer. That large field is deliberately NOT the indexed
 * field driving the query -- btree_insert()'s BT_MAX_VAL_LEN = 512-byte
 * cap on indexed values (src/db/btree.h) rejects anything larger, which
 * is exactly what the original fixture hit at the pre-fix-4 checkpoint
 * (see PLAN_NOTES.md) before this correction split the fixture into a
 * small indexed field (drives the OR-eq routing) plus a separate large
 * unindexed field (inflates the record past the malloc threshold). An
 * OR-of-indexed-eq criteria set on the small field routes the query
 * through PRIMARY_KEYSET -> keyset_emit_agg / keyset_emit_agg_cb, the
 * path that owns that tight allocation. This is a heap-buffer-overflow
 * READ -- again, ASan is the authoritative proof, per the task order.
 *
 * Third correction at the same checkpoint (see PLAN_NOTES.md): the
 * trimmed group-by field must not be `bool` (or `enum`).
 * idx_should_auto_bitmap() (src/db/config.c) unconditionally promotes a
 * bare bool/enum field to an auto-created bitmap index at create-object,
 * whether or not it's named in "indexes" -- confirmed live via
 * describe-object. A bitmap-indexed group-by field routes through
 * cmd_aggregate_do's separate indexed-group-by (IGB) bitmap fast path
 * (~line 4786), which reads the bitmap value dictionary directly and
 * never calls agg_scan_cb, making the vulnerable code structurally
 * unreachable regardless of criteria shape. Fixed by using a plain
 * `byte` field instead (1 byte, same all-zero-bytes trim rule, not in
 * idx_should_auto_bitmap's bool/enum-only list) -- verified live to
 * reach and trigger the intended heap-buffer-overflow at
 * typed_field_to_buf_raw (query_aggregate.c:978) via agg_scan_cb:2024
 * and keyset_emit_agg_cb:49.
 *
 * Fourth correction, discovered after the first five fix-4 call sites
 * were applied and this checkpoint reverified (see PLAN_NOTES.md): a
 * sixth site in the same function, agg_scan_cb's separate integer-key
 * extraction loop (typed_field_to_raw, query_aggregate.c:1848-1854,
 * called from query_aggregate.c:2058-2059), reads the identical raw
 * buffer with no bounds check of its own -- missed by the original
 * five-site grep because it's a differently-named sibling of
 * typed_field_to_buf_raw, not a second call to it. ctx->use_int_keys is
 * set whenever every group field is integer-class per type_desc.c
 * (FT_BYTE included, width 1), so this fixture's `f2:byte` group field
 * runs both extraction loops on every scanned record -- meaning this one
 * test now exercises *both* agg_scan_cb guards end-to-end, not just the
 * string-buffer one the "Note" below originally described. Confirmed
 * live via ASan: same 2556-byte allocation, same producer
 * (keyset_emit_agg_cb's malloc(klen+vlen)), reported at
 * typed_field_to_raw (query_aggregate.c:1852) via agg_scan_cb:2058.
 * Unlike the string-path overrun, this one did not abort the daemon
 * process under halt_on_error=0 -- the request completed and the
 * functional assertion passed regardless (irrelevant to the bug's
 * reality: with one seeded record there's no second group for a garbage
 * hash-key byte to collide or fail to collide with). Fixed with the same
 * g_zero_field_65537 guard pattern as the string-buffer loop, applied to
 * this second call site -- see "agg_scan_cb integer-key loop body
 * change" under fix 4's call-site table.
 *
 * Note: fix 4 also touches extract_local_key / buf_join_values /
 * buf_driver_values / build_joined_csv_row (the join-path call sites in
 * query_join.c and query.c) -- those share the exact same
 * g_zero_field_65537 guard pattern added to agg_scan_cb here and are
 * covered by the fix's call-site table and code-level review rather than
 * a second, separately-crafted join regression test; this file exercises
 * one concrete call site end-to-end rather than duplicating the same
 * proof four more times.
 *
 * Separately, build_joined_csv_row's *driver*-field cells get a related
 * but mechanically different fix under the same fix-4 signature bump: no
 * new guard is added there (typed_get_field_str's own fix-3 bounds check
 * already handles out-of-range reads correctly) -- the bug was that
 * every caller passed driver_fs->ts->total_size as data_len instead of
 * the driver record's real length, which defeated that existing bounds
 * check by construction (total_size is never less than any field's
 * offset+size). That fix is proven by fix 3's own regression test above
 * plus code-level review of the driver_len threading in the call-site
 * table, not a dedicated test here, for the same reason the other four
 * fix-4 call sites aren't separately tested: it is the same
 * already-verified typed_get_field_str bounds check (fix 3), reached via
 * one more caller now passing the correct length.
 */
#include "test_runner.h"
#include "test_assert.h"
#include "test_client.h"
#include "fixtures.h"
#include <stdlib.h>
#include <string.h>

static char *make_filled_varchar(size_t n, char c) {
    char *s = malloc(n + 1);
    memset(s, c, n);
    s[n] = '\0';
    return s;
}

/* ---- Fix 3: typed_get_field_str global-buffer-overflow ---- */

static int test_trim_compact_oob_field3_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        TAP_DIAG("# test-trim-compact-oob-field3: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_NOT_NULL(tc, "connect");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    char *f1 = make_filled_varchar(65535, 'A');

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;

    /* f1: varchar:65535 -> on-disk size 65537 (2-byte length prefix +
       65535 content). f2 (last field, bool, default false) offset =
       65537 = sizeof(zero_field) exactly, so once f2 is trimmed off,
       f2->offset + f2->size (65538) is exactly 1 byte past
       sizeof(zero_field) (65537). Deliberately minimal: a wider schema
       reaches the same vulnerable branch but its far-larger overrun
       lands on other valid mapped memory in the real binary rather than
       ASan's redzone or an unmapped page, producing no ASan report even
       though the bug fires -- see the comment block above this test and
       PLAN_NOTES.md's pre-fix-3 checkpoint writeup for the confirming
       repro. A 1-byte overrun is small enough to land in the redzone
       deterministically. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"wide\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"f1:varchar:65535\",\"f2:bool\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create wide object");
    free(resp); resp = NULL;

    /* f2 omitted from the insert value -> encoded at its default (false)
       -> trimmed off entirely, since it's the last field and default. */
    size_t vbuf_sz = 65535 + 256;
    char *vbuf = malloc(vbuf_sz);
    snprintf(vbuf, vbuf_sz,
             "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"wide\","
             "\"key\":\"k1\",\"value\":{\"f1\":\"%s\"}}", f1);
    tc_request(tc, vbuf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed wide insert");
    free(resp); resp = NULL;
    free(vbuf);

    /* Explicit field projection on f2 is required to reach
       typed_get_field_str: a plain "find" with no "fields" and no
       "format" decodes the whole record via typed_decode, which already
       uses the safe fold-before-ternary pattern and never calls
       typed_get_field_str at all. This is the call that walks 1 byte
       off zero_field on unfixed code. */
    tc_request(tc,
        "{\"mode\":\"find\",\"dir\":\"d1\",\"object\":\"wide\",\"criteria\":[],"
        "\"fields\":[\"f2\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"k1\"", "find returns the seeded key");
    ASSERT_CONTAINS(resp, "\"f2\":false", "trimmed f2 reads back as default false");
    free(resp); resp = NULL;

    free(f1);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-trim-compact-oob-field3", test_trim_compact_oob_field3_run)

/* ---- Fix 4: typed_field_to_buf_raw heap-buffer-overflow via
   agg_scan_cb / keyset_emit_agg_cb ---- */

static int test_trim_compact_oob_field4_run(void) {
    TestEnv env = {0};
    if (test_env_start(&env) != 0) {
        TAP_DIAG("# test-trim-compact-oob-field4: daemon spawn failed\n");
        return 1;
    }

    TestClientCfg cfg = { .port = env.port, .io_timeout_ms = 30000 };
    TestClient *tc = tc_connect(&cfg);
    if (!tc) {
        ASSERT_NOT_NULL(tc, "connect");
        test_env_stop(&env);
        return 1;
    }

    char *resp = NULL;
    char *f1 = make_filled_varchar(2500, 'C');

    tc_request(tc, "{\"mode\":\"add-dir\",\"dir\":\"d1\"}", &resp);
    free(resp); resp = NULL;

    /* Correction from the pre-fix-4 checkpoint halt (see PLAN_NOTES.md,
       resolved 2026-08-18): the original fixture indexed f1 directly at
       varchar:2500. That collides with an unrelated, pre-existing
       constraint -- btree_insert()'s BT_MAX_VAL_LEN = 512-byte cap on
       indexed key values (src/db/btree.c, src/db/btree.h) -- so the
       insert failed with EINVAL before the aggregate path was ever
       exercised. Fixed by splitting the OR-driving index off of the
       large field: f0 (varchar:50, indexed, well under the 512-byte
       cap) drives the OR-eq plan; f1 (varchar:2500, NOT indexed) is
       kept purely to inflate the record past keyset_emit_agg_cb's
       2048-byte stack threshold. f2 (last field, default zero) stays
       the trimmed group-by field.

       Second correction, same checkpoint (see PLAN_NOTES.md): f2 must
       NOT be `bool`. idx_should_auto_bitmap() (src/db/config.c) always
       auto-promotes a bare (no explicit index-type) bool or enum field
       to a bitmap index at create-object, whether or not it's named in
       "indexes" -- confirmed live via describe-object, which reported
       "f2:bitmap" even though only f0 was requested. With f2 bitmap-
       indexed, group_by:["f2"] routes through cmd_aggregate_do's
       separate "indexed group_by" (IGB) bitmap fast path (~line 4786),
       which reads the bitmap value dictionary directly and never calls
       agg_scan_cb / typed_field_to_buf_raw at all -- so the vulnerable
       code was structurally unreachable for any bool group-by field,
       regardless of criteria shape. (That fast path also independently
       surfaced a real but out-of-scope bug: the trimmed-off default
       f2=false has no bitmap-dict entry at all, so `f2 eq false` matches
       zero records -- verified live, not pursued further here.) Fixed
       by using `f2:byte` instead of `f2:bool`: byte is a plain 1-byte
       type, absent from idx_should_auto_bitmap's bool/enum-only list,
       so it gets no automatic index and the aggregate correctly falls
       through to the generic agg_run_plan -> FP_UNION ->
       keyset_agg_from_or -> keyset_emit_agg -> keyset_emit_agg_cb ->
       agg_scan_cb path. typed_encode_trim_len (src/db/config.c) trims
       purely on all-zero-bytes, type-agnostically, so byte:0 trims
       exactly like bool:false did -- the offset/size arithmetic below
       is unchanged.

       f0 on-disk = 52 (2-byte length prefix + 50 content). f1 on-disk =
       2502. Trimmed record vlen = 52 + 2502 = 2554 (f2 is trimmed off
       entirely). klen ("k1") = 2, so klen + vlen = 2556 -- past
       keyset_emit_agg_cb's `klen + vlen + 1 < 2048` stack-buffer
       threshold, forcing its malloc(klen + vlen) fallback with no slack
       past vlen. f2's offset (52 + 2502 = 2554) + size (1) = 2555 = one
       byte past that exact malloc'd block once f2 is trimmed.

       Verified live (pre-fix binary): this exact fixture produces
       AddressSanitizer: heap-buffer-overflow, READ of size 1, "0 bytes
       after 2556-byte region", with the expected stack --
       typed_field_to_buf_raw (query_aggregate.c:978) <-
       agg_scan_cb (query_aggregate.c:2024) <-
       keyset_emit_agg_cb (query_aggregate.c:49). ASan aborts the daemon
       on the report, which severs the test's connection mid-request --
       so at the pre-fix-4 checkpoint (before Fix 4's code lands) this
       regression test's functional assertion is EXPECTED to fail (resp
       reads back NULL/"(null)"), same as any other pre-fix regression
       test under CORE-PROCESS.md's revert/confirm-fails/reapply/confirm-
       passes protocol. Once Fix 4's bounds-checked
       typed_field_to_buf_raw lands, the daemon no longer aborts and this
       assertion is expected to pass.

       Third correction, same checkpoint (see PLAN_NOTES.md): after the
       first five fix-4 call sites landed and the above was reverified,
       ASan found a sixth, previously-missed site in the same function --
       agg_scan_cb's separate integer-key extraction loop
       (typed_field_to_raw, query_aggregate.c:1848-1854/2058-2059), which
       reads the identical raw buffer unguarded. f2:byte is integer-class
       (type_desc.c, int_width=1), so this fixture's group_by:["f2"]
       already exercises this loop unconditionally alongside the
       string-buffer one -- no fixture change was needed to surface it,
       only the code fix (see "agg_scan_cb integer-key loop body change"
       under Root Cause 4). This site's overrun does not abort the
       daemon under halt_on_error=0 (unlike the string-path one above),
       so pre-this-fix it does NOT fail this test's functional assertion
       -- the assertion can read `"cnt":1` correctly while ASan still
       correctly flags the read. The ASan report remains the authoritative
       signal for this site, same as noted in step 8 of the task order;
       don't rely on the functional assertion alone to prove this second
       guard is in place. */
    tc_request(tc,
        "{\"mode\":\"create-object\",\"dir\":\"d1\",\"object\":\"agg\","
        "\"splits\":8,\"max_key\":64,"
        "\"fields\":[\"f0:varchar:50\",\"f1:varchar:2500\",\"f2:byte\"],"
        "\"indexes\":[\"f0\"]}",
        &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"created\"", "create agg object");
    free(resp); resp = NULL;

    size_t vbuf_sz = 2500 + 256;
    char *vbuf = malloc(vbuf_sz);
    snprintf(vbuf, vbuf_sz,
             "{\"mode\":\"insert\",\"dir\":\"d1\",\"object\":\"agg\","
             "\"key\":\"k1\",\"value\":{\"f0\":\"idxval\",\"f1\":\"%s\"}}", f1);
    tc_request(tc, vbuf, &resp);
    ASSERT_CONTAINS(resp, "\"status\":\"inserted\"", "seed agg insert");
    free(resp); resp = NULL;
    free(vbuf);

    /* OR-of-indexed-eq on f0 -> PRIMARY_KEYSET -> keyset_emit_agg /
       keyset_emit_agg_cb -> agg_scan_cb's group-by extraction over f2,
       the trimmed field. Spec keys are "fn"/"alias" (parse_agg_specs,
       src/db/query_aggregate.c) -- not "op"/"as". */
    size_t qbuf_sz = 2500 + 512;
    char *qbuf = malloc(qbuf_sz);
    snprintf(qbuf, qbuf_sz,
             "{\"mode\":\"aggregate\",\"dir\":\"d1\",\"object\":\"agg\","
             "\"aggregates\":[{\"fn\":\"count\",\"alias\":\"cnt\"}],"
             "\"group_by\":[\"f2\"],"
             "\"criteria\":{\"or\":[{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"idxval\"},"
             "{\"field\":\"f0\",\"op\":\"eq\",\"value\":\"nonexistent\"}]}}");
    tc_request(tc, qbuf, &resp);
    ASSERT_CONTAINS(resp, "\"cnt\":1", "OR/GROUP BY aggregate counts the trimmed record");
    free(resp); resp = NULL;
    free(qbuf);

    free(f1);
    tc_close(tc);
    test_env_stop(&env);
    return t_ctx->failed > 0 ? 1 : 0;
}

TEST_REGISTER("test-trim-compact-oob-field4", test_trim_compact_oob_field4_run)
```

If the exact `aggregate` JSON field names above (`aggregates`, `as`,
`group_by`, `criteria`) don't match `docs/query-protocol/aggregate.md`
once the executor checks it, that's a plan-vs-reality mismatch on a
newly-written test's own wire-format usage, not a source-anchor mismatch
— fix the JSON literal to match the documented protocol and proceed
(this is not the "halt on anchor mismatch" case, since nothing here
anchors into existing source); if instead the *routing* assumption is
wrong (e.g. this shape doesn't actually reach `PRIMARY_KEYSET`), that
*is* a plan-vs-reality mismatch on the root-cause reasoning — stop and
write `PLAN_NOTES.md`.

## Task order

1. **Write the regression test** (file above), against the current
   unfixed `main`. Build and run it in isolation; confirm it **fails** at
   the step-4 assertion (`"update to true: get reflects true (regression)"`)
   with the actual response showing `"flag":false`. Paste the real command
   and output. Do not proceed until this failure is reproduced and matches
   the documented mechanism (root cause 1) — if it fails for a different
   reason (e.g. a connection error, a schema-creation error), that's a
   plan-vs-reality mismatch: stop and write `PLAN_NOTES.md`, do not guess.

2. **Apply fix 1** (`v2_update_new_from_old` in `src/db/storage.c`, per
   "Fix 1" above).

3. **Checkpoint — confirm fix 1's effect.** Rebuild, rerun the regression
   test. Steps 1-4's assertions (including the step-4 regression
   assertion) must now pass. The step-5 count/find assertions **may or may
   not** pass at this point — root cause 2 (the OOB read in the index-key
   builders) is not yet fixed, so any index-diff logic touching the
   now-still-sometimes-trimmed OLD buffer is still undefined behavior; a
   passing count here is not proof of correctness. Do not treat this
   checkpoint as "done" — this is an interim milestone confirming fix 1
   in isolation, not the full fix.

   At this checkpoint, also run the test once under ASan
   (`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then
   `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1"
   ./build/bin/shard-db-test run test-bool-literal-update-roundtrip`) and
   paste the output. Root cause 2 is still present, so expect (and
   document) a heap-buffer-overflow READ report inside
   `typed_field_to_index_key` reached via `apply_index_diff` — this is the
   test-first proof for fix 2, analogous to CORE-PROCESS's revert-and-
   prove requirement but expressed as "still broken before fix 2" rather
   than "revert fix 2 after the fact," since fix 2 hasn't been written
   yet at this point in the task order. (If ASan does *not* report the
   overflow here, that's a mismatch with this plan's root-cause analysis —
   stop and write `PLAN_NOTES.md` rather than proceeding to fix 2 assuming
   it's still needed for an unconfirmed reason.)

4. **Apply fix 2** — the three signature changes
   (`typed_field_to_index_key`, `build_index_key_from_record`,
   `build_index_key_from_record_into`), the `extract_local_key` signature
   change, the `IndexDiffApplyArgs` field additions, and every call site
   in the "Full call-site table" above. Update both `build_index_key_from_record`
   declarations in `types.h` (854 and 1453) and the
   `build_index_key_from_record_into` declaration (1456) plus
   `typed_field_to_index_key`'s declaration (845). Update
   `extract_local_key`'s declaration in `query_internal.h:404`.

5. **Write fix 3's regression test** (`test-trim-compact-oob-field3` in
   the new `test_trim_compact_oob_fields.c`, above), against `main` with
   fixes 1-2 applied but fix 3 not yet applied. Build under ASan
   (`BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`) and run it in isolation:
   `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1"
   ./build/bin/shard-db-test run test-trim-compact-oob-field3`. The
   functional assertions are expected to **pass** (this bug is a
   memory-safety violation on already-zero bytes, not a value
   corruption — see this plan's Fix 3 edge cases), but the ASan run must
   report a **global-buffer-overflow READ** inside `typed_get_field_str`
   (`config.c`). Paste the real command and output. If ASan does not
   report the overflow, that's a plan-vs-reality mismatch on root cause
   3's reasoning — stop and write `PLAN_NOTES.md` rather than proceeding
   to fix 3 assuming it's still needed for an unconfirmed reason.

6. **Apply fix 3** (`typed_get_field_str` in `src/db/config.c`, per
   "Fix 3" above).

7. **Checkpoint — confirm fix 3's effect.** Rebuild under ASan, rerun
   `test-trim-compact-oob-field3` in isolation. Functional assertions
   must still pass, and the global-buffer-overflow report from step 5
   must now be **absent**. Paste the output.

8. **Write fix 4's regression test** (`test-trim-compact-oob-field4` in
   the same new file), against `main` with fixes 1-3 applied but fix 4
   not yet applied. Build under ASan and run it in isolation:
   `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1"
   ./build/bin/shard-db-test run test-trim-compact-oob-field4`. Unlike
   fix 3's static-sentinel overrun, this overrun lands one byte past a
   heap allocation's redzone — verified live that ASan aborts the daemon
   on the report even with `halt_on_error=0:abort_on_error=0`, severing
   the test's connection mid-request. So **expect the functional
   assertion to fail** (`resp` reads back NULL / "(null)") at this
   pre-fix checkpoint — this is the expected pre-fix failure state per
   CORE-PROCESS.md's regression-test protocol, not a fixture problem.
   What must be present is ASan's **heap-buffer-overflow READ** report,
   with `typed_field_to_buf_raw` (`query_aggregate.c`) at the top of the
   stack via `agg_scan_cb` → `keyset_emit_agg_cb`. Paste the output. A
   *missing* ASan report (not a missing functional pass) is the "stop
   and write `PLAN_NOTES.md`" mismatch case here, analogous to step 5.

9. **Apply fix 4** — the shared `g_zero_field_65537` definition
   (`util.c`) and declaration (`types.h`), the `agg_scan_cb` string-buffer
   body change, the `agg_scan_cb` integer-key loop body change (sixth
   site — see "agg_scan_cb integer-key loop body change" above; found
   after this checkpoint was first reverified, so if step 8's ASan run
   predates this addition, rerun it after both `agg_scan_cb` changes are
   in), the `extract_local_key` non-composite-branch guard (combined with
   fix 2's own `extract_local_key` signature change — one edit, not two),
   the `buf_join_values` / `buf_driver_values` signature + body changes,
   the `build_joined_csv_row` signature + body change (`jraws` →
   `jrefs`, **plus** the new `driver_len` parameter and the driver-fields
   loop body change that replaces `driver_fs->ts->total_size` with
   `(int)driver_len` in both `typed_get_field_str` calls), the
   `query_internal.h` declaration updates, and every call site in the
   "Call-site table — every caller that must be updated" above
   (`query_join.c:599-600, 630-634, 641-644, 646-647` and
   `query.c:4454-4455, 4472-4476, 4482-4486, 4499-4500`).

10. **Checkpoint — confirm fix 4's effect.** Rebuild under ASan, rerun
    `test-trim-compact-oob-field4` in isolation. Functional assertions
    must still pass, and the heap-buffer-overflow report from step 8 must
    now be **absent**. A passing functional assertion alone does not
    prove this — the sixth (integer-key-loop) site found after this
    checkpoint's first pass did not crash the daemon or fail the
    assertion even while still reading out of bounds (see the in-test
    comment's "Third correction"), so absence of *any* ASan report is the
    thing to check, not just the functional result. `run` (not `run-all`)
    prints ASan output straight to this shell's stdout/stderr on a crash,
    but a non-crashing overrun's report only lands in the daemon's own
    captured log — if relying on the isolated `shard-db-test run` output
    alone left any doubt, cross-check with a manual daemon run (dedicated
    `db.env`/tmpdir, `ASAN_OPTIONS=...:log_path=<path>`, drive the same
    request over a raw socket, inspect `<log_path>.<pid>` directly) same
    as used to isolate both sites this round. Paste the output (and the
    ASan log path/contents if a manual run was needed).

11. **Full local verification**, per this repo's AGENTS.md standing
   exceptions (this diff touches `apply_index_diff` / index-key building
   reached from commit hooks under kf-shard locks, and object lifetimes
   of `SlotcaskOldRecord` snapshots — the dynamic-safety gate applies).
   This single pass is the final verification for **all four fixes**
   (1-4) together — it is not repeated per-fix; each fix's own
   before/after proof already happened at its own checkpoint step above:

   - `SKIP_TESTS=1 ./build.sh` (plain build) then
     `./build/bin/shard-db-test run-all` — full suite, zero regressions.
   - `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`, then **three consecutive
     fresh runs** of
     `ASAN_OPTIONS="halt_on_error=0:detect_leaks=1:abort_on_error=0:print_stacktrace=1"
     ./build/bin/shard-db-test run-all` — paste all three outputs. The
     heap-buffer-overflow report from step 3's checkpoint run, the
     global-buffer-overflow report from step 5's checkpoint run, and the
     heap-buffer-overflow report from step 8's checkpoint run must all now
     be **absent**.
   - `BUILD_MODE=tsan SKIP_TESTS=1 ./build.sh`, then **three consecutive
     fresh runs** of
     `TSAN_OPTIONS="halt_on_error=0:second_deadlock_stack=1:print_stacktrace=1:suppressions=$(pwd)/.tsan.supp"
     ./build/bin/shard-db-test run-all` — paste all three outputs.
   - Any new finding from either sanitizer gets root-caused and either
     fixed now (if simple) or written up as a new
     `docs/plans/<date>-<slug>.md` and, only if deliberately deferred,
     added to `.tsan.supp` with a named-function suppression and a full
     rationale paragraph. Never a blanket suppression.

12. **Final regression-test confirmation.** With all four fixes applied,
   rerun each regression test in isolation one more time and paste the
   output — every assertion in each must pass:
   - `./build/bin/shard-db-test run test-bool-literal-update-roundtrip`
     (fixes 1-2; including its step 5 count/find assertions).
   - `./build/bin/shard-db-test run test-trim-compact-oob-field3` (fix 3).
   - `./build/bin/shard-db-test run test-trim-compact-oob-field4` (fix 4).

   This is the "reapply the fix and confirm it passes" half of
   CORE-PROCESS's regression-test requirement for all four root causes;
   combined with step 1's failing baseline + step 3's ASan checkpoint
   (fixes 1-2), step 5's ASan checkpoint (fix 3), and step 8's ASan
   checkpoint (fix 4), every root cause in this plan has an explicit
   before/after proof.

## Execution rules

- **Branch:** create a fresh branch off `main` —
  `git fetch origin main && git checkout -b fix/bool-literal-merge-bug origin/main`.
  Do **not** stack this on `perf/single-op-index-sync` (the branch this
  planning session happened on) or any other in-flight branch.
- **Build:** `SKIP_TESTS=1 ./build.sh`. **Test:**
  `./build/bin/shard-db-test run-all` (full suite) or
  `./build/bin/shard-db-test run <name>` (single case, e.g. during
  iteration on the new test before it's registered in a full run).
- **Dynamic-safety gate:** see task 5 above — exact commands are quoted
  verbatim from this repo's `AGENTS.md`. Run locally; do not defer to CI.
  CI (`.github/workflows/sanitizers.yml`, `.github/workflows/tsan.yml`) is
  a backstop, not a substitute.
- **Execution mode:** per this repo's AGENTS.md standing exception, leave
  all work **uncommitted** when execution finishes — the reviewing agent
  and the human review the raw `git diff` before anything is committed.
  Do not run `git add` / `git commit` as part of executing this plan.
- **If a quoted anchor isn't found exactly** in the file at execution
  time, **stop the entire execution run immediately** — do not guess,
  reinterpret, or continue to any further task, even an unrelated one.
  Write `docs/plans/PLAN_NOTES.md` describing exactly which anchor didn't
  match and what the surrounding code looks like instead. Resuming
  requires a human (or the planning model, re-engaged) to read
  `PLAN_NOTES.md` and decide whether it's a stale-anchor problem
  (re-derive and patch this plan) or a wrong-assumption problem (rethink
  the plan) — execution does not resume on its own initiative.
- **If you hit a decision this plan doesn't cover, stop and ask** — do
  not improvise. (Two known candidates that *shouldn't* come up if the
  call-site table above is complete, but are called out in case the
  codebase has drifted since this plan was written: a
  `typed_field_to_index_key` / `build_index_key_from_record[_into]` call
  site not listed in the table above; or a length expression that turns
  out not to actually be in scope at one of the listed call sites.)
