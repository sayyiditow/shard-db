# Fix: `match_typed`/`criteria_match_tree` read past trimmed record length

Follow-up to the round-8 diagnostic probe (CI run
https://github.com/sayyiditow/shard-db/actions/runs/33634646691, PR #330,
closed unmerged after posting evidence), which established the root cause
below with byte-exact proof. This plan fixes it.

## Root cause

`typed_encode_trim_len` (`src/db/config.c:3134-3149`) shortens a record's
stored length when its trailing fields are all-zero (e.g. a `numeric` value
of exactly `0`). The stored/on-disk length (`data_len` below) can then be
smaller than the schema's fixed `total_size`.

`match_typed` (`src/db/query_plan.c:894`) has no `data_len` parameter and
never learns this. Its fixed-width cases (`FT_NUMERIC`, `FT_LONG`, `FT_INT`,
every other `FT_*` arm) do an unconditional read at `rec + f->offset`
regardless of how many bytes are actually valid. When the field's byte
range extends past `data_len`, the read lands on whatever bytes happen to
occupy that space in the append-only segment file next — unwritten space
(reads zero, masks the bug) or another record's real bytes that landed
adjacent (garbage, observed on macOS CI; mechanism is platform-independent,
only exposure via write-ordering nondeterminism differs — see round-8's
Evidence section for the full explanation already given to the user).

**A second, independent bug in the same function**, found auditing this
fix's blast radius: `match_typed`'s composite-field branch
(`query_plan.c:896`) calls `decode_field((const char *)rec, 0, cc->raw->field, fs)`
with a **hardcoded literal `0`** for `raw_len`. That flows into
`typed_get_field_str(ts, data, data_len=0, idx)`
(`src/db/config.c:3159-3175`), whose bounds check is
`f->offset + f->size > data_len` — with `data_len=0` this is true for every
real field, so it substitutes `zero_field` unconditionally.
**Composite-field (`field1+field2`) criteria matching through `match_typed`
currently always compares against an empty value, regardless of the
record's real data.** Not caused by trim, not platform-specific, pre-existing.
User-approved to fix in this same plan (same function, same new
parameter fixes both).

**Existing proven pattern this fix reuses** — `typed_get_field_str`
(`config.c:3159-3175`) already does exactly this bounds check against a
`data_len` parameter, substituting a shared zero buffer:

```c
static const uint8_t zero_field[65537];
const uint8_t *src = (data_len >= 0 &&
                        (size_t)f->offset + (size_t)f->size > (size_t)data_len)
    ? zero_field
    : data + f->offset;
```

A shared instance of that buffer already exists and is used the same way
elsewhere in the codebase: `extern const uint8_t g_zero_field_65537[65537];`
(`src/db/types.h:810`, defined `src/db/util.c:712`), already referenced by
`query_aggregate.c:2083/2117` and `query_join.c:220/383/407/423/512`.
`query_plan.c` already `#include "types.h"` (line 1), so no new symbol is
needed.

## Fix design

Add a `size_t data_len` parameter to `match_typed` and `criteria_match_tree`
(inserted right after the `rec` pointer, mirroring
`typed_get_field_str(ts, data, data_len, field_idx)`'s ordering). Thread it
through every call site — in every site audited below, the caller already
has the record's real length in scope (a `vlen`/`value_len` parameter, a
`RecordRef.vlen`, a `SlotHeader.value_len`, a `SlotcaskOldRecord.vlen`, or a
`SlotcaskBulkRec.old_vlen`); it is currently in scope but not passed
through, so the ripple is mechanical (thread an existing value), not new
plumbing.

Inside `match_typed`, apply the guard at three points — one shared
`g_zero_field_65537` substitution per pointer, matching the existing
pattern exactly:

1. **Composite/unknown-field branch**: pass `data_len` instead of the
   literal `0` into `decode_field`. `decode_field` already forwards
   `raw_len` straight into `typed_get_field_str`, which already has the
   correct bounds-check logic (`config.c:3159-3175`) — this one-word change
   fixes the composite-matching bug outright.
2. **Field-vs-field branch** (`OP_EQ_FIELD` etc.): both `rec + cc->tf->offset`
   and `rec + cc->rhs_tf->offset` need independent bounds checks before
   `cmp_typed_field_pair` — the two fields can have different offsets, so
   one can be in-bounds while the other isn't.
3. **Fixed-width switch**: a single substitution of `p` right after
   `p = rec + f->offset` covers every `case` uniformly (all of them read
   through `p`, confirmed by inspection of every arm including `FT_ENUM`
   at `query_plan.c:1180-1215` and `FT_VARCHAR`'s `match_typed_varchar`,
   which already treats a length-prefix of `0,0` as an empty value —
   `zero_field`'s first two bytes are `0,0`, so this is safe) — no
   per-case changes needed.

### Signature changes

Anchor (`src/db/query_plan.c:894`, current):
```c
int match_typed(const uint8_t *rec, const CompiledCriterion *cc, FieldSchema *fs) {
```
New:
```c
int match_typed(const uint8_t *rec, size_t data_len, const CompiledCriterion *cc, FieldSchema *fs) {
```

Anchor (`src/db/query_plan.c:896`, current):
```c
    if (cc->composite || !cc->tf) {
        char *attr = decode_field((const char *)rec, 0, cc->raw->field, fs);
```
New:
```c
    if (cc->composite || !cc->tf) {
        char *attr = decode_field((const char *)rec, data_len, cc->raw->field, fs);
```

Anchor (`src/db/query_plan.c:903-910`, current):
```c
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        if (!cc->rhs_tf) return 0;
        int r = cmp_typed_field_pair(rec + cc->tf->offset,
                                     rec + cc->rhs_tf->offset, cc->tf);
        return field_vs_field_match(r, cc->op);
    }
```
New:
```c
    if (cc->op == OP_EQ_FIELD || cc->op == OP_NEQ_FIELD ||
        cc->op == OP_LT_FIELD || cc->op == OP_GT_FIELD ||
        cc->op == OP_LTE_FIELD || cc->op == OP_GTE_FIELD) {
        if (!cc->rhs_tf) return 0;
        const uint8_t *lhs = ((size_t)cc->tf->offset + (size_t)cc->tf->size > data_len)
            ? g_zero_field_65537 : rec + cc->tf->offset;
        const uint8_t *rhs = ((size_t)cc->rhs_tf->offset + (size_t)cc->rhs_tf->size > data_len)
            ? g_zero_field_65537 : rec + cc->rhs_tf->offset;
        int r = cmp_typed_field_pair(lhs, rhs, cc->tf);
        return field_vs_field_match(r, cc->op);
    }
```

Anchor (`src/db/query_plan.c:913-915`, current):
```c
    const TypedField *f = cc->tf;
    const uint8_t *p = rec + f->offset;

    switch (f->type) {
```
New:
```c
    const TypedField *f = cc->tf;
    const uint8_t *p = ((size_t)f->offset + (size_t)f->size > data_len)
        ? g_zero_field_65537 : rec + f->offset;

    switch (f->type) {
```

Anchor (`src/db/query_plan.c:1694`, current):
```c
int criteria_match_tree(const uint8_t *rec, const CriteriaNode *n, FieldSchema *fs) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF:
        if (!n->compiled) return 0;
        return match_typed(rec, n->compiled, fs);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++)
            if (!criteria_match_tree(rec, n->children[i], fs)) return 0;
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (criteria_match_tree(rec, n->children[i], fs)) return 1;
        return 0;
    }
    return 0;
}
```
New:
```c
int criteria_match_tree(const uint8_t *rec, size_t data_len, const CriteriaNode *n, FieldSchema *fs) {
    if (!n) return 1;
    switch (n->kind) {
    case CNODE_LEAF:
        if (!n->compiled) return 0;
        return match_typed(rec, data_len, n->compiled, fs);
    case CNODE_AND:
        for (int i = 0; i < n->n_children; i++)
            if (!criteria_match_tree(rec, data_len, n->children[i], fs)) return 0;
        return 1;
    case CNODE_OR:
        for (int i = 0; i < n->n_children; i++)
            if (criteria_match_tree(rec, data_len, n->children[i], fs)) return 1;
        return 0;
    }
    return 0;
}
```

### Header declarations

Anchor (`src/db/types.h:1119`, current):
```c
int  match_typed(const uint8_t *rec, const CompiledCriterion *cc, FieldSchema *fs);
```
New:
```c
int  match_typed(const uint8_t *rec, size_t data_len, const CompiledCriterion *cc, FieldSchema *fs);
```

Anchor (`src/db/types.h:1392`, current):
```c
int criteria_match_tree(const uint8_t *rec, const CriteriaNode *node, FieldSchema *fs);
```
New:
```c
int criteria_match_tree(const uint8_t *rec, size_t data_len, const CriteriaNode *node, FieldSchema *fs);
```

## Repo-wide sweep for the same bug class

User asked, after the composite-field finding above, to check the whole
repo for other instances of the same pattern — an unconditional
`field->offset` read against a typed record buffer with no check against
the record's actual stored length (`vlen`/`data_len`). Method: grepped
every `->offset` dereference against a record pointer across `src/db/*.c`
(`f->offset`, `tf->offset`, `field->offset`, `gtf->offset`, `rtf->offset`)
and classified each as already-guarded (matches the
`(size_t)f->offset + (size_t)f->size > data_len ? g_zero_field_65537 : ...`
pattern, or padded-to-`total_size` by construction) or unguarded, then
read full context for every unguarded hit.

**Already safe** (confirmed, no action needed): `typed_get_field_str`
and `typed_decode_stream`/the JSON-encode helper (`config.c`, the
originally-cited precedent); `typed_field_to_index_key` and every one of
its 13 call sites across `config.c`/`index.c`/`query_bulk.c`/`query.c`
(all pass a real length — `record_len`, `ts->total_size`, `rec->vlen`,
or `vlen`); every `->offset` read in `query_join.c` (5 sites, all already
guarded with `g_zero_field_65537`); `agg_scan_cb`'s own group-by key
extraction (`query_aggregate.c:2081-2118`, guarded — inconsistent with
the *rest* of the same function, see below); `query_schema.c:136-138`
(edit-field path, explicit `if (of->offset + (size_t)of->size > vlen)
continue; /* defensive */`); `query_find.c:923-926`
(`v2_rebuild_walk_cb`, same defensive-skip pattern); `index.c:3494`
(`mf_append_field`) and `index.c:3701-3712` (segment-scan bitmap
extraction) — both safe by construction, since their one caller
(`reindex_seg_cb`, `index.c:3687-3699`) explicitly pads `value` to
`ts->total_size` with zeros before either is reached (own comment: "Pad
to total_size so field access at tf->offset is always safe").

**Found unguarded — same bug class, same fix pattern, folded into this
plan** (user confirmed: fix everything found, one pass):

1. **`src/db/query_aggregate.c:1605-1607`, `wfc_batch_cb`** — window/best-value
   aggregate helper. `typed_field_to_double(bc->agg_tf, (const uint8_t *)value
   + bc->agg_tf->offset, &v)`, unguarded. `vlen` is a callback parameter,
   currently `(void)vlen;`-discarded.
2. **`src/db/query_aggregate.c:2151` (count-on-varchar) and `:2162`
   (SUM/AVG/MIN/MAX accumulation), `agg_scan_cb`** — **highest severity of
   the sweep**: this is the primary aggregate accumulation path, hit by
   every `aggregate` query with a `sum`/`avg`/`min`/`max`/`count(field)`
   spec. `hdr->value_len` is already in scope and already used two dozen
   lines above (2081-2118) for the group-by extraction in the *same
   function* — the accumulation loop below it was simply never given the
   same treatment. Any aggregate over a field that can trim to zero (a
   `numeric`/`int`/`long`/etc. value of exactly 0, mirroring round-8's
   exact scenario) silently corrupts the aggregate result whenever a
   trimmed record lands adjacent to another record's bytes — broader
   blast radius than the original BETWEEN bug, since it hits every
   aggregate query, not just BETWEEN-across-zero.
3. **`src/db/query_aggregate.c:3237`, `vs_lookup_cb`** — vacuum-staged
   aggregate re-verification lookup. `typed_field_to_double(tf, rec +
   tf->offset, &v)`, unguarded. `vlen` is a callback parameter, currently
   `(void)vlen;`-discarded.
4. **`src/db/index.c:1972`, `bm_rebuild_cb`** — bitmap-index rebuild
   during `add-index`/reindex, walking live kf records directly (not the
   padded segment-scan path). `const uint8_t *vbase = (const uint8_t *)value
   + f->offset;`, unguarded. `vlen` is a callback parameter, currently
   `(void)vlen;`-discarded. Corrupts the rebuilt bitmap index for any
   trimmed record — wrong records included/excluded from bitmap-indexed
   lookups on that field.
5. **`src/db/query_maint.c:67`, `tg_estimate_cb`** — trigram-index size
   estimation sampling (used to size the index before `add-index
   foo:trigram`). `const uint8_t *vbase = (const uint8_t *)value +
   f->offset;`, unguarded. `vlen` is a callback parameter, currently
   `(void)vlen;`-discarded. Lower severity — corrupts only a sizing
   *estimate* heuristic, not stored data — but still a genuine
   out-of-bounds-adjacent read of memory the function doesn't own; fixed
   for the same reason as the rest (a stray read past a heap/mmap
   allocation is a real defect regardless of how its output is used).

### Fix hunks for the sweep findings

Anchor (`src/db/query_aggregate.c:1597-1608`, current):
```c
static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    if (!criteria_match_tree(value, bc->tree, bc->fs)) return 0;
    double v;
    if (!typed_field_to_double(bc->agg_tf,
                               (const uint8_t *)value + bc->agg_tf->offset,
                               &v))
        return 0;
```
New:
```c
static int wfc_batch_cb(const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen;
    WfcBatchCtx *bc = (WfcBatchCtx *)ctx;
    if (!criteria_match_tree(value, vlen, bc->tree, bc->fs)) return 0;
    const uint8_t *agg_p = ((size_t)bc->agg_tf->offset + (size_t)bc->agg_tf->size > vlen)
        ? g_zero_field_65537 : (const uint8_t *)value + bc->agg_tf->offset;
    double v;
    if (!typed_field_to_double(bc->agg_tf, agg_p, &v))
        return 0;
```

Anchor (`src/db/query_aggregate.c`, inside `agg_scan_cb`, the
count-on-varchar branch, current):
```c
            if (ctx->specs[i].field[0] && ctx->spec_tfs[i] &&
                ctx->spec_tfs[i]->type == FT_VARCHAR) {
                int elen = varchar_eff_len(raw + ctx->spec_tfs[i]->offset,
                                           ctx->spec_tfs[i]->size);
                if (elen <= 0) continue;
            }
            a->count++;
            continue;
        }

        double v;
        int present = 0;
        if (ctx->spec_tfs[i]) {
            present = typed_field_to_double(ctx->spec_tfs[i],
                                            raw + ctx->spec_tfs[i]->offset, &v);
        } else {
```
New:
```c
            if (ctx->specs[i].field[0] && ctx->spec_tfs[i] &&
                ctx->spec_tfs[i]->type == FT_VARCHAR) {
                const TypedField *cvtf = ctx->spec_tfs[i];
                const uint8_t *cvp = ((size_t)cvtf->offset + (size_t)cvtf->size >
                                      (size_t)hdr->value_len)
                    ? g_zero_field_65537 : raw + cvtf->offset;
                int elen = varchar_eff_len(cvp, cvtf->size);
                if (elen <= 0) continue;
            }
            a->count++;
            continue;
        }

        double v;
        int present = 0;
        if (ctx->spec_tfs[i]) {
            const TypedField *stf = ctx->spec_tfs[i];
            const uint8_t *sp = ((size_t)stf->offset + (size_t)stf->size >
                                 (size_t)hdr->value_len)
                ? g_zero_field_65537 : raw + stf->offset;
            present = typed_field_to_double(stf, sp, &v);
        } else {
```

Anchor (`src/db/query_aggregate.c:3220-3238`, current):
```c
static int vs_lookup_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *raw) {
    (void)key; (void)klen; (void)vlen;
    VSLookupCtx *c = (VSLookupCtx *)raw;
    int p = vs_pair_find(c, hash16);
    if (p < 0) return 0;
    pthread_mutex_lock(&c->mu);
    VSStaged *cur = &c->staged[c->pairs[p].slot];
    const uint8_t *rec = (const uint8_t *)value;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT) continue;
        const TypedField *tf = c->spec_tfs[i];
        if (!tf) continue;
        double v;
        if (!typed_field_to_double(tf, rec + tf->offset, &v)) continue;
```
New:
```c
static int vs_lookup_cb(const uint8_t hash16[16],
                        const void *key, size_t klen,
                        const void *value, size_t vlen,
                        void *raw) {
    (void)key; (void)klen;
    VSLookupCtx *c = (VSLookupCtx *)raw;
    int p = vs_pair_find(c, hash16);
    if (p < 0) return 0;
    pthread_mutex_lock(&c->mu);
    VSStaged *cur = &c->staged[c->pairs[p].slot];
    const uint8_t *rec = (const uint8_t *)value;
    for (int i = 0; i < c->nspecs; i++) {
        enum AggFn fn = c->specs[i].fn;
        if (fn == AGG_COUNT) continue;
        const TypedField *tf = c->spec_tfs[i];
        if (!tf) continue;
        const uint8_t *fp = ((size_t)tf->offset + (size_t)tf->size > vlen)
            ? g_zero_field_65537 : rec + tf->offset;
        double v;
        if (!typed_field_to_double(tf, fp, &v)) continue;
```

Anchor (`src/db/index.c:1963-1972`, current):
```c
static int bm_rebuild_cb(uint32_t slot, const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen; (void)vlen;
    BmRebuildCtx *c = (BmRebuildCtx *)ctx;
    if (c->field_index < 0) return 0;
    const TypedField *f = &c->ts->fields[c->field_index];
    /* Pull the raw field bytes out of the typed-record value. */
    const uint8_t *vbase = (const uint8_t *)value + f->offset;
```
New:
```c
static int bm_rebuild_cb(uint32_t slot, const uint8_t hash16[16],
                         const void *key, size_t klen,
                         const void *value, size_t vlen,
                         void *ctx) {
    (void)hash16; (void)key; (void)klen;
    BmRebuildCtx *c = (BmRebuildCtx *)ctx;
    if (c->field_index < 0) return 0;
    const TypedField *f = &c->ts->fields[c->field_index];
    /* Pull the raw field bytes out of the typed-record value. Trim-encoded
       records may be shorter than f->offset+f->size — substitute the
       shared zero buffer rather than reading past vlen (same pattern as
       typed_get_field_str, config.c:3159-3176). */
    const uint8_t *vbase = ((size_t)f->offset + (size_t)f->size > vlen)
        ? g_zero_field_65537 : (const uint8_t *)value + f->offset;
```

Anchor (`src/db/query_maint.c:59-67`, current):
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
```
New:
```c
static int tg_estimate_cb(uint32_t slot, const uint8_t hash16[16],
                          const void *key, size_t klen,
                          const void *value, size_t vlen,
                          void *ctx) {
    (void)slot; (void)hash16; (void)key; (void)klen;
    TgEstimateCtx *c = (TgEstimateCtx *)ctx;
    if (c->sampled >= c->max_sample) return -1;
    const TypedField *f = &c->ts->fields[c->field_index];
    const uint8_t *vbase = ((size_t)f->offset + (size_t)f->size > vlen)
        ? g_zero_field_65537 : (const uint8_t *)value + f->offset;
```

All three files (`query_aggregate.c`, `index.c`, `query_maint.c`) already
`#include "types.h"` (verified), so `g_zero_field_65537` is directly
usable — no new includes needed.

### Regression test for the sweep findings

Extend `bi_num`'s existing setup in `test_binary_index.c` (same object,
same 5 records, same known-adjacent-trim scenario already proven on CI)
with one new assertion exercising the highest-severity finding
(`agg_scan_cb`'s SUM path, #2 above):
```c
    ASSERT_TRUE(fabs(do_sum_amt(tc, "bi_num") - 0.0) < 0.001,
        "numeric sum(amt) across -999.99..999.99 incl. 0 = 0.00");
```
(`do_sum_amt` — new small helper mirroring `do_count`'s shape, issuing
`{"mode":"aggregate","dir":"default","object":"bi_num","aggregates":[{"fn":"sum","field":"amt"}]}`
and parsing the numeric result.) This is expected red on macOS-arm64
alongside the existing BETWEEN assertion, for the same underlying
CI-proven adjacency mechanism, and green everywhere once the sweep fixes
are applied. The remaining 4 findings (`wfc_batch_cb`, `vs_lookup_cb`,
`bm_rebuild_cb`, `tg_estimate_cb`) are fixed by the same one-line pattern
applied at their already-identified anchors but are not independently
regression-tested by name — they share the identical mechanism and fix
shape as the two directly tested paths (`match_typed` and `agg_scan_cb`'s
SUM), and engineering a reliable adjacency trigger for each individually
(window-function best-value, vacuum-staged lookup, bitmap rebuild,
trigram sizing) is materially more setup for marginal additional proof of
a mechanically-identical fix. Flagged here explicitly as a deliberate
scope call, not an oversight — say if broader per-site coverage is wanted
instead.

## Call-site enumeration (CORE-PROCESS.md requirement — every consumer, before proposing the signature change)

All 26 call sites below were read in full context and confirmed to already
have the record's real stored length in scope. Grouped by file, with the
enclosing function name as the textual anchor (function names are unique
per file; the target line is the only `criteria_match_tree`/`match_typed`
call inside that function, so it disambiguates even where two functions
share an identical one-line call text).

### `src/db/query.c` (16 sites)

| Function | Current call | Length source | New call |
|---|---|---|---|
| `count_batch_cb` (~915) | `criteria_match_tree((const uint8_t *)value, c->sc->tree, c->sc->fs)` | `vlen` (used, not discarded) | `criteria_match_tree((const uint8_t *)value, vlen, c->sc->tree, c->sc->fs)` |
| stream-find callback (~1611) | `criteria_match_tree(value, sc->tree, sc->fs)` | `vlen` (currently `(void)vlen;`— drop that cast, it becomes used) | `criteria_match_tree(value, vlen, sc->tree, sc->fs)` |
| D1 prefix record cb (~2059) | `criteria_match_tree(raw, c->tree, c->fs)` | `rr.vlen` → local `value_len` already computed | `criteria_match_tree(raw, value_len, c->tree, c->fs)` |
| `composite_prefix_record_cb` (~2133) | `criteria_match_tree(value, c->tree, c->fs)` | `vlen` (currently `(void)vlen;` — drop) | `criteria_match_tree(value, vlen, c->tree, c->fs)` |
| D2-style prefix record cb (~2680) | `criteria_match_tree(raw, c->tree, c->fs)` | `rr.vlen` → local `value_len` already computed | `criteria_match_tree(raw, value_len, c->tree, c->fs)` |
| dispatched-fetch inner loop (~4812) | `criteria_match_tree(raw, tree, fs)` | `fvlens[fi]` → local `value_len` already computed | `criteria_match_tree(raw, value_len, tree, fs)` |
| `or_count_batch_cb` (~5064) | `criteria_match_tree((const uint8_t *)value, c->tree, c->fs)` | `vlen` (used, not discarded) | `criteria_match_tree((const uint8_t *)value, vlen, c->tree, c->fs)` |
| OR fallback sequential loop (~5102) | `criteria_match_tree(rr.val, tree, fs)` | `rr.vlen` | `criteria_match_tree(rr.val, rr.vlen, tree, fs)` |
| ordered-cursor scan cb (~5713) | `criteria_match_tree(raw, oc->tree, oc->fs)` | `hdr->value_len` (`SlotHeader *hdr` in scope) | `criteria_match_tree(raw, hdr->value_len, oc->tree, oc->fs)` |
| btree-ordered-walk record cb (~6011) | `criteria_match_tree(raw, c->remaining, c->fs)` | `rr.vlen` → local `value_len` already computed | `criteria_match_tree(raw, value_len, c->remaining, c->fs)` |
| `D2BatchCtx` batch cb (~6216) | `criteria_match_tree((const uint8_t *)value, ctx->tree, ctx->fs)` | `vlen` param in scope | `criteria_match_tree((const uint8_t *)value, vlen, ctx->tree, ctx->fs)` |
| `cursor_fetch_cb` (~6290) | `criteria_match_tree((const uint8_t *)value, ctx->tree, ctx->fs)` | `vlen` param in scope | `criteria_match_tree((const uint8_t *)value, vlen, ctx->tree, ctx->fs)` |
| `fetch_sort_batch_cb` (~6517) | `criteria_match_tree((const uint8_t *)value, c->tree, c->fs)` | `vlen` param in scope | `criteria_match_tree((const uint8_t *)value, vlen, c->tree, c->fs)` |
| `bulk_criteria_indexed_cb` (~6845) | `criteria_match_tree(value, bc->tree, bc->fs)` | `vlen` (currently `(void)vlen;` — drop) | `criteria_match_tree(value, vlen, bc->tree, bc->fs)` |
| join pre-check loop A (~7452) | `criteria_match_tree((const uint8_t *)rr.val, tree, &driver_fs)` | `rr.vlen` | `criteria_match_tree((const uint8_t *)rr.val, rr.vlen, tree, &driver_fs)` |
| join pre-check loop B (~8035) | `criteria_match_tree((const uint8_t *)rr.val, tree, &driver_fs)` | `rr.vlen` | `criteria_match_tree((const uint8_t *)rr.val, rr.vlen, tree, &driver_fs)` |

### `src/db/query_bulk.c` (4 sites)

| Function | Current call | Length source | New call |
|---|---|---|---|
| `bulk_criteria_scan_cb` (~2935) | `criteria_match_tree(raw, bc->tree, bc->fs)` | `hdr->value_len` (`SlotHeader *hdr` param) | `criteria_match_tree(raw, hdr->value_len, bc->tree, bc->fs)` |
| `v2_bulk_upd_value_compute` (~3053) | `criteria_match_tree(old->value, w->tree, w->fs)` | `old->vlen` (`SlotcaskOldRecord *old`) | `criteria_match_tree(old->value, old->vlen, w->tree, w->fs)` |
| `v2_bulk_del_crit_pre_commit_bulk` (~5146) | `criteria_match_tree(old->value, w->tree, w->fs)` | `old->vlen` | `criteria_match_tree(old->value, old->vlen, w->tree, w->fs)` |
| `v2_bulk_del_crit_prepare_window` (~5166) | `criteria_match_tree(r->old_value, w->tree, w->fs)` | `r->old_vlen` (`SlotcaskBulkRec *r`) | `criteria_match_tree(r->old_value, r->old_vlen, w->tree, w->fs)` |

### `src/db/query_aggregate.c` (2 sites)

| Function | Current call | Length source | New call |
|---|---|---|---|
| `wfc_batch_cb` (~1603) | `criteria_match_tree(value, bc->tree, bc->fs)` | `vlen` (currently `(void)vlen;` — drop) | `criteria_match_tree(value, vlen, bc->tree, bc->fs)` |
| `agg_scan_cb` (~2068) | `criteria_match_tree(raw, ctx->tree, ctx->fs)` | `hdr->value_len` (`SlotHeader *hdr` param) | `criteria_match_tree(raw, hdr->value_len, ctx->tree, ctx->fs)` |

### `src/db/query_join.c` (1 site, both branches)

| Function | Current call | Length source | New call |
|---|---|---|---|
| `adv_search_cb` (~605-606) | `sc->fast_cc ? match_typed((const uint8_t *)raw, sc->fast_cc, sc->fs) : criteria_match_tree((const uint8_t *)raw, sc->tree, sc->fs)` | `hdr->value_len` — `SlotHeader` stores `key_len` and `value_len` as independent fields (not a combined length), so `raw` (already past the key) pairs directly with `hdr->value_len` unmodified, no arithmetic. Verify against `SlotHeader`'s doc comment at `types.h:213-221` during implementation. | `sc->fast_cc ? match_typed((const uint8_t *)raw, hdr->value_len, sc->fast_cc, sc->fs) : criteria_match_tree((const uint8_t *)raw, hdr->value_len, sc->tree, sc->fs)` |

### `src/db/query_find.c` (1 function, both branches)

| Function | Current call | Length source | New call |
|---|---|---|---|
| `kf_live_match_cb` (~170, ~173) | `match_typed(value, mc->single_cc, mc->fs)` / `criteria_match_tree(value, mc->tree, mc->fs)` | `vlen` (currently `(void)vlen;` — drop) | `match_typed(value, vlen, mc->single_cc, mc->fs)` / `criteria_match_tree(value, vlen, mc->tree, mc->fs)` |

### `src/db/query_plan.c` (definition's own recursion — not a "caller" in the audit sense, but part of the signature-change task)

Covered by the signature-change hunks above (`criteria_match_tree`'s own
`CNODE_LEAF`/`CNODE_AND`/`CNODE_OR` bodies, and the two internal
`match_typed`/`criteria_match_tree` calls inside it).

**Total: 26 external call sites + 1 recursive definition. Every one confirmed
to have `data_len` already available in scope before this change — no new
plumbing required anywhere up any call stack.**

## `SlotHeader.value_len` verification (needed before touching `query_join.c`/`query_bulk.c`/`query_aggregate.c`'s `SlotHeader`-based sites)

`src/db/types.h:213-221`:
```c
/* Zone A entry: 24 bytes. Payload (key+value) lives in Zone B at a fixed offset. */
typedef struct __attribute__((packed)) {
    uint8_t  hash[16];
    uint16_t flag;        /* 0=empty, 1=active, 2=deleted */
    uint16_t key_len;
    uint32_t value_len;
} SlotHeader;
```
`value_len` is the value-region byte count, independent of `key_len` (the
struct stores both as separate fields, not one combined length) — so
`hdr->value_len` is the correct `data_len` to pass at every `SlotHeader`-based
call site, no arithmetic needed. Task 1's first step re-confirms this by
grep-reading every producer of a `SlotHeader` before editing any call site,
per the "verify, don't assume" standard the round-8 work already held itself to.

## Invariants / edge cases

- `data_len` is the **value**-region length only (matches `vlen`/`rr.vlen`/
  `hdr->value_len`'s existing meaning throughout the codebase) — never
  includes the key-region bytes. `rec`/`raw`/`value` at every call site
  already points past the key (`block + hdr->key_len`, `rr.val`,
  `old->value`), consistent with what `f->offset` is computed relative to.
- `g_zero_field_65537` is `65537` bytes — the largest possible `f->size`
  (varchar max content 65535 + 2-byte length prefix = 65537) fits exactly;
  no field type can overrun it.
- `f->offset + f->size > data_len` (not `f->offset >= data_len`) is the
  exact condition `typed_get_field_str` already uses and the comment there
  explains why: correct for both field-boundary trim and any hypothetical
  future byte-level trim. Reuse verbatim, do not re-derive.
- Untyped/legacy (v1, non-`FieldSchema->ts`) records never reach
  `match_typed`'s fixed-width switch (guarded by `cc->composite || !cc->tf`
  routing to `decode_field` instead) — `data_len` only gates the typed
  fast path, matching `typed_get_field_str`'s own scope.
- A `data_len` of 0 is not a special case requiring extra handling: every
  call site passes an already-validated `vlen`/`value_len` read from a
  real on-disk record, and the parameter is `size_t` throughout the audit
  above (matching `vlen`'s own type at every site) — so there is no path
  where a negative value could reach this parameter and silently wrap to
  a huge `size_t`, which would otherwise defeat the `> data_len` guard.

## Dynamic-safety tooling gate assessment

Per `AGENTS.md`'s standing exception, ASan+UBSan+TSan (3× each) is required
for diffs touching "locks, shared/cached state (kfcache, segcache, bitmap
cache, btree cache), object lifetimes, or background threads." This diff
touches none of those: `match_typed`/`criteria_match_tree` read only
already-fetched, per-call stack-local/caller-owned buffers (`rec`, `cc`,
`fs` — all passed by the caller, no locking or shared-cache access inside
either function), and `g_zero_field_65537` is a read-only, statically
zero-initialized BSS symbol already read concurrently by
`query_aggregate.c`/`query_join.c` today without any lock. **The gate does
not apply** — still run the existing local build+test cycle (below) since
that's the ordinary Definition-of-Done bar, but the 3×ASan/3×TSan gate is
not additionally required for this diff. State this explicitly rather than
silently skipping it, per the plan-quality bar.

## Regression test

`src/test/cases/test_binary_index.c:110-112` **already contains** the
exact regression assertion:
```c
    ASSERT_EQ_INT(do_count(tc, "bi_num",
        "[{\"field\":\"amt\",\"op\":\"between\",\"value\":\"-1\",\"value2\":\"1\"}]"),
                  3, "numeric between -1 and 1 = 3");
```
against the `bi_num` object built at lines 96-107 (single `amt:numeric:10,2`
field, 5 sequential single-connection inserts including a `0` value at
`n_2`). This is the pre-existing test `.github/workflows/ci.yml` already
carries in `SHARD_TEST_EXCLUDE` with the comment
`"test-binary-index: pre-existing macOS-arm64-only failure (numeric BETWEEN
across zero returns a wrong count; numeric lt/eq pass)"` — i.e. this test
**is the regression test**; no new test file is needed.

**Verified locally just now** (this dev machine is Linux x86_64):
`./build/bin/shard-db-test run test-binary-index` → all 22 assertions pass,
including assertion 12 ("numeric between -1 and 1 = 3"), confirming what
round-8 already established: the bug does not reproduce on Linux, only on
macOS-arm64. **Local pass/fail verification of this fix therefore cannot
happen on this machine** — the fail→fix→pass cycle CORE-PROCESS.md asks
for must run on CI's macOS-arm64 leg, exactly as round-7/8 already did for
diagnosis. Task 1 below is structured around that: push the branch with
`test-binary-index` re-included in the PR gate (temporarily, for this
verification push only) and the fix **not yet applied**, confirm the macOS
leg goes red on assertion 12 for the expected reason (not a timeout, not
an unrelated failure); then apply the fix, push again, confirm the macOS
leg goes green and Linux/Linux-arm64 remain green (they already pass, so
this also proves no regression on the platforms where it currently masks).

Additionally, worth confirming as an incidental proof-point (not required,
but strengthens the fix's evidence for the composite-field bug found
above): if a cheap targeted assertion for composite-field criteria
matching already exists anywhere in the suite, note whether it currently
passes despite the bug (likely — a broken "always-zero" comparison can
still coincidentally pass an `eq` against a genuinely-empty/zero record,
or the composite-criteria path may simply have no test coverage today).
This is a documentation step only — do not add new test coverage for it
beyond what's needed to prove the fix; scope stays on what round-8 + this
audit already established.

## Task 1 — TDD-first fix

Branch off `main`: `fix/match-typed-trim-boundary-overread`.

1. **Confirm current red, on CI, for the right reason.** On the new
   branch, temporarily remove `test-binary-index` from
   `SHARD_TEST_EXCLUDE` in `.github/workflows/ci.yml` (quoted anchor: the
   `"test-auto-reshard-throttle,...,test-binary-index"` string at
   `ci.yml` — drop the trailing `,test-binary-index"` back to the closing
   quote, and drop the five comment lines above it, `ci.yml:29-33`
   (the `# test-binary-index: ...` block, including its
   `docs/plans/2026-08-28-macos-arm64-numeric-between.md` link), since
   they're about to become stale). Do **not** touch
   `src/db/query_plan.c`/`types.h`/any call site yet.
   **Per CORE-PROCESS.md's git-safety rules: commit, push, and opening the
   draft PR are git write operations — do not perform them without the
   human's explicit go-ahead for this specific push, even though this is
   scratch verification mirroring round-7/8's pattern.** Once authorized,
   commit, push, open a **draft** PR (not for merge as-is). Confirm the
   macOS-arm64 leg's `test-binary-index` run shows assertion 12
   ("numeric between -1 and 1 = 3") failing with `count()=2` — the same
   failure round-8 already characterized — and that Linux x86_64 /
   Linux arm64 legs still pass all 22 assertions (matches round-8's
   findings; if either Linux leg unexpectedly fails, STOP — something
   about this run differs from round-8's evidence and needs
   investigation before proceeding, do not paper over it).
2. **Apply the fix.** Make every edit in "Fix design" and the call-site
   table above, file by file:
   - `src/db/types.h`: the two signature-declaration hunks.
   - `src/db/query_plan.c`: the five hunks in "Fix design" (signature,
     composite branch, field-vs-field branch, fixed-width switch,
     `criteria_match_tree`'s own body).
   - `src/db/query.c`: all 16 call sites per the table (including
     dropping the now-stale `(void)vlen;` casts at the 3 sites in this
     file marked "currently `(void)vlen;` — drop": the stream-find
     callback ~1611, `composite_prefix_record_cb` ~2133, and
     `bulk_criteria_indexed_cb` ~6845. The other two `(void)vlen;`-drop
     sites from the call-site table, `wfc_batch_cb` and
     `kf_live_match_cb`, are covered by the `query_aggregate.c` and
     `query_find.c` bullets below, not this one).
   - `src/db/query_bulk.c`: all 4 call sites.
   - `src/db/query_aggregate.c`: both 2 `criteria_match_tree` call sites
     (including dropping `wfc_batch_cb`'s `(void)vlen;`) **plus** the 3
     sweep-finding hunks in the same file (`wfc_batch_cb`'s
     `typed_field_to_double` guard, `agg_scan_cb`'s count-on-varchar and
     SUM/AVG/MIN/MAX guards, `vs_lookup_cb`'s guard) per "Fix hunks for
     the sweep findings" above.
   - `src/db/query_join.c`: the 1 site (both the `match_typed` and
     `criteria_match_tree` branches of the ternary), after confirming the
     `SlotHeader.value_len` semantics per the verification step above.
   - `src/db/query_find.c`: the 1 function's 2 call sites (dropping its
     `(void)vlen;`).
   - `src/db/index.c`: the `bm_rebuild_cb` sweep-finding hunk.
   - `src/db/query_maint.c`: the `tg_estimate_cb` sweep-finding hunk.
   - `src/test/cases/test_binary_index.c`: add the `do_sum_amt` helper and
     the `sum(amt)` regression assertion from "Regression test for the
     sweep findings" above.
   Build: `SKIP_TESTS=1 ./build.sh` — must compile clean, no new warnings
   (every dropped `(void)vlen;` must correspond to `vlen` becoming
   genuinely used at that exact call site, or the build will warn/fail
   under `-Wunused-parameter` if the project enables it; if a site turns
   out not to need `vlen` after all, leave its cast in place rather than
   dropping it blind).
   Run full local suite: `./build/bin/shard-db-test run-all` — must be
   green (this exercises every call site's new parameter across the whole
   suite, not just `test-binary-index`; a threading mistake at any of the
   26 sites — wrong variable, off-by-one on which struct field — would
   most likely show up here as a new failure elsewhere, since `data_len`
   now actively gates real reads it didn't before for every typed
   fixed-width criterion in every test).
3. **Confirm green, on CI, on all three legs.**
   **Per CORE-PROCESS.md's git-safety rules: pushing this commit is a git
   write operation — do not push without the human's explicit go-ahead
   for this specific push**, distinct from the authorization obtained in
   step 1. Once authorized, push the same branch. Confirm macOS-arm64's
   `test-binary-index` assertion 12 now passes
   (`count()=3`), all 22 of its assertions pass, and Linux x86_64 /
   Linux arm64 remain fully green (proves no regression on the
   platforms that already masked this). Paste the raw CI log lines for
   assertion 12 on all three legs into this plan's own Evidence section
   (append one, mirroring round-8's), not just a summary.
4. **Restore `SHARD_TEST_EXCLUDE`'s comment accuracy.** `test-binary-index`
   stays permanently un-excluded from the PR gate now (the whole reason it
   was excluded is fixed) — this is a real, wanted diff, not a revert of
   step 1. Double check no other currently-excluded test in that list was
   excluded for a reason this fix also touches (quick read of the
   remaining exclusion comments in `ci.yml` — expected: no, they're
   unrelated timing/nesting issues, but verify before assuming).
5. Full local suite one more time post-cleanup:
   `./build/bin/shard-db-test run-all` green.
6. Leave the diff **uncommitted** for review (this repo's standing
   execution mode) — do not commit/push/open a non-draft PR without the
   human's explicit go-ahead for that specific operation, same as every
   prior round in this investigation.

If any quoted anchor above doesn't match exactly at execution time, write
`PLAN_NOTES.md` describing the mismatch and halt — do not guess or
reinterpret. If a decision surfaces mid-execution that this plan doesn't
cover, stop and ask.

## Evidence

### Red — pre-fix (commit `b3ec41a`, CI run 33665962971, PR #331)

macOS-arm64 leg, `test-binary-index` assertion 12 — fails for the expected
reason (`count()=2`, not a timeout, not an unrelated failure); all other
21 assertions pass:

```
ok 11 - numeric lt 0 → 2 negatives
not ok 12 - numeric between -1 and 1 = 3
#   expected 3 got 2
# test-binary-index: 21 passed, 1 failed
```

Linux arm64 leg (same run) — `test-binary-index` fully green, matching
round-8's finding that the bug does not reproduce there:

```
ok 12 - numeric between -1 and 1 = 3
# test-binary-index: 22 passed, 0 failed
```

Linux x86_64 leg (same run) — fully green:

```
# test-binary-index: 22 passed, 0 failed
```

### Green — post-fix (commit `c09b661`, CI run 33670354238, same PR)

macOS-arm64 leg — assertion 12 now passes; the new sweep assertion
(`agg_scan_cb` SUM path) passes as assertion 13; 23/23:

```
ok 12 - numeric between -1 and 1 = 3
ok 13 - numeric sum(amt) across -999.99..999.99 incl. 0 = 0.00
# test-binary-index: 23 passed, 0 failed
```

Linux x86_64 leg (same run):

```
ok 12 - numeric between -1 and 1 = 3
# test-binary-index: 23 passed, 0 failed
```

Linux arm64 leg (same run):

```
ok 12 - numeric between -1 and 1 = 3
# test-binary-index: 23 passed, 0 failed
```

Local (Linux x86_64, this machine): `SKIP_TESTS=1 ./build.sh` clean, no new
warnings; `./build/bin/shard-db-test run-all` → `12816 passed, 0 failed
across 437 cases`.

Supplementary local sanitizer evidence (beyond the waived gate, run to
close the CI ASan question): `BUILD_MODE=asan SKIP_TESTS=1 ./build.sh`
then one full `./build/bin/shard-db-test run-all` → `12816 passed, 0
failed across 437 cases`, exit 0 — zero ASan/UBSan/LSan findings under
the strict build (`-fno-sanitize-recover=undefined`, LSan failing the
exit code). Together with the first CI ASan attempt (20 min of suite,
zero findings before the contention watchdog trip) and the second (30
min, zero findings, `test-rebuild-recovery` itself passing 112/0 before
the 35-minute job timeout), every case in the suite has executed under
ASan with a clean report.

### Run-level infrastructure failures on the fix push (not code regressions)

The fix-push run had three workflow-level failures, each root-caused to
infrastructure, none related to the diff (which touches no startup,
networking, or build machinery):

1. **CI macOS-arm64 leg fail** — its only two case failures
   (`test-small-prefilter-orderby`, `test-cursor-bitmap-intersect`, both
   `0 passed, 1 failed`) were daemon-startup deaths: `bind: Address already
   in use` → `wait_daemon_ready: timeout`. Exact signature of the
   documented port-picker TOCTOU flake
   (`docs/plans/2026-07-21-test-harness-port-toctou-flake.md`: "no
   consistent victim test across runs"); both cases passed 18/0 and 9/0 on
   the same leg pre-fix. `test-binary-index` itself passed 23/23.
   Failed-job rerun requested.
2. **cppcheck job fail** — the step is report-only
   (`continue-on-error: true`); the job actually died on its 20-minute
   job timeout. Pre-fix it "passed" in 14-19s because the analysis-cache
   key (hash of `src/db`/`src/cli`) matched main's; this push touches
   `types.h`, which every TU includes, invalidating the cache and forcing
   a full re-analysis on an already-saturated fleet. The reported
   findings (incl. `query.c:1474`/`6208` uninitvar) are pre-existing
   report-only noise in code this diff did not write. Rerun requested for
   a fair shot at the budget on a quieter runner.
3. **Sanitizers (ASan+UBSan) job fail** — `# WATCHDOG: test
   'test-rebuild-recovery' exceeded 900s — aborting run-all`, exit 124.
   Zero sanitizer findings anywhere in the log (the workflow's
   findings-grep step never triggered). TSan passed on the exact same
   commit and suite; the full suite passes locally. Same-push contention
   (cppcheck's full `-j nproc` re-analysis + coverage + TSan all running
   concurrently) starved the heavy rebuild case past its 900s watchdog.
   Rerun requested.

   **Human disposition (2026-09-03):** all three are job-budget
   exhaustion, not correctness bugs — accepted as-is for this PR. The
   job timeouts for ASan, TSan, and CI will be raised to 1800s in a
   follow-up change by the human; the memory-safety question itself is
   closed by the local ASan+UBSan full-suite pass above plus CI TSan
   green on the same commit.
