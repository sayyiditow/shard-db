# Bug brief: partial updates silently drop bare JSON boolean literals (unassigned)

Found: 2026-08-17, while executing `docs/plans/2026-08-17-single-op-index-sync.md`
(see PLAN_NOTES.md history in that session; evidence below is reproducible with
the test object described).

## Symptom

`update` with a partial value containing a bare boolean literal —
`{"mode":"update",...,"value":{"flag":true}}` — reports success but the value
is **not persisted**. A subsequent `get` returns the previous/default value.
Both `true` and `false` literals are dropped by the merge; `false` is masked
because an absent bool field decodes to default `false`, so only
`false → true` visibly fails. Once the field's byte has been trimmed from the
compact record, **a bool field can never be set back to `true` via partial
update**. Full-value inserts are unaffected.

## Reproducer (observed, 2026-08-17 on main)

Object: `fields:["a:varchar:32","b:varchar:32","t:varchar:64","flag:bool"]`,
`indexes:["a","b","t:trigram","flag"]` (bool auto-bitmap).

1. Insert with `"flag":true` → stored 135-byte record, flag byte `0x01`. ✓
2. `update {"flag":false}` → merge output at hook time is a 134-byte record
   with the flag byte **absent**; `get` returns `flag:false` (absent→default).
3. `update {"flag":true}` (solo, nothing else) → `get` **still returns
   `flag:false`**. Value lost; no error.

## Two distinct root-cause sites to investigate

1. **Literal drop:** the partial-update merge (cmd_update_v2 →
   `v2_update_new_from_old` in src/db/storage.c; JSON literal parsing for
   non-string scalars) drops bare `true`/`false` tokens instead of encoding
   them. String literals in the same position merge correctly (a/b/t fields
   all round-trip).
2. **Stale-byte index diff:** for a compact record whose trailing bool byte
   is trimmed/absent, `build_index_key_from_record` (src/db/config.c /
   storage.c index-key path) returns a key built from bytes **past the end of
   the record** — observed returning `0x01` (leftover from the original
   insert record in the slot region) for an absent field. Consequences: the
   index diff computed a phantom "true→false" change for the `false` update,
   and masked the real change as "unchanged" in multi-field updates — i.e.
   **secondary index state can diverge from record state** on this path.

## Regression test requirement (when fixing)

Insert bool=true → update `{"flag":false}` → get asserts false → update
`{"flag":true}` → get asserts **true** (this is the leg that fails today),
plus an index-consistency assertion (e.g. count/find on `flag == true`
returns the record after step 3). No existing test covers update-with-bool-
literal + read-back; that gap is why this survived.

## Severity notes

- Silent data loss on a documented operation (update mode, typed bool field).
- Possible index/record divergence via the stale-byte key build (site 2),
  which can also affect bitmap correctness for trimmed trailing fields.
- Scoped to the single-record update path; bulk-update JSON merge and insert
  encoding were not yet audited for the same literal parsing — the fix plan
  should check both.
