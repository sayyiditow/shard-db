# Plan: fix C1 cursor pagination losing entries at score-tie boundaries

**Date:** 2026-06-10
**Branch:** `feat/c1-cursor-hash-tiebreak`
**File:** `src/db/query.c`

## Problem

The C1 fetch+sort cursor path sorts candidates by `sort_key` (the order_by field
encoded as memcmp-sortable bytes). For equal `sort_key` values (ties in score) the
comparator returns 0 — no tiebreaker. qsort places equal-key entries in an
arbitrary but deterministic order.

`cursor_find_cb` uses `memcmp(hash16, cursor_hash16, 16)` as the tiebreaker: in
DESC mode it **emits** entries with `hash16 < cursor_hash16` and **skips** entries
with `hash16 >= cursor_hash16` (plus the cursor entry itself).

When a page boundary falls inside a run of equal-score entries, the next page
cursor_find_cb will:
- **Skip** entries that appear _after_ the cursor in the sorted list but have
  `hash16 > cursor_hash16` — permanently lost.
- **Re-emit** entries that appear _before_ the cursor in the sorted list but have
  `hash16 < cursor_hash16` — duplicates.

Net effect: multi-page searches that have many results at the same score level
(e.g. `icontains "copyright"` where many results share score=1) silently truncate
pagination well before all matches are served. The total count (computed from the
full candidate KeySet) is correct; only the cursor walk is short.

Example observation: `icontains "copyright"` returns `total:281` but cursor
reaches only 199 items (page 8 of the expected 12), because ~82 entries with
`score=1` have `hash16 > cursor_hash16` and are dropped.

The C2 btree-walk path does **not** have this bug: `btree_idx_walk_ordered` visits
entries in `(value, hash16)` order natively, so cursor_find_cb's tiebreaker is
always consistent with the walk order.

## Root cause

```c
/* src/db/query.c — small_prefilter_cmp_asc */
return 0;   /* ← no hash tiebreaker; comment falsely claims it's fine */
```

The comment says "we don't depend on tie-break stability since hash16 break-down
is the btree's responsibility". That is true for the C2 walk but **not** for C1.

## Fix

Add `memcmp(ra->hash, rb->hash, 16)` as the final tiebreaker, matching
cursor_find_cb exactly.

## Execution rules

- Branch off `main`.
- Tasks in order; build after each task with `SKIP_TESTS=1 ./build.sh`.
- Test with `./build/bin/shard-db-test run-all`.
- Locate every edit by the **quoted anchor text** below; if not found exactly,
  stop and write `PLAN_NOTES.md`.
- Never claim a step passed without the real build/test output.

---

## Task 1 — Add hash16 tiebreaker to `small_prefilter_cmp_asc`

**Anchor (full function body):**
```
static int small_prefilter_cmp_asc(const void *a, const void *b) {
    const SmallPrefilterRow *ra = a, *rb = b;
    size_t mlen = ra->sort_key_len < rb->sort_key_len
                  ? ra->sort_key_len : rb->sort_key_len;
    int c = memcmp(ra->sort_key, rb->sort_key, mlen);
    if (c != 0) return c;
    if (ra->sort_key_len < rb->sort_key_len) return -1;
    if (ra->sort_key_len > rb->sort_key_len) return 1;
    return 0;
}
```

Replace with:

```c
static int small_prefilter_cmp_asc(const void *a, const void *b) {
    const SmallPrefilterRow *ra = a, *rb = b;
    size_t mlen = ra->sort_key_len < rb->sort_key_len
                  ? ra->sort_key_len : rb->sort_key_len;
    int c = memcmp(ra->sort_key, rb->sort_key, mlen);
    if (c != 0) return c;
    if (ra->sort_key_len < rb->sort_key_len) return -1;
    if (ra->sort_key_len > rb->sort_key_len) return 1;
    /* hash16 tiebreaker: matches cursor_find_cb's memcmp(hash16, cursor_hash16)
       skip logic so equal-sort-key entries are visited in a consistent order
       across C1 pages.  Without this, entries with hash16 > cursor_hash16 that
       appear after the cursor entry in the sorted list are permanently lost on
       subsequent pages. */
    return memcmp(ra->hash, rb->hash, 16);
}
```

Also update the comment above the function to remove the false claim:

**Anchor:**
```
 * don't depend on tie-break stability since hash16 break-down is the
 * btree's responsibility, not ours.
```

Replace with:

```
 * hash16 tiebreaker matches cursor_find_cb's skip logic so page boundaries
 * inside equal-sort-key runs are handled consistently.
```

### 1b. Build

```bash
SKIP_TESTS=1 ./build.sh
```

Must succeed with zero errors/warnings.

---

## Task 2 — Build and test

```bash
SKIP_TESTS=1 ./build.sh
./build/bin/shard-db-test run-all
```

Expected: `# total: N passed, 0 failed`.

Verify manually with the copyright query (requires the running HN server):

```bash
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"copyright"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```

Expected: `"total":281`. Advance through all pages until cursor is null.
Expected: last page emits the final items (total pages = ceil(281/25) = 12), not
stopping at page 8 like before.

Also verify hawking still works:

```bash
./shard-db query '{"mode":"find","dir":"hn","object":"stories",
  "criteria":[{"field":"dead","op":"eq","value":"false"},
              {"field":"deleted","op":"eq","value":"false"},
              {"field":"title","op":"icontains","value":"hawking"}],
  "order_by":"score","order":"desc","limit":25,"cursor":null,"total":"true"}'
```

Expected: `"total":21`, `"cursor":null` (all fit in one page).

---

## Invariants and edge cases

| Case | Expected |
|---|---|
| Single page (all results < limit) | Unaffected — no cursor tiebreak needed |
| Multi-page, all unique scores | Unaffected — tiebreaker never fires |
| Multi-page, many equal scores at page boundary | Fixed — sorted list now matches cursor_find_cb ordering |
| ASC pagination at score tie | Fixed — `small_prefilter_cmp_asc` hash tiebreaker ascending matches cursor_find_cb's ASC skip logic (`hcmp <= 0 → skip`) |
| DESC pagination at score tie | Fixed — `small_prefilter_cmp_desc = -cmp_asc` → descending hash order matches cursor_find_cb's DESC skip logic (`hcmp >= 0 → skip`) |
| C2 btree-walk path | Unaffected — `btree_idx_walk_ordered` already visits in (value, hash16) order |
| Existing test suite | No test currently covers multi-page C1 with tied sort keys; existing tests must still pass |
