# Planner upgrade — Workstream 3: unified op-capability table + in-fold + issue-C + scale acceptance

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans. Steps use `- [ ]` checkboxes.

**Goal:** Close the *operator-coverage* dimension of the planner. Today each plan path hard-codes its own operator whitelist (the audit found **no shared op-capability table** — that's why coverage gaps surface one operator at a time). WS3: (A) introduce one shared `op_caps` table and migrate the scattered whitelists to it; (B) fix the bitmap `OP_IN` card-est bug (issue-C); (C) extend the table + executor so `OP_IN` can seed a composite (the `in`-fold) via k-way merge; (D) a standing scale-acceptance test on the 25M-`users` object proving broad/selective/in find·count·aggregate stay bounded.

**This is for shard-db the product, not the explorer** — the explorer's hot queries are already handled by WS1/WS2 (and its `type in` was removed). WS3 is engine completeness: it makes operator capabilities a single source of truth so future ops/paths don't re-introduce gaps, and closes the remaining real coverage gaps.

**Across modes:** the migrated gates (`op_eligible_for_intersect`, composite-seed, order-bound, trigram) are consulted by find/count/aggregate planning alike.

**Tech Stack:** C. Build `SKIP_TESTS=1 ./build.sh`; test `./build/bin/shard-db-test`.

---

## Execution rules

- **Branch off `main` AFTER WS2 (`perf/planner-ws2-ordered-path-cost`) merges** — WS3 Part C edits `find_via_composite_prefix`, which WS2 Part A also changed (varchar bound). `git checkout main && git pull && git checkout -b perf/planner-ws3-op-capability`. Confirm WS2's varchar-bound fix is present in `find_via_composite_prefix` before starting Part C; if not, STOP.
- Leave work **uncommitted**; reviewer commits. Locate edits by **quoted anchor text**.
- Build `SKIP_TESTS=1 ./build.sh`; test via `./build/bin/shard-db-test`. **Never report green without the real `# total: N passed, 0 failed`.**
- Parts are independent and **may be committed/reviewed as separate PRs** (A, B, C, D) — do them in order; each builds + tests green before the next.
- If any anchor/symbol/signature differs, **STOP and write `PLAN_NOTES.md`**.

---

## The scattered whitelists (audit, verified)

Each is a hard-coded op set; WS3 Part A routes them through one table:
- `op_eligible_for_intersect(op)` — `OP_EQUAL, LESS, GREATER, LESS_EQ, GREATER_EQ, BETWEEN, IN, STARTS_WITH`.
- `find_covering_composite` — `if (leaves[i]->op != OP_EQUAL && leaves[i]->op != OP_STARTS_WITH) continue;` (composite **prefix** seed).
- `build_exact_composite_key` — `OP_EQUAL` per sub-field (composite **exact** pin).
- `op_prefers_trigram(op)` / `op_allows_trigram_starts(op)` — trigram eligibility.
- `order_walk_bounds` / the composite `order_range` fold — `GREATER_EQ/GREATER/LESS_EQ/LESS/EQUAL/BETWEEN` (order-by range bound).
- (card-est and `btree_dispatch` keep their per-op *logic* — they're not pure whitelists — but Part B fixes the issue-C bug in card-est.)

---

## PART A — the shared op-capability table

**Files:** Modify `src/db/query.c` (and `types.h` if the table is shared beyond query.c — it isn't here, keep it file-local).

- [ ] **A1.** Define the table near the top of the planner-helpers section (above `op_eligible_for_intersect`). One row per `SearchOp`; flags capture the *coarse* eligibility each whitelist encodes (site-specific nuance like trigram length≥3 stays at the site):
```c
/* Single source of truth for which operators can drive which plan capability.
 * Replaces the per-site op whitelists (intersect / composite-seed / composite-
 * exact / order-bound / trigram). Site-specific logic (trigram min length,
 * card-est estimation, btree_dispatch bounds) stays at the site; this table
 * only answers "is op X eligible for capability Y". Add an op or flip a flag
 * here and every path sees it — no more one-gate-at-a-time drift. */
typedef struct {
    unsigned intersect      : 1;  /* can participate in PRIMARY_INTERSECT */
    unsigned composite_seed : 1;  /* can seed a <field>+order_by prefix walk (D1) */
    unsigned composite_exact: 1;  /* can pin a composite sub-field (Phase B) */
    unsigned order_bound    : 1;  /* a leaf on order_by can bound the walk */
    unsigned trigram_prefers: 1;  /* contains/icontains → prefer trigram */
    unsigned trigram_starts : 1;  /* starts_with → trigram fallback ok */
} OpCaps;

static OpCaps op_caps(enum SearchOp op) {
    OpCaps c = {0};
    switch (op) {
        case OP_EQUAL:        c.intersect=1; c.composite_seed=1; c.composite_exact=1; c.order_bound=1; break;
        case OP_STARTS_WITH:  c.intersect=1; c.composite_seed=1; c.trigram_starts=1; break;
        case OP_LESS: case OP_GREATER: case OP_LESS_EQ: case OP_GREATER_EQ: case OP_BETWEEN:
                              c.intersect=1; c.order_bound=1; break;
        case OP_IN:           c.intersect=1; break;   /* composite_seed flipped on in Part C */
        case OP_CONTAINS: case OP_ICONTAINS: c.trigram_prefers=1; break;
        default: break;       /* everything else: no index-drive capability */
    }
    return c;
}
```
> Cross-check this table against the *current* behaviour of each whitelist before migrating — it must reproduce them exactly (e.g. `op_eligible_for_intersect` returns true for exactly {EQ, LESS, GREATER, LESS_EQ, GREATER_EQ, BETWEEN, IN, STARTS_WITH} → those are the `intersect=1` rows). If a whitelist includes an op not flagged here (or vice-versa), reconcile and note it — Part A must be **behavior-preserving**.

- [ ] **A2.** Migrate each whitelist to consult the table (behavior-preserving):
  - `op_eligible_for_intersect(op)` → `return op_caps(op).intersect;`
  - `find_covering_composite`: replace `if (leaves[i]->op != OP_EQUAL && leaves[i]->op != OP_STARTS_WITH) continue;` → `if (!op_caps(leaves[i]->op).composite_seed) continue;`
  - `build_exact_composite_key`: the `OP_EQUAL` per-sub-field check → `op_caps(...).composite_exact` (only EQ has it, so identical).
  - `order_walk_bounds` and the composite `order_range` fold: keep the `switch` that maps each op to lo/hi (that's *logic*, not a whitelist) — but the `default: break` (no bound) is equivalent to `!op_caps(op).order_bound`. Optionally guard the switch with `if (!op_caps(leaf->op).order_bound) continue;` for clarity; no behavior change.
  - `op_prefers_trigram(op)` → `return op_caps(op).trigram_prefers;`; `op_allows_trigram_starts(op)` → `return op_caps(op).trigram_starts;`

- [ ] **A3.** **Behavior-preserving test.** Add `test_op_capability_table.c`: for a representative object (bitmap + btree + trigram + composite fields), assert via `plan_filter_kind_for_test` that the chosen plan kind/order for a matrix of (op × shape) is **unchanged** from a captured baseline. Capture the baseline by running the matrix *before* A2 (on the A1-only build) and pasting the values into the test as expected. This proves the migration changed nothing.
> If introspection can't express a case, fall back to asserting `find`/`count` row counts are identical for that case. The point: A is a refactor, prove zero behavior change.

- [ ] **A4.** `SKIP_TESTS=1 ./build.sh && ./build/bin/shard-db-test run-all` → green.

---

## PART B — issue-C: bitmap `OP_IN` card-est sums all values

**Files:** Modify `src/db/query.c`

The bitmap eq/in card-est encodes only the first value. Anchor:
```c
    if (it == IT_BITMAP && (leaf->op == OP_EQUAL || leaf->op == OP_IN)) {
        uint8_t val[1024]; size_t vlen = 0;
        encode_criterion_value(tf, leaf->value, strlen(leaf->value), val, &vlen);
        if (vlen == 0) { e.estimable = 0; return e; }
        for (int s = 0; s < splits; s++) {
            char bp[1024];
            bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
            BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
            if (!bm) continue;
            e.k += bm_count(bm, val, vlen);
            bm_close(bm);
        }
        e.saturated = 0;
        return e;
    }
```

- [ ] **B1.** Make it loop the IN values (mirror the btree `OP_IN` card-est which already sums `in_values`). For `OP_IN`, iterate `leaf->in_count` / `leaf->in_values[iv]`; for `OP_EQUAL`, the single `leaf->value`. Sum `bm_count` across shards for each value. (Bitmap counts are exact and disjoint per distinct value, so summing is correct.) Replace the block:
```c
    if (it == IT_BITMAP && (leaf->op == OP_EQUAL || leaf->op == OP_IN)) {
        int nv = (leaf->op == OP_IN) ? leaf->in_count : 1;
        for (int iv = 0; iv < nv; iv++) {
            const char *vstr = (leaf->op == OP_IN) ? leaf->in_values[iv] : leaf->value;
            uint8_t val[1024]; size_t vlen = 0;
            encode_criterion_value(tf, vstr, strlen(vstr), val, &vlen);
            if (vlen == 0) continue;
            for (int s = 0; s < splits; s++) {
                char bp[1024];
                bm_build_path(bp, sizeof(bp), db_root, object, leaf->field, s);
                BitmapShard *bm = bm_open(bp, 0, 0, 0, 0, 0 /* reader */);
                if (!bm) continue;
                e.k += bm_count(bm, val, vlen);
                bm_close(bm);
            }
        }
        e.saturated = 0;               /* exact count — never saturated */
        return e;
    }
```
> Confirm `leaf->in_count` / `leaf->in_values` are the right field names (grep the btree `OP_IN` card-est branch, which already uses them). Match exactly.

- [ ] **B2.** Test in `test_cardinality_estimate.c` (or a new case): build a bitmap field with values a(100 rows), b(50), c(10); assert the estimate for `field in (a,b,c)` ≈ 160, not 100. Use a `card_est`/`leaf_selective_for_test`-style hook if one exists for IN; if not, assert indirectly via a plan choice that depends on the correct estimate. Build + run.

- [ ] **B3.** `run-all` green.

---

## PART C — the `in`-fold: `OP_IN` seeds a composite (k-way merge)

**Files:** Modify `src/db/query.c`. **Depends on WS2 Part A** (the varchar composite bound) being merged.

Today `OP_IN` can't seed a composite (`composite_seed=0`), so `type in (a,b,c) ORDER BY t` falls to walk/sort even when a `type+t` composite exists. The fix: fold each in-value into its own composite-prefix sub-walk and **k-way merge** them by `order_by` so the merged stream is globally ordered.

- [ ] **C1.** Flip the capability: in `op_caps`, set `OP_IN` `composite_seed=1`.
> This alone would route `type in (...)` to `find_via_composite_prefix`, which currently assumes a *single* seed prefix — so C2 must teach the executor to handle multiple prefixes before C1 is safe. Implement C2 first, then flip C1.

- [ ] **C2.** Generalize the composite executor for a multi-value seed. In `find_via_composite_prefix` (post-WS2, varchar-bound-correct), when `seed->op == OP_IN`: for each `in_values[iv]`, compute that value's `[lo,hi]` composite bounds (reuse the now-correct single-value bounding, incl. the WS2 varchar fix and any `order_range` fold), open a `BtRangeIter`-style ordered cursor per value, and **k-way merge** them by the encoded `order_by` value in the requested direction — emitting through the same `composite_prefix_cb` (post-filter + offset/limit + early-exit). Stop when `limit` filled. Cost ≈ `(offset+limit)·log(n_values)`, not a full scan.
> shard-db already has a k-way streaming merge for cursor pagination (`btree_idx_walk_ordered` does an inter-shard k-way merge). Reuse that machinery: each in-value's bounded sub-range is one more stream into the same merge. If a clean reuse isn't available, factor a small merge over `n_values` bounded iterators. Quote the approach in `PLAN_NOTES.md`; this is the one genuinely complex piece.

- [ ] **C3. Test** `test_composite_in_fold.c`: object with `tag:varchar`, `t:long`, `tag+t` composite; rows for tag in {a,b,c,d} interleaved in `t`. Query `tag in (a,c) ORDER BY t DESC limit N`:
  - **Correctness:** returns exactly the a∪c rows, in strict `t` DESC order (the merge is the thing to get right — assert the emitted `t` sequence is monotonic and no b/d rows appear).
  - **Bounded:** via the `g_order_walk_scanned` counter, scans ≈ the a+c partitions + merge overhead, not the whole index.
  - Plan introspection: `plan_filter_kind_for_test` returns `"composite"` for the `tag in (...)`+order shape.

- [ ] **C4.** `run-all` green; verify existing composite/intersect tests unaffected.

---

## PART D — 25M-`users` scale acceptance

Suite tests prove correctness on small data; this is a **reviewer-run** acceptance script on the deployed prod daemon proving the planner stays bounded at real scale across modes. (If `users` lacks suitable indexed fields, use whichever 25M-class object does, or add a throwaway index for the run.)

- [ ] **D1.** For `find`, `count`, and `aggregate` on the 25M object, time each of: a **broad** filter (e.g. a bitmap eq matching most rows) ordered; a **selective** filter ordered; a **`type in (...)`**-style multi-value ordered. Record times.
- [ ] **D2.** Assert each is bounded (find/aggregate ≲ low-hundreds of ms warm for selective; broad ordered uses the walk and stays ≲ ~100ms; the `in` case uses the C2 fold, not a scan). Confirm result counts match a ground-truth `count`.
- [ ] **D3.** Capture the numbers in `docs/` (a short bench note) so this becomes the standing "planner at scale" baseline to re-run before releases.

---

## Self-review

1. **Part A is behavior-preserving** — the captured baseline matrix is identical pre/post migration; `op_caps` reproduces every old whitelist exactly.
2. **Issue-C** — bitmap `OP_IN` estimate sums all values (test proves ≈160 not 100).
3. **In-fold** — `tag in (a,c) ORDER BY t` returns a∪c in strict order, bounded scan, plan = composite; no b/d leakage.
4. No regressions: `test_d1_composite_executor`, `test_composite_typed`, `test_and_intersection`, `test_bm_intersect_count`, `test_planner_*` all green.
5. `run-all` total pasted; D-numbers recorded.

---

## Roadmap context

WS3 closes the **operator-coverage** dimension; WS1 (broad materialization guard) and WS2 (limit-aware sort/walk + varchar composite + cursor fetch+sort) closed the **cost/decision** dimension. Together the three workstreams cover the full planner surface the three audits mapped. Remaining items are not planner-internal: the **bulk-delete ↔ concurrent bulk-insert deadlock** and the **criteria-delete perf** (full-scan match + per-record index drops) — separate storage-layer backlog, plus the **bench gaps** (criteria-delete, delete-with-indexes).
