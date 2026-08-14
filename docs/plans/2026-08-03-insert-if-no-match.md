# Plan: conditional insert when no existing record matches

## Goal

Support an atomic collection-level insert guard for UUID-keyed objects where a
business field, such as `domain`, identifies the logical company.

Proposed request shape:

```json
{
  "mode": "insert",
  "dir": "default",
  "object": "companies",
  "key": "new-uuid",
  "value": {"domain": "acme.example", "name": "Acme"},
  "if_no_match": [
    {"field": "domain", "op": "eq", "value": "acme.example"}
  ]
}
```

`if_no_match` means: insert only when zero live records in the object satisfy
the criteria. A matching record returns `condition_not_met`; the insert does
not occur. The criteria use the same grammar/operators as `find` and are
evaluated with the normal planner, so an indexed equality uses the index and a
non-indexed condition falls back to a scan.

This is intentionally a new field rather than overloading the existing `if`:
current insert `if` checks the record at the supplied key, while this condition
spans the entire object.

## Atomicity decision

A standalone `find` followed by `insert` is not sufficient: two UUID inserts can
both observe no matching domain and then both commit. The check and insert must
share an exclusive object-level scope.

The first implementation should classify an insert carrying `if_no_match` as
an exclusive object operation. It will take `objlock_wrlock` before running the
planner check and retain that lock through the insert commit. This is a
correctness-first implementation: a non-indexed scan can block other traffic
for its duration, so documentation should strongly recommend an index on the
guard field (`domain`). No new on-disk format or index type is required.

## Tasks

1. Add a red integration test at the TCP JSON seam, named
   `test-insert-if-no-match`:
   - create a UUID-keyed `companies` object with `domain` but no domain index;
   - insert the first company with `if_no_match: domain == acme.example`;
   - verify a second UUID insert with the same domain returns
     `condition_not_met` and leaves one record;
   - add an index on `domain`, repeat with another domain, and verify the same
     result through the indexed planner path;
   - run two concurrent inserts with different UUIDs and the same domain and
     verify exactly one succeeds.
   The concurrent assertion must be deterministic enough to catch a
   check-then-insert implementation; if needed, use the repository's existing
   test-only pause/marker mechanism at the boundary between the guard check and
   insert commit.

2. Extend JSON insert dispatch at the quoted anchor
   `char *if_cond = json_obj_strdup_raw(&req, "if");` to parse
   `if_no_match` separately. Allow it for auto-generated keys because the guard
   is object-scoped, unlike the existing key-scoped `if` predicate. Reject
   malformed criteria before taking the exclusive lock.

3. Extend the lock classification at the quoted anchor
   `int took_wrlock = mode_is_schema(mode);` so an insert with `if_no_match`
   takes `objlock_wrlock`; all other normal inserts retain the existing
   `objlock_rdlock` path. Ensure every early return releases the selected lock.

4. Add an internal query seam named `cmd_exists_tree` beside the quoted anchor
   `int cmd_count_tree(const char *db_root, const char *object, CriteriaNode *tree);`.
   It must return whether at least one live record matches, use the existing
   `plan_filter`/index dispatch, stop after the first match, and avoid emitting
   normal `find` output. It must propagate timeout/invalid-query errors so a
   failed guard never falls through to insertion.

5. In the insert branch, after parsing `if_no_match` and while the exclusive
   object lock is held, call `cmd_exists_tree`. If a match exists, emit
   `{"error":"condition_not_met"}` with the matched key/current value when
   available; otherwise call the existing `cmd_insert` path unchanged. The
   existing per-key `if` and `if_not_exists` semantics remain unchanged.

6. Add protocol and operational documentation:
   - `docs/query-protocol/cas.md` — define `if_no_match`, its whole-object
     scope, response, and atomicity;
   - `docs/query-protocol/overview.md` — list the new conditional-write form;
   - `docs/concepts/indexes.md` or the relevant operations guide — recommend an
     index for high-volume guarded fields and explain the non-indexed scan cost.

7. Decide separately whether to extend this to `bulk-insert`. Do not silently
   reuse the field for bulk requests: batch semantics must define duplicate
   guard values inside one request, per-record versus all-or-nothing behavior,
   response accounting, and the throughput cost of holding the object lock.

## Invariants

- No two successful guarded inserts may leave two live records matching the
  same guard criteria when their operations target the same object.
- A guard match must not overwrite or modify the existing record.
- A timeout, parse error, or planner failure must not insert the new record.
- Existing key-scoped `if` and `if_not_exists` behavior remains compatible.
- The feature is conditional per request; it does not make `domain` globally
  unique for callers that omit `if_no_match`. A future schema-level unique
  constraint would be a separate feature.

## Verification

Test first, then build with `SKIP_TESTS=1 ./build.sh`. Run the focused case,
the existing CAS/insert/index cases, and finally the full suite with
`./build/bin/shard-db-test run-all`. Leave all work uncommitted and unpushed.
