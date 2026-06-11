# shard-db npm: Typed QueryBody + Object Form query()

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `QueryBody` discriminated union to the npm package so callers get IDE autocomplete on query mode and fields, and update `query()` to accept an object directly (no manual `JSON.stringify`).

**Architecture:** Add `QueryBody` as a namespace member in `index.d.ts` (TypeScript `export =` + namespace pattern) and overload `query()` to accept `ShardDb.QueryBody | string`. Update `index.js` to stringify objects before passing to the native binding. Existing string callers are unaffected (backward compatible).

**Tech Stack:** Node.js N-API native addon, CJS module, TypeScript declaration file (`export =` + namespace)

---

## File map

| Action | Path | Purpose |
|---|---|---|
| Modify | `npm/index.d.ts` | Add `QueryBody` discriminated union in `ShardDb` namespace; overload `query()` |
| Modify | `npm/index.js` | Accept object or string in `query()`; stringify objects before native call |
| Modify | `npm/test/basic.js` | Add tests for object form; keep string-form tests as backward-compat check |

---

## Task 1: Update index.js to accept object or string

**Files:**
- Modify: `npm/index.js`

- [ ] **Step 1: Run the existing test as baseline**

```bash
cd npm && bun test/basic.js
```
Expected:
```
# 7 passed, 0 failed
```

- [ ] **Step 2: Add failing test for object form to `npm/test/basic.js`**

After the final `assert(count2 === 0, 'count is 0 after delete')` line but **before** the `} finally {` block, add:

```js
  // 8. Object form — insert (no JSON.stringify needed)
  const ins2 = JSON.parse(db.query({ mode: 'insert', dir: 'test', object: 'items', key: 'k2', value: { title: 'object form' } }))
  assert(!ins2.error, 'object-form insert succeeds')

  // 9. Object form — get
  const get2 = JSON.parse(db.query({ mode: 'get', dir: 'test', object: 'items', key: 'k2' }))
  assert(get2.title === 'object form', 'object-form get returns correct value')

  // 10. update
  const updResp = JSON.parse(db.query({ mode: 'update', dir: 'test', object: 'items', key: 'k2', value: { title: 'updated' } }))
  assert(!updResp.error, 'update succeeds')
  const getUpd = JSON.parse(db.query({ mode: 'get', dir: 'test', object: 'items', key: 'k2' }))
  assert(getUpd.title === 'updated', 'update persisted correct value')

  // 11. exists — present and absent
  const ex1 = JSON.parse(db.query({ mode: 'exists', dir: 'test', object: 'items', key: 'k2' }))
  assert(ex1 === true, 'exists returns true for present key')
  const ex2 = JSON.parse(db.query({ mode: 'exists', dir: 'test', object: 'items', key: 'missing' }))
  assert(ex2 === false, 'exists returns false for absent key')

  // 12. bulk-insert
  const bulkIns = JSON.parse(db.query({ mode: 'bulk-insert', dir: 'test', object: 'items', records: [
    { key: 'k3', value: { title: 'three' } },
    { key: 'k4', value: { title: 'four'  } }
  ]}))
  assert(!bulkIns.error, 'bulk-insert succeeds')

  // 13. find
  const found = JSON.parse(db.query({ mode: 'find', dir: 'test', object: 'items', limit: 10 }))
  assert(Array.isArray(found) && found.length === 3, 'find returns all 3 records')

  // 14. keys
  const keys = JSON.parse(db.query({ mode: 'keys', dir: 'test', object: 'items', limit: 10 }))
  assert(Array.isArray(keys) && keys.length === 3, 'keys returns 3 keys')

  // 15. fetch
  const fetched = JSON.parse(db.query({ mode: 'fetch', dir: 'test', object: 'items', limit: 10 }))
  assert(Array.isArray(fetched) && fetched.length === 3, 'fetch returns 3 records')

  // 16. bulk-delete
  const bulkDel = JSON.parse(db.query({ mode: 'bulk-delete', dir: 'test', object: 'items', keys: ['k3', 'k4'] }))
  assert(!bulkDel.error, 'bulk-delete succeeds')
  const countAfterBulk = JSON.parse(db.query({ mode: 'count', dir: 'test', object: 'items' }))
  assert(countAfterBulk === 1, 'count is 1 after bulk-delete')

  // 17. aggregate — create a second object with a numeric field
  const createScores = JSON.parse(db.query({ mode: 'create-object', dir: 'test', object: 'scores', splits: 8, max_key: 64, fields: ['score:int'] }))
  assert(!createScores.error, 'create-object scores succeeds')
  db.query({ mode: 'bulk-insert', dir: 'test', object: 'scores', records: [
    { key: 's1', value: { score: 10 } },
    { key: 's2', value: { score: 20 } },
    { key: 's3', value: { score: 30 } }
  ]})
  const agg = JSON.parse(db.query({ mode: 'aggregate', dir: 'test', object: 'scores', aggregates: [{ fn: 'sum', field: 'score', alias: 'total' }] }))
  assert(Array.isArray(agg) && agg[0].total === 60, 'aggregate sum returns correct total')

  // 18. list-objects
  const objs = JSON.parse(db.query({ mode: 'list-objects', dir: 'test' }))
  assert(Array.isArray(objs) && objs.includes('items'), 'list-objects includes items')

  // 19. describe-object
  const desc = JSON.parse(db.query({ mode: 'describe-object', dir: 'test', object: 'items' }))
  assert(desc && !desc.error, 'describe-object returns schema')

  // 20. drop-object
  const drop = JSON.parse(db.query({ mode: 'drop-object', dir: 'test', object: 'scores' }))
  assert(!drop.error, 'drop-object succeeds')
  const objsAfterDrop = JSON.parse(db.query({ mode: 'list-objects', dir: 'test' }))
  assert(!objsAfterDrop.includes('scores'), 'dropped object no longer listed')
```

- [ ] **Step 3: Run test — expect failure**

```bash
cd npm && bun test/basic.js
```
Expected: `not ok - object-form insert succeeds` (TypeError: json must be a string)

- [ ] **Step 4: Update `query()` in `npm/index.js`**

Replace:
```js
  query(json) {
    if (typeof json !== 'string')
      throw new TypeError('json must be a string')
    return binding.query(this._handle, json)
  }
```

With:
```js
  query(bodyOrJson) {
    const json = (typeof bodyOrJson === 'object' && bodyOrJson !== null)
      ? JSON.stringify(bodyOrJson)
      : bodyOrJson
    if (typeof json !== 'string')
      throw new TypeError('query() argument must be a QueryBody object or a JSON string')
    return binding.query(this._handle, json)
  }
```

- [ ] **Step 5: Run test — expect all pass**

```bash
cd npm && bun test/basic.js
```
Expected:
```
ok - open returns instance
ok - create-object succeeds
ok - insert succeeds
ok - get returns correct value
ok - count returns 1
ok - delete succeeds
ok - count is 0 after delete
ok - object-form insert succeeds
ok - object-form get returns correct value
ok - update succeeds
ok - update persisted correct value
ok - exists returns true for present key
ok - exists returns false for absent key
ok - bulk-insert succeeds
ok - find returns all 3 records
ok - keys returns 3 keys
ok - fetch returns 3 records
ok - bulk-delete succeeds
ok - count is 1 after bulk-delete
ok - create-object scores succeeds
ok - aggregate sum returns correct total
ok - list-objects includes items
ok - describe-object returns schema
ok - drop-object succeeds
ok - dropped object no longer listed

1..25
# 25 passed, 0 failed
```

- [ ] **Step 6: Commit**

```bash
git add npm/index.js npm/test/basic.js
git commit -m "feat(npm): query() accepts QueryBody object or raw JSON string"
```

---

## Task 2: Add QueryBody discriminated union to index.d.ts

**Files:**
- Modify: `npm/index.d.ts`

- [ ] **Step 1: Replace `npm/index.d.ts` entirely**

```ts
export = ShardDb

declare class ShardDb {
  /** Open a shard-db data directory for in-process use. */
  constructor(dbRoot: string)

  /**
   * Execute a query synchronously.
   * Accepts a typed QueryBody object (recommended — enables autocomplete)
   * or a raw JSON string (backward compatible).
   * Returns the JSON response string. Parse with JSON.parse().
   * Thread-safe: multiple threads may call concurrently on the same instance.
   */
  query(body: ShardDb.QueryBody): string
  query(json: string): string

  /** Close the database and release all resources. */
  close(): void
}

declare namespace ShardDb {
  /** Arbitrary field criteria — keys are schema field names, values are
   *  scalars (exact match) or operator objects e.g. { gt: 100 }. */
  type Criteria = Record<string, unknown>

  interface Aggregate {
    fn: 'sum' | 'avg' | 'min' | 'max' | 'count'
    field: string
    alias?: string
  }

  type QueryBody =
    // ── CRUD ──────────────────────────────────────────────────────────────
    | { mode: 'get'
        dir: string; object: string; key: string
        fields?: string[] }

    | { mode: 'exists'
        dir: string; object: string; key: string }

    | { mode: 'insert'
        dir: string; object: string; key: string
        value: Record<string, unknown> }

    | { mode: 'delete'
        dir: string; object: string; key: string }

    | { mode: 'bulk-insert'
        dir: string; object: string
        records: Array<{ key: string; value: Record<string, unknown> }> }

    | { mode: 'bulk-delete'
        dir: string; object: string
        keys: string[] }

    // ── Query ─────────────────────────────────────────────────────────────
    | { mode: 'find'
        dir: string; object: string
        criteria?: Criteria
        limit?: number
        offset?: number
        order_by?: string
        order_dir?: 'asc' | 'desc'
        /** Resume cursor from a previous paginated response. null = first page. */
        cursor?: Record<string, unknown> | null
        /** Return full match count alongside the page. */
        total?: boolean
        fields?: string[] }

    | { mode: 'count'
        dir: string; object: string
        criteria?: Criteria }

    | { mode: 'aggregate'
        dir: string; object: string
        aggregates: Aggregate[]
        group_by?: string
        criteria?: Criteria
        having?: Criteria }

    | { mode: 'keys'
        dir: string; object: string
        offset?: number; limit?: number }

    | { mode: 'fetch'
        dir: string; object: string
        offset?: number; limit?: number
        fields?: string[] }

    // ── Schema ────────────────────────────────────────────────────────────
    | { mode: 'create-object'
        dir: string; object: string
        splits: number; max_key: number
        fields: string[]
        indexes?: string[] }

    | { mode: 'add-dir'
        dir: string }

    | { mode: 'add-index'
        dir: string; object: string
        fields: string[] }

    | { mode: 'remove-index'
        dir: string; object: string
        fields: string[] }

    // ── Maintenance ───────────────────────────────────────────────────────
    | { mode: 'truncate'
        dir: string; object: string }

    | { mode: 'vacuum'
        dir: string; object: string }

    | { mode: 'stats' }

    // ── Single-record update ──────────────────────────────────────────────
    | { mode: 'update'
        dir: string; object: string; key: string
        value: Record<string, unknown>
        /** CAS guard — only update if current value matches. */
        if?: Record<string, unknown> }

    // ── Object lifecycle ──────────────────────────────────────────────────
    | { mode: 'drop-object'
        dir: string; object: string }

    // ── Catalog ───────────────────────────────────────────────────────────
    | { mode: 'list-objects'
        dir: string }

    | { mode: 'describe-object'
        dir: string; object: string }
}
```

- [ ] **Step 2: Verify the declaration file is valid TypeScript**

Run from the repo root (not `npm/`):

```bash
npx tsc --strict --noEmit --target es2020 --moduleResolution node \
  --esModuleInterop --allowSyntheticDefaultImports \
  --declaration false \
  - <<'EOF'
import ShardDb = require('./npm/index')
const db = new ShardDb('/tmp/test')
const _r1: string = db.query({ mode: 'find', dir: 'hn', object: 'stories', limit: 20 })
const _r2: string = db.query({ mode: 'count', dir: 'hn', object: 'stories' })
const _r3: string = db.query('{"mode":"stats"}')
db.close()
EOF
```
Expected: exits 0, no errors.

- [ ] **Step 3: Commit**

```bash
git add npm/index.d.ts
git commit -m "feat(npm): add QueryBody discriminated union with autocomplete for all query modes"
```

---

## Self-review

### Spec coverage

| Requirement | Covered |
|---|---|
| `query()` accepts plain object (no `JSON.stringify` needed) | Task 1 |
| Existing string-form callers unaffected | Task 1 test step 1 (baseline) + object added without breaking |
| `QueryBody` discriminated union for all common modes | Task 2 |
| IDE narrows fields when `mode` is set | Task 2 (discriminated union) |
| `ShardDb.QueryBody` importable as a type by consumers | Task 2 (namespace export) |
| `Aggregate` and `Criteria` helper types exported | Task 2 |
| `update` (with optional CAS `if`) | Task 2 |
| `drop-object`, `list-objects`, `describe-object` | Task 2 |

### Type consistency

- `Aggregate` is defined once in the namespace and referenced in the `aggregate` union member.
- `Criteria` is `Record<string, unknown>` — used in `find`, `count`, `aggregate`, `having`.
- The `query()` overload ordering matters: TypeScript picks the first matching overload. `QueryBody` is listed before `string` so objects resolve to the typed overload.
