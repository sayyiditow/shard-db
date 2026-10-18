# shard-db

**Upgrading:** a populated data directory must carry clean-open evidence at
or after shard-db `2026.09.2` — older roots are refused with an upgrade
hint. In binding versions that means `1.0.22` or later (these embed the
`2026.09.2` engine); if your data was last opened by an older binding,
install `1.0.22`+ and open once before upgrading. A filesystem-empty
directory initializes directly. The first `2026.10.2` open of an existing
root performs a one-time, crash-safe startup migration that reindexes
every indexed object before serving; subsequent opens skip it. Leave the
data directory untouched while a migration open is in progress.

High-performance embedded database for Node.js and Bun. Runs entirely in-process — no daemon, no TCP socket, no external dependencies.

```bash
npm install shard-db
# or
bun add shard-db
```

Prebuilt binaries for **Linux x64**, **Linux arm64**, and **macOS Apple Silicon**. Node.js ≥ 18, Bun ≥ 1.0.

## Quick start

```js
const ShardDb = require('shard-db')

const db = new ShardDb('/path/to/data')

// Create a table
await db.query({
  mode: 'create-object',
  dir: 'myapp',
  object: 'users',
  splits: 16,
  max_key: 128,
  fields: ['name:varchar:100', 'email:varchar:200', 'age:int', 'active:bool'],
  indexes: ['email', 'age']
})

// Insert
await db.query({
  mode: 'insert',
  dir: 'myapp', object: 'users', key: 'u1',
  value: { name: 'Alice', email: 'alice@example.com', age: 30, active: true }
})

// Get
const user = JSON.parse(await db.query({
  mode: 'get', dir: 'myapp', object: 'users', key: 'u1'
}))
// { name: 'Alice', email: 'alice@example.com', age: 30, active: true }

db.close()
```

`query()` returns a `Promise<string>` — queries run on worker threads so the event loop is never blocked.

## CRUD

```js
// Insert / upsert
await db.query({ mode: 'insert', dir: 'myapp', object: 'users', key: 'u1',
  value: { name: 'Alice', email: 'alice@example.com', age: 30, active: true } })

// Update
await db.query({ mode: 'update', dir: 'myapp', object: 'users', key: 'u1',
  value: { age: 31 } })

// Get
const user = JSON.parse(await db.query({
  mode: 'get', dir: 'myapp', object: 'users', key: 'u1'
}))

// Exists
const exists = JSON.parse(await db.query({
  mode: 'exists', dir: 'myapp', object: 'users', key: 'u1'
}))  // true or false

// Delete
await db.query({ mode: 'delete', dir: 'myapp', object: 'users', key: 'u1' })
```

### Conditional writes

Single writes take CAS guards and computed values:

```js
// Atomic increment — only while the balance can cover it
const resp = JSON.parse(await db.query({
  mode: 'update', dir: 'myapp', object: 'accounts', key: 'a1',
  value: { balance: { $incr: -25 } },
  if: [{ field: 'balance', op: 'gte', value: 25 }],
  return_record: true
}))
// resp.record — the committed record with the computed balance applied

// Uniqueness guard — insert only while NO other record has that domain
await db.query({
  mode: 'insert', dir: 'myapp', object: 'domains', key: 'g1',
  value: { domain: 'd.io' },
  if_no_match: [{ field: 'domain', op: 'eq', value: 'd.io' }]
})
```

`$incr` is a checked add (overflow rejects the whole request) and `$copy`
copies a same-type field; guards check *other* records atomically with
the write and fail closed.

## Querying

### Find with criteria

```js
const users = JSON.parse(await db.query({
  mode: 'find',
  dir: 'myapp', object: 'users',
  criteria: { age: { gt: 25 } },
  order_by: 'age', order: 'desc',
  limit: 20
}))
// [ { name: 'Alice', age: 30, ... }, ... ]
```

Common operators: `eq`, `neq`, `lt`, `gt`, `lte`, `gte`, `between`, `in`, `starts`, `contains`, `like`, `exists`, `not_exists`. Criteria also accept nested `{ and: [...] }` / `{ or: [...] }` trees.

Typed fields are nullable since `2026.10.2`: an explicit JSON `null` stores NULL, an omitted insert field with no `:default` reads back as `null`, and value predicates never match NULL — use `exists` / `not_exists` to test nullness.

### Count

```js
const total = JSON.parse(await db.query({
  mode: 'count', dir: 'myapp', object: 'users',
  criteria: { active: { eq: true } }
}))
```

### Aggregate

```js
const stats = JSON.parse(await db.query({
  mode: 'aggregate',
  dir: 'myapp', object: 'users',
  aggregates: [
    { fn: 'count', alias: 'n' },
    { fn: 'avg',   field: 'age', alias: 'avg_age' },
    { fn: 'max',   field: 'age', alias: 'max_age' }
  ],
  group_by: 'active'
}))
// [ { active: true, n: 38, avg_age: 31.4, max_age: 55 }, ... ]
```

### Bulk operations

```js
// Bulk insert (acts as upsert)
await db.query({
  mode: 'bulk-insert', dir: 'myapp', object: 'users',
  records: [
    { key: 'u2', value: { name: 'Bob',   email: 'b@x.com', age: 22, active: true } },
    { key: 'u3', value: { name: 'Carol', email: 'c@x.com', age: 45, active: false } }
  ]
})

// Bulk delete
await db.query({
  mode: 'bulk-delete', dir: 'myapp', object: 'users',
  keys: ['u2', 'u3']
})
```

### Cursor pagination

Efficient keyset pagination — O(limit) per page regardless of depth.

```js
// First page
let page = JSON.parse(await db.query({
  mode: 'find', dir: 'myapp', object: 'users',
  order_by: 'age', order: 'asc',
  limit: 20, cursor: null   // null = first page
}))
// page.rows    — array of records
// page.cursor  — pass to the next call; null when exhausted

// Next page
page = JSON.parse(await db.query({
  mode: 'find', dir: 'myapp', object: 'users',
  order_by: 'age', order: 'asc',
  limit: 20, cursor: page.cursor
}))
```

`order_by` must be an indexed field.

## Log handler

Silent by default. Register a callback to receive errors, warnings, and slow-query events:

```js
db.setLogHandler((type, msg) => {
  // type: 1=error 2=warn 3=info 4=debug 5=audit 6=slow
  if (type <= 2) console.error('[shard-db]', msg.trim())
  if (type === 6) console.warn('[shard-db:slow]', msg.trim())
})

// Unregister
db.setLogHandler(null)
```

`msg` is pre-formatted: `"YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n"`. The handler fires on the JS thread after each query completes.

## TypeScript

Full types included. `QueryBody` is a discriminated union with autocomplete for all query modes:

```ts
import ShardDb = require('shard-db')

const db = new ShardDb('/path/to/data')

db.setLogHandler((type, msg) => {
  if (ShardDb.LOG_TYPES[type] === 'error') console.error(msg.trim())
})

const count = JSON.parse(
  await db.query({ mode: 'count', dir: 'myapp', object: 'users' })
) as number
```

## Configuration

Place a `db.env` file in the process working directory to tune defaults:

```bash
SLOW_QUERY_MS=200    # slow-query threshold in ms (default 500)
GLOBAL_LIMIT=10000   # max records returned per query (default 100000)
LOG_LEVEL=3          # 1=ERROR 2=WARN 3=INFO 4=DEBUG
```

`DB_ROOT` is ignored — the constructor argument is always used. Settings must be set before `new ShardDb()` is called.

## Platform support

| Platform | Architecture | Prebuild |
|---|---|---|
| Linux | x64 | ✓ |
| Linux | arm64 | ✓ |
| macOS | arm64 (Apple Silicon) | ✓ |
| Windows | — | ✗ not supported |

## Links

- [GitHub](https://github.com/sayyiditow/shard-db)
- [Full documentation](https://github.com/sayyiditow/shard-db/tree/main/docs)
- [Changelog](https://github.com/sayyiditow/shard-db/blob/main/CHANGELOG.md)
