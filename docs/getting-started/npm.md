# Node.js / Bun (npm)

shard-db ships a prebuilt native addon for Node.js ≥ 18 and Bun ≥ 1.0.
It runs in [embedded mode](embedded-mode.md) — in-process, no daemon, no
TCP socket.

## Install

```bash
npm install shard-db
# or
bun add shard-db
```

Prebuilt binaries are provided for:

| Platform | Architecture |
|---|---|
| Linux | x64, arm64 |
| macOS | arm64 (Apple Silicon) |

On other platforms the package falls back to building from source
(`node-gyp` required).

## Open and close

```js
const ShardDb = require('shard-db')

const db = new ShardDb('/path/to/data')
// ... use db ...
db.close()
```

`dbRoot` must be an existing, writable directory path.  Only one instance
per process is supported.

## Queries

`db.query()` accepts a `QueryBody` object (TypeScript autocomplete included)
or a raw JSON string and returns a `Promise<string>` that resolves to the
JSON response.  Queries run on worker threads — the event loop is never
blocked, and multiple queries may be in flight concurrently.

```js
const result = await db.query({ mode: 'count', dir: 'default', object: 'users' })
const count = JSON.parse(result)   // e.g. 42
```

### Create an object (table)

```js
await db.query({
  mode: 'create-object',
  dir: 'default',
  object: 'users',
  splits: 16,
  max_key: 128,
  fields: [
    'name:varchar:100',
    'email:varchar:200',
    'age:int',
    'active:bool',
    'created:datetime:auto_create'
  ],
  indexes: ['email', 'age']
})
```

### Insert a record

```js
await db.query({
  mode: 'insert',
  dir: 'default',
  object: 'users',
  key: 'u1',
  value: { name: 'Alice', email: 'alice@example.com', age: 30, active: true }
})
```

### Get a record

```js
const raw = await db.query({ mode: 'get', dir: 'default', object: 'users', key: 'u1' })
const user = JSON.parse(raw)
// { name: 'Alice', email: 'alice@example.com', age: 30, active: true, created: '...' }
```

### Find with criteria

```js
// All users older than 25, newest first
const raw = await db.query({
  mode: 'find',
  dir: 'default',
  object: 'users',
  criteria: [{ field: 'age', op: 'gt', value: '25' }],
  order_by: 'age',
  order: 'desc',
  limit: 20
})
const users = JSON.parse(raw)  // array of value objects
```

Common operators: `eq`, `neq`, `lt`, `gt`, `lte`, `gte`, `between`, `in`,
`starts`, `contains`, `like`.  See
[Query protocol → find](../query-protocol/find.md) for the full list.

### Count

```js
const n = JSON.parse(await db.query({
  mode: 'count',
  dir: 'default',
  object: 'users',
  criteria: [{ field: 'active', op: 'eq', value: 'true' }]
}))
```

### Aggregate

```js
const raw = await db.query({
  mode: 'aggregate',
  dir: 'default',
  object: 'users',
  aggregates: [
    { fn: 'count', alias: 'n' },
    { fn: 'avg', field: 'age', alias: 'avg_age' }
  ],
  group_by: 'active'
})
const rows = JSON.parse(raw)
// e.g. [{ active: true, n: 38, avg_age: 31.4 }, { active: false, n: 4, avg_age: 55.0 }]
```

### Bulk insert

```js
await db.query({
  mode: 'bulk-insert',
  dir: 'default',
  object: 'users',
  records: [
    { key: 'u2', value: { name: 'Bob',   email: 'b@x.com', age: 22, active: true } },
    { key: 'u3', value: { name: 'Carol', email: 'c@x.com', age: 45, active: false } }
  ]
})
```

Bulk insert is an **upsert** — it overwrites existing keys and updates their
index entries.

### Concurrent queries

Multiple queries may be awaited concurrently without any locking on the JS
side — shard-db's internal worker pool handles parallelism:

```js
const [usersRaw, countRaw] = await Promise.all([
  db.query({ mode: 'find', dir: 'default', object: 'users', limit: 20 }),
  db.query({ mode: 'count', dir: 'default', object: 'users' })
])
```

### Cursor pagination

Cursor pagination is O(limit) per page regardless of depth — prefer it over
`offset` for large datasets.

```js
// First page
let page = JSON.parse(await db.query({
  mode: 'find',
  dir: 'default',
  object: 'users',
  order_by: 'age',
  order: 'asc',
  limit: 20,
  cursor: null        // null = first page
}))
// page.rows   — array of records
// page.cursor — pass to next call, or null when exhausted

// Next page
page = JSON.parse(await db.query({
  mode: 'find',
  dir: 'default',
  object: 'users',
  order_by: 'age',
  order: 'asc',
  limit: 20,
  cursor: page.cursor
}))
```

`order_by` must be an indexed field.  See
[Query protocol → find](../query-protocol/find.md) for the full cursor
protocol.

## Log handler

Embedded mode is **silent by default**.  Register a handler to receive
errors, warnings, and slow-query events:

```js
db.setLogHandler((type, msg) => {
  const label = ShardDb.LOG_TYPES[type]   // 'error' | 'warn' | 'info' | ...
  if (type <= 2)       console.error('[shard-db:' + label + ']', msg.trim())
  else if (type === 6) console.warn('[shard-db:slow]', msg.trim())
})
```

Log types:

| `type` | `LOG_TYPES[type]` | When |
|---|---|---|
| 1 | `'error'` | Internal errors |
| 2 | `'warn'`  | Warnings |
| 3 | `'info'`  | General info |
| 4 | `'debug'` | Verbose debug |
| 5 | `'audit'` | Auth / write audit trail |
| 6 | `'slow'`  | Slow-query threshold crossed |

`msg` is a pre-formatted string:
`"YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n"`

Call `db.setLogHandler(null)` to unregister.

> **Note:** The handler fires on the JS thread after each query Promise
> resolves.  Startup events from `new ShardDb()` are not delivered.

## Configuration

Place a `db.env` file in the **working directory of the Node.js / Bun process**
to override defaults:

```bash
# db.env
SLOW_QUERY_MS=200    # threshold for slow-query events (default 500)
GLOBAL_LIMIT=10000   # max records per query result (default 100000)
LOG_LEVEL=3          # 1=ERROR 2=WARN 3=INFO 4=DEBUG
```

`DB_ROOT` in the file is ignored — the `dbRoot` constructor argument is
always authoritative.  All other settings take effect before caches are
allocated, so they must be in the file before `new ShardDb()` is called.

## TypeScript

Full type definitions are included.  `QueryBody` is a discriminated union
covering all query modes:

```ts
import ShardDb = require('shard-db')

const db = new ShardDb(dbRoot)

db.setLogHandler((type: number, msg: string) => {
  if (ShardDb.LOG_TYPES[type] === 'error') console.error(msg.trim())
})

const raw: string = await db.query({ mode: 'count', dir: 'default', object: 'users' })
const count: number = JSON.parse(raw)
```

`type` in the log handler is a raw number (1–6).  Use `ShardDb.LOG_TYPES[type]`
to get the string label.

## Limitations

See [Embedded mode → Limitations](embedded-mode.md#limitations).
