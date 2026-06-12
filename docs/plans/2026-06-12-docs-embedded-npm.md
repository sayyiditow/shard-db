# Plan: Embedded mode and npm docs

**Date:** 2026-06-12

## Execution rules

- Branch off `main`: `git checkout -b feat/docs-embedded-npm`
- Do tasks in order.
- No build or test step needed — docs only.
- Never invent API details; use only what is documented in this plan.
- If an anchor is not found exactly, stop and write `PLAN_NOTES.md`.

---

## Task 1 — Create `docs/getting-started/embedded-mode.md`

Create the file with this exact content:

```markdown
# Embedded mode

shard-db can run **in-process** inside your application — no daemon, no TCP
socket, no separate process.  You open a data directory, run queries
synchronously, and close when you're done.  All the same storage, indexes,
and query capabilities are available; the only things missing are the TCP
server and its config knobs (`PORT`, `TLS_*`, `THREADS`, etc.).

## When to use it

| Use daemon mode | Use embedded mode |
|---|---|
| Multiple applications share one database | Single application owns the data |
| Remote access required | All access is in-process |
| Multi-language clients (Python, Go, …) | C, C++, or Node.js / Bun |
| Hot reload / live config changes | Restart is acceptable |

## C API

Include `shard_db.h` (exported alongside `libshard-db.a` during `./build.sh`):

```c
#include "shard_db.h"
```

### Open

```c
ShardDb *db = shard_db_open("/path/to/data");
if (!db) { /* check stderr */ }
```

`db_root` must be an existing, writable directory.  One instance per process
(single-instance guard enforced with an atomic flag).

### Query

```c
char  *out     = NULL;
size_t out_len = 0;

int rc = shard_db_query(db,
    "{\"mode\":\"count\",\"dir\":\"default\",\"object\":\"users\"}",
    &out, &out_len);

if (rc == 0) {
    printf("%.*s\n", (int)out_len, out);   /* e.g. 42 */
    shard_db_free_result(out);
}
```

`shard_db_query` is **thread-safe** — multiple threads may call it concurrently
on the same handle.  Returns 0 on success, -1 on allocation failure.  The
response is the same JSON string the daemon would return over TCP; parse it
with any JSON library.

Always free the result with `shard_db_free_result(out)`.

### Close

```c
shard_db_close(db);
```

Shuts down the CPU and I/O thread pools, flushes caches, and frees all
resources.  After this call the pointer is invalid.

### Log handler

By default embedded mode is **silent** — no log files, no ring buffer.
Register a callback to receive events:

```c
void my_log(int type, const char *msg, void *userdata) {
    /* msg: "YYYY-MM-DD HH:MM:SS LEVEL [subsystem] text\n" */
    if (type <= SHARD_DB_LOG_WARN)
        fprintf(stderr, "[shard-db] %s", msg);
}

shard_db_set_log_handler(db, my_log, NULL);
```

Log type constants:

| Constant | Value | When |
|---|---|---|
| `SHARD_DB_LOG_ERROR` | 1 | Internal errors |
| `SHARD_DB_LOG_WARN`  | 2 | Warnings |
| `SHARD_DB_LOG_INFO`  | 3 | General info |
| `SHARD_DB_LOG_DEBUG` | 4 | Verbose debug |
| `SHARD_DB_LOG_AUDIT` | 5 | Auth / write audit trail |
| `SHARD_DB_LOG_SLOW`  | 6 | Slow-query threshold crossed |

**Thread safety:** the handler is called from whichever thread emits the
event — including shard-db's internal worker threads.  The handler must be
thread-safe.

**Timing:** set the handler after `shard_db_open` returns.  Log events that
occur during `open` itself (schema load, recovery) are emitted before the
handler is registered and will not be delivered.

## Configuration

shard-db reads `db.env` from the process's **current working directory** on
`shard_db_open`, if the file exists.  `DB_ROOT` inside the file is
ignored — the `db_root` argument to `shard_db_open` is always authoritative.

Useful knobs for embedded mode:

```bash
# db.env  (place in your app's working directory)
SLOW_QUERY_MS=200       # default 500 — log queries slower than this
GLOBAL_LIMIT=10000      # default 100000 — max records returned per query
FCACHE_MAX=4096         # default 4096 — open file-handle cache size
TOKEN_CAP=1024          # default 1024 — token table buckets
LOG_LEVEL=3             # default 3 — 1=ERROR 2=WARN 3=INFO 4=DEBUG
```

If no `db.env` is present all settings stay at their compiled defaults.
The file is only read once at open time; changing it after `shard_db_open`
has no effect until the next open.

## Linking

`./build.sh` produces `build/bin/libshard-db.a` and `build/bin/shard_db.h`.

```makefile
CFLAGS  += -I/path/to/build/bin
LDFLAGS += -L/path/to/build/bin -lshard-db -lpthread
# On Linux also: -lm
# With TLS enabled: -lssl -lcrypto
```

## Limitations

- **One instance per process.**  A second `shard_db_open` call returns NULL
  until the first is closed.
- **No TCP server.**  Daemon config knobs that relate to the TCP layer
  (`PORT`, `TLS_*`, `THREADS`, `WORKERS`, `MAX_CONCURRENT_QUERIES`,
  `MAX_REQUEST_SIZE`) are parsed from `db.env` if present but have no effect.
- **Startup logs not delivered.**  Events emitted during `shard_db_open`
  occur before the handler is registered.  If you need to capture them, start
  the daemon, redirect its log to your log pipeline, and use the TCP API
  instead.
```

---

## Task 2 — Create `docs/getting-started/npm.md`

Create the file with this exact content:

```markdown
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
or a raw JSON string and returns the JSON response string synchronously.

```js
const result = db.query({ mode: 'count', dir: 'default', object: 'users' })
const count = JSON.parse(result)   // e.g. 42
```

### Create an object (table)

```js
db.query({
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
db.query({
  mode: 'insert',
  dir: 'default',
  object: 'users',
  key: 'u1',
  value: { name: 'Alice', email: 'alice@example.com', age: 30, active: true }
})
```

### Get a record

```js
const raw = db.query({ mode: 'get', dir: 'default', object: 'users', key: 'u1' })
const user = JSON.parse(raw)
// { name: 'Alice', email: 'alice@example.com', age: 30, active: true, created: '...' }
```

### Find with criteria

```js
// All users older than 25, newest first
const raw = db.query({
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
const n = JSON.parse(db.query({
  mode: 'count',
  dir: 'default',
  object: 'users',
  criteria: [{ field: 'active', op: 'eq', value: 'true' }]
}))
```

### Aggregate

```js
const raw = db.query({
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
db.query({
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

### Cursor pagination

Cursor pagination is O(limit) per page regardless of depth — prefer it over
`offset` for large datasets.

```js
// First page
let raw = db.query({
  mode: 'find',
  dir: 'default',
  object: 'users',
  order_by: 'age',
  order: 'asc',
  limit: 20,
  cursor: null        // null = first page
})
let page = JSON.parse(raw)
// page.items  — array of records
// page.cursor — pass to next call, or null when exhausted

// Next page
raw = db.query({
  mode: 'find',
  dir: 'default',
  object: 'users',
  order_by: 'age',
  order: 'asc',
  limit: 20,
  cursor: page.cursor
})
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

> **Note:** The handler fires after `query()` returns, on the calling thread.
> Startup events from `new ShardDb()` are not delivered.

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

db.setLogHandler((type: ShardDb.LogType, msg: string) => {
  if (type === 'error') console.error(msg.trim())
})

const raw: string = db.query({ mode: 'count', dir: 'default', object: 'users' })
const count: number = JSON.parse(raw)
```

Wait — `type` in the callback is a **number** (1–6), not the `LogType` string.
Map it with `ShardDb.LOG_TYPES[type]` to get the string label.

## Limitations

See [Embedded mode → Limitations](embedded-mode.md#limitations).  The npm
package adds one more:

- **Synchronous only.**  `db.query()` blocks the calling thread.  In a
  Node.js HTTP server, run `new ShardDb()` once at startup and share the
  handle — the underlying worker pool is multi-threaded even though the JS
  call is synchronous.
```

---

## Task 3 — Update `docs/index.md`

### 3a — Add embedded mode and npm to the feature list

Anchor — find the exact line:
```
- **shard-cli TUI** — separate ncurses client that connects over the same TCP+TLS wire; menus for browse / query / schema / maintenance / auth / live stats. See [CLI reference → shard-cli](cli/shard-cli.md).
```

Insert immediately after it:
```
- **Embedded mode** — run shard-db in-process with no daemon and no TCP socket.  C API (`shard_db_open` / `shard_db_query` / `shard_db_close`) and a Node.js / Bun npm package (`shard-db`) with full TypeScript types and a log handler callback.
```

### 3b — Add embedded mode and npm rows to the "Where to go next" table

Anchor — find the exact line:
```
| See what shipped | [Changelog](reference/changelog.md) |
```

Insert immediately after it:
```
| Use shard-db in-process (C / C++) | [Embedded mode](getting-started/embedded-mode.md) |
| Use shard-db from Node.js / Bun | [npm package](getting-started/npm.md) |
```
```
